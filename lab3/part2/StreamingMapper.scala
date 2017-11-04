import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import java.io._
import java.util.Locale
import java.text._
import java.net._
import java.util.Calendar
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.parsers.DocumentBuilder
import org.w3c.dom.Document
import htsjdk.samtools._
import sys.process._

object StreamingMapper
{
	final val runLocal = false
	final val linesPerChunk = 16384

	def bwaRun(inputFile: String, outputFile: String, bwaPath: String, refPath: String, numThreads: Int) : Int =
	{
		// Create the command string (bwa mem...)and then execute it using the Scala's process package. More help about
		//	Scala's process package can be found at http://www.scala-lang.org/api/current/index.html#scala.sys.process.package.

		// bwa mem refFolder/RefFileName -p -t numOfThreads fastqChunk > outFileName
		
		val command = Seq(bwaPath, "mem", refPath, "-v", "2", "-p", "-t", numThreads.toString, inputFile)
		//println(command)
		val result = command #> new File(outputFile) !;
        if(result==0){
            println("BWA Mem completed succesfully for %s.".format(outputFile));
        } else {
            println("BWA Mem returned %d for %s.".format(result, outputFile));
        }
		return result
	}

    def deleteFolder(folder: File) {
        var files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            files.foreach({ f=>
                if(f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            })
        }
        folder.delete();
    }

	def getTimeStamp() : String =
	{
		return new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime())
	}

	def getTagValue(document: Document, tag: String) : String =
	{
		document.getElementsByTagName(tag).item(0).getTextContent
	}

	def main(args: Array[String])
	{
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
		Logger.getRootLogger.setLevel(Level.OFF)


		val sparkConf = new SparkConf().setAppName("WordCount")

		// Read the parameters from the config file //////////////////////////
		val file =
			if (runLocal) {
                if(System.getProperty("user.name")=="Erwin"){
                    if(System.getProperty("os.name").startsWith("Windows")){
                        new File("config_erwin.win.xml")
                    } else {
                        new File("config_erwin.nix.xml")

                    }    				
                } else {
                    new File("config_robin.xml")
                }
			}
			else {
				new File("config.xml")
			}

		val documentBuilderFactory = DocumentBuilderFactory.newInstance
		val documentBuilder = documentBuilderFactory.newDocumentBuilder
		val document = documentBuilder.parse(file)

		val refPath = getTagValue(document, "refPath")
		val bwaPath = getTagValue(document, "bwaPath")
		val numTasks = getTagValue(document, "numTasks").toInt
		val numThreads = getTagValue(document, "numThreads").toInt
		val intervalSecs = getTagValue(document, "intervalSecs").toInt
		val streamDir = getTagValue(document, "streamDir")
		val inputDir = getTagValue(document, "inputDir")
		val tempDir = getTagValue(document, "tempDir")
		val outputDir = getTagValue(document, "outputDir")

		println(s"refPath = $refPath\nbwaPath = $bwaPath\nnumTasks = $numTasks\nnumThreads = $numThreads\nintervalSecs = $intervalSecs")
		println(s"streamDir = $streamDir\ninputDir = $inputDir\noutputDir = $outputDir")

		// Create and clean stream and output directories
		val streamDirFile = new File(streamDir)
		val outputDirFile = new File(outputDir)
		val tempDirFile = new File(tempDir)

        if(streamDirFile.exists() && streamDirFile.isDirectory()){
            // Clean old directory
            deleteFolder(streamDirFile)
        }
        streamDirFile.mkdirs
        if(outputDirFile.exists() && outputDirFile.isDirectory()){
            // Clean old directory
            deleteFolder(outputDirFile)
        }
        outputDirFile.mkdirs

        if(tempDirFile.exists() && tempDirFile.isDirectory()){
            // Clean old directory
            deleteFolder(tempDirFile)
        }
        tempDirFile.mkdirs
		//////////////////////////////////////////////////////////////////////

		sparkConf.setMaster("local[" + numTasks.toString + "]")
		sparkConf.set("spark.cores.max", numTasks.toString)
        val sc = new SparkContext(sparkConf)
		val ssc = new StreamingContext(sc, Seconds(intervalSecs))
		val filenames = ssc.textFileStream(streamDir)
        var submittedParts : Int = 0
        var processedParts = sc.accumulator(0)
        var successfulParts = sc.accumulator(0)

        sys.ShutdownHookThread {
              println("Gracefully stopping Spark Streaming Application")
              println("Pre-Stop Progress: %d out of %d. %d were successful.".format(processedParts.value,submittedParts,successfulParts.value) )
              ssc.stop(true, true)
              println("After-Stop Progress: %d out of %d. %d were successful.".format(processedParts.value,submittedParts,successfulParts.value) )
              println("Application stopped")
        }


		if (runLocal) {
			filenames.foreachRDD({file =>  
                // Here we can sort if we need to so that fastq1 is always first            
                file.foreach({x=>
                    println("Processing file: %s/%s".format(tempDir,x))                   
                    val extra_file = new PrintWriter("%s/bwamem%s.sam".format(outputDir,x.filter(_.isDigit)), "UTF-8")
                    extra_file.println("File: %s/%s".format(tempDir,x))
                    extra_file.close()
                    processedParts += 1
                    successfulParts += 1
                    println("Processed file: %s/%s".format(tempDir,x))
                })
                
            })
            //filenames.foreachRDD({file => val extra_file = new PrintWriter("%s.extra".format(file), "UTF-8"); extra_file.close(); })
		}
		else {
            filenames.foreachRDD({file =>  
                // Here we can sort if we need to so that fastq1 is always first            
                file.foreach({x=>
                    println("Remote: Processing file: %s/%s".format(tempDir,x))                    
                    //val extra_file = new PrintWriter("%s/bwamem%s.sam".format(outputDir,x.filter(_.isDigit)), "UTF-8")
                    //extra_file.println("File: %s/%s".format(tempDir,x))
                    //extra_file.close()
                    if(bwaRun("%s/%s".format(tempDir,x),"%s/bwamem%s.sam".format(outputDir,x.filter(_.isDigit)), bwaPath, refPath, numThreads)==0){
                        successfulParts += 1
                    }

                    processedParts += 1
                    println("Remote: Processed file: %s/%s".format(tempDir,x))
                })
                
            })
			// var bwaResults = filenames.flatMap(files => bwaRun(files.getPath, bcconfig)).collect
		}
        //After all operations have been added, start the streaming.
        ssc.start()
		val reader1 = new fastq.FastqReader(new File(inputDir + "/fastq1.fq"))
		val reader2 = new fastq.FastqReader(new File(inputDir + "/fastq2.fq"))

		var j = 0
		val factory = new fastq.FastqWriterFactory()

		while(reader1.hasNext() && reader2.hasNext()) {
			var i = 0
			val writer1 = factory.newWriter(new File("%s/fastq_%04d.fq".format(tempDir, j)))
			//val writer2 = factory.newWriter(new File("%s/fastq2_%04d.fq".format(tempDir, j)))

            var counter : Int = 0
			while (i < linesPerChunk && ((i%2==0 && reader1.hasNext()) || (i%2==1 && reader2.hasNext()))) {                
                if(i%2==0){
                    writer1.write(reader1.next())
                    i += 1
                } else {
                    writer1.write(reader2.next())
                    i += 1
                }
			}

			writer1.close()
			//writer2.close()

			val writer = new PrintWriter("%s/files_%04d.txt".format(streamDir, j), "UTF-8");
			writer.println("fastq_%04d.fq".format(j));
			//writer.println("fastq2_%04d.fq".format(j));
			writer.close();
            submittedParts += 1
			j += 1
		}

		while(submittedParts>processedParts.value){
            println("Waiting to exit; Progress: %d out of %d. %d were successful.".format(processedParts.value,submittedParts,successfulParts.value))
            Thread.sleep(1000)
        }
        //ssc.stop(true, true)
	}
}
