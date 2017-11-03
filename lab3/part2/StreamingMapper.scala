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

object StreamingMapper
{
	final val runLocal = true
	final val linesPerChunk = 1024

	// def bwaRun(x: String, bcconfig: Broadcast[Configuration]): Array[(Int, SAMRecord)] =
	// {
	// 	val config = bcconfig.value
	// 	val refFolder = config.getRefFolder
	// 	val toolsFolder = config.getToolsFolder
	// 	val numThreads = config.getNumThreads
	// 	val numChunks = config.getNumInstances
	// 	val inputFolder = config.getInputFolder
	// 	val tmpFolder = config.getTmpFolder

	// 	// Create the command string (bwa mem...)and then execute it using the Scala's process package. More help about
	// 	//	Scala's process package can be found at http://www.scala-lang.org/api/current/index.html#scala.sys.process.package.

	// 	// bwa mem refFolder/RefFileName -p -t numOfThreads fastqChunk > outFileName
	// 	val inputFile = x
	// 	val outFileName = tmpFolder + "bwamem" + x.filter(_.isDigit)
	// 	println(inputFile)
	// 	println(outFileName)

	// 	val command = Seq(toolsFolder + "bwa", "mem", refFolder + RefFileName, "-p", "-t", numThreads, inputFile)
	// 	println(command)
	// 	command #> new File(outFileName) lines

	// 	val bwaKeyValues = new BWAKeyValues(outFileName)
	// 	bwaKeyValues.parseSam()
	// 	val kvPairs: Array[(Int, SAMRecord)] = bwaKeyValues.getKeyValuePairs()

	// 	//new File(outFileName).delete
	// 	Seq("rm", outFileName).lines

	// 	return kvPairs
	// }

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
				new File("config_local.xml")
			}
			else {
				new File("config.xml")
			}

		val documentBuilderFactory = DocumentBuilderFactory.newInstance
		val documentBuilder = documentBuilderFactory.newDocumentBuilder
		val document = documentBuilder.parse(file)

		val refPath = getTagValue(document, "refPath")
		val bwaPath = getTagValue(document, "bwaPath")
		val numTasks = getTagValue(document, "numTasks")
		val numThreads = getTagValue(document, "numThreads")
		val intervalSecs = getTagValue(document, "intervalSecs").toInt
		val streamDir = getTagValue(document, "streamDir")
		val inputDir = getTagValue(document, "inputDir")
		val tempDir = getTagValue(document, "tempDir")
		val outputDir = getTagValue(document, "outputDir")

		println(s"refPath = $refPath\nbwaPath = $bwaPath\nnumTasks = $numTasks\nnumThreads = $numThreads\nintervalSecs = $intervalSecs")
		println(s"streamDir = $streamDir\ninputDir = $inputDir\noutputDir = $outputDir")

		// Create stream and output directories if they don't already exist
		new File(streamDir).mkdirs
		new File(outputDir).mkdirs
		new File(tempDir).mkdirs
		//////////////////////////////////////////////////////////////////////

		sparkConf.setMaster("local[" + numTasks + "]")
		sparkConf.set("spark.cores.max", numTasks)
		val ssc = new StreamingContext(sparkConf, Seconds(intervalSecs))
		val filenames = ssc.textFileStream(streamDir)

		if (runLocal) {
			filenames.print()
		}
		else {
			// var bwaResults = filenames.flatMap(files => bwaRun(files.getPath, bcconfig))
			// 		.combineByKey(
			// 			(sam: SAMRecord) => Array(sam),
			// 			(acc: Array[SAMRecord], value: SAMRecord) => (acc :+ value),
			// 			(acc1: Array[SAMRecord], acc2: Array[SAMRecord]) => (acc1 ++ acc2)
			// 		).persist(MEMORY_ONLY_SER)//cache
			// bwaResults.setName("rdd_bwaResults")
		}

		val reader1 = new fastq.FastqReader(new File(inputDir + "/fastq1.fq"))
		val reader2 = new fastq.FastqReader(new File(inputDir + "/fastq2.fq"))

		var j = 0
		val factory = new fastq.FastqWriterFactory()

		while(reader1.hasNext()) {
			var i = 0
			val writer1 = factory.newWriter(new File("%s/fastq1_%04d.fq".format(tempDir, j)))
			val writer2 = factory.newWriter(new File("%s/fastq2_%04d.fq".format(tempDir, j)))

			while (i < linesPerChunk && reader1.hasNext()) {
				writer1.write(reader1.next())
				writer2.write(reader2.next())

				i += 1
			}

			writer1.close()
			writer2.close()

			val writer = new PrintWriter("%s/files_%04d.txt".format(streamDir, j), "UTF-8");
			writer.println("%s/fastq1_%04d.fq".format(tempDir, j));
			writer.println("%s/fastq2_%04d.fq".format(tempDir, j));
			writer.close();

			j += 1
		}

		ssc.start()
		ssc.awaitTermination()
	}
}
