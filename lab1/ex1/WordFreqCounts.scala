// sbt assembly help: https://sparkour.urizone.net/recipes/building-sbt/
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.nio.file._
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object WordFreqCounts
{

	def writeToFile(file: String, stringToWrite: String): Unit = {

	}


	def main(args: Array[String])
	{
		val inputFile = args(0) // Get input file's name from this command line argument
		val conf = new SparkConf().setAppName("WordFreqCounts")
		val sc = new SparkContext(conf)

		println("Input file: " + inputFile)

		// Uncomment these two lines if you want to see a less verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		val t0 = System.currentTimeMillis

		System.err.println("Loading data...")
		val lines = sc.textFile(inputFile, 16)

		System.err.println("Processing data...")
		val sentences = lines.flatMap(line => line.toLowerCase.split("[^\\w' #*]+").filter(el => el.length != 0))

		//sentences.take(20).foreach(x => println(x))

		val wordpairs = sentences.flatMap(sentence => ("' " + sentence).split("[^\\w']+").filter(el => el.length != 0).sliding(2).flatMap{
			case Array(x,"") => None
			case Array(x,y) => Some(y.replaceAll("[^\\p{L}\\p{Nd}]+", ""),(x,1))
			case _ => None
		}).groupByKey()

		//val wordpairs = words.map(word => word)

		val wordpairswithcount = wordpairs.map(wordpair => (wordpair._1, wordpair._2.size, (wordpair._2.foldLeft(List[(String, Int)]())((accum, curr)=>{
		  val accumAsMap = accum.toMap
		  accumAsMap.get(curr._1) match {
		    case Some(value : Int) => (accumAsMap + (curr._1 -> (value + curr._2))).toList
		    case None => curr :: accum
		  }
		})).sortWith(_._1 < _._1).sortWith(_._2 > _._2))).sortBy(_._1, true).sortBy(_._2, false)

		/*val wordpairswithcount = wordpairs.map(wordpair => (wordpair._1, wordpair._2.size, wordpair._2)).sortBy(_._2, false)

		val wordpairsflat = wordpairs.flatMap(wordpair => ((wordpair._1, wordpair._2._1), wordpair._2._2)).reduceByKey(_+_).sortBy(_._2, false)*/


		//wordpairs.foreach(wordpair => )


		//var sorted = wordpairswithcount.filter(el => el._1.contains("bone"))


		//val wordpairswithcountsubsorted = wordpairswithcount.map(wordpairswithcount => wordpairswithcount._3.sortBy(_._2,false))
		val et = (System.currentTimeMillis - t0)
		System.err.println("Writing results...")

		val writer = Files.newBufferedWriter(Paths.get("freq.txt"))
		try
			wordpairswithcount.collect().foreach{ e=>
				writer.write(e._1+":"+e._2+"\n")
				for (x <- e._3 if x._1!="'")
	    			writer.write("\t"+x._1+":"+x._2+"\n")
			}
		finally
			writer.close()




        /*val counts = words.map(word => (word(0), 1))
                 .reduceByKey(_ + _)

        counts.take(50).foreach(x => println(x))*/

        //words.saveAsTextFile("freq.txt")
        //predcounts.saveAsTextFile("predfreq.txt")*/

		val et2 = (System.currentTimeMillis - t0)
		System.err.println("Done!\nCompute Time taken = %f secs\nIO Time taken = %f secs".format(et / 1000.0, (et2-et) / 1000.0))
	}
}
