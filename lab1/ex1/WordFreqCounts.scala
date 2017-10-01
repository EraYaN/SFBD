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
		// 1. Split every line on sentence boundaries to obtain strings with sentence segments.
		// 2. Split every sentence segment on word boundaries to convert sentence segments to an array of words.
		// 3. Trim any non-alphanumeric characters from the start and end of all words.
		// 4. Filter any empty elements from the list.
		// rdd[String] => rdd[List[String]]
		val substrings = lines.flatMap(line => line.toLowerCase.split("[^\\w' *]+").map(x => x.split("[^\\w']+").map(_.replaceAll("^[^a-z0-9]*([\\w']*?)[^a-z0-9]*$", "$1")).filter(_.nonEmpty).toList))

		// 1. Flatten the list of sentences to a list containing all individual words.
		// 2. Perform a count of every individual word.
		// rdd[List[String]] => rdd[(String, Int)]
		val counts = substrings.flatMap{identity}.map((_, 1)).reduceByKey(_+_)

		// substrings.take(20).foreach(x => println(x))
		// counts.take(20).foreach(println(_))

		// 1. Filter any sentence segment that only contains a single word.
		// 2. Obtain arrays of pairs of words using a sliding window.
		// 3. Map every array onto a tuple containg words and their precedent and flatten.
		// rdd[List[String]] => rdd[((String, String), 1)]
		val bigrams = (substrings.filter(_.size > 1).map(_.sliding(2).map{case List(a, b) => ((b, a), 1)})).flatMap{identity}

		// bigrams.take(20).foreach(println(_))

		// 1. Group bigrams by key.
		// 2. Replace list of precedents by a tuple of the precedent and its occurence count.
		// 3. Sort results by precedent and count.
		// 4. Group the new datastructure by its new key.
		// rdd[((String, String), Int)] => rdd[(String, CompactBuffer[(String, Int)])]
		val reduced = bigrams.groupByKey().map(x => (x._1._1, (x._1._2, x._2.reduce(_+_)))).sortBy(_._2._1).sortBy(_._2._2, false).groupByKey()

		// reduced.take(20).foreach(println(_))

		// 1. Join counts with the list of words and precedents.
		// 2. Perform an outer sort by word and count.
		// rdd[(String, CompactBuffer[(String, Int)])] => rdd[(String, (Int, Some(CompactBuffer[(String, Int)])))]
		val result = counts.leftOuterJoin(reduced).sortBy(_._1).sortBy(_._2._1, false)

		// result.take(20).foreach(println(_))

		val et = (System.currentTimeMillis - t0)
		System.err.println("Writing results...")

		val writer = Files.newBufferedWriter(Paths.get("freq.txt"))
		try
			result.collect().foreach{ e=>
				writer.write(e._1+":"+e._2._1+"\n")
				for (x <- e._2._2)
					for(y <- x)
						writer.write("\t"+y._1+":"+y._2+"\n")
			}
		finally
			writer.close()

		val et2 = (System.currentTimeMillis - t0)
		System.err.println("Done!\nCompute Time taken = %f secs\nIO Time taken = %f secs".format(et / 1000.0, (et2-et) / 1000.0))
	}
}
