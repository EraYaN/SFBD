/* VarDensity.scala */
/* Author: Hamid Mushtaq */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageLevel._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source
import java.io._
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

object VarDensity
{
	final val compressRDDs = true
	final val regionSize = 1e6.toInt

	val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("sparkLog.txt"), "UTF-8"))

	def getTimeStamp() : String =
	{
		return "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()) + "] "
	}

	def main(args: Array[String])
	{
		val tasks = args(0)
		val dbsnpFile = args(1)
		val dictFile = args(2)

		println(s"Tasks = $tasks\ndbsnpFile = $dbsnpFile\ndictFile = $dictFile\n")

		val conf = new SparkConf().setAppName("Variant Density Calculator App")
		conf.setMaster("local[" + tasks + "]")
		conf.set("spark.cores.max", tasks)
		if (compressRDDs)
			conf.set("spark.rdd.compress", "true")
		val sc = new SparkContext(conf)

		sc.addSparkListener(new SparkListener()
		{
			override def onApplicationStart(applicationStart: SparkListenerApplicationStart)
			{
				bw.write(getTimeStamp() + " Spark ApplicationStart: " + applicationStart.appName + "\n");
				bw.flush
			}

			override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd)
			{
				bw.write(getTimeStamp() + " Spark ApplicationEnd: " + applicationEnd.time + "\n");
				bw.flush
			}

			override def onStageCompleted(stageCompleted: SparkListenerStageCompleted)
			{
				val map = stageCompleted.stageInfo.rddInfos
				map.foreach(row => {
					if (row.isCached)
					{
						bw.write(getTimeStamp() + row.name + ": memsize = " + (row.memSize / 1000000) + "MB, rdd diskSize " +
							row.diskSize + ", numPartitions = " + row.numPartitions + "-" + row.numCachedPartitions + "\n");
					}
					else if (row.name.contains("rdd_"))
						bw.write(getTimeStamp() + row.name + " processed!\n");
					bw.flush
				})
			}
		});

		val t0 = System.currentTimeMillis

		val dbsnpLines = sc.textFile(dbsnpFile, tasks.toInt)
		// Filter headers.
		val dbsnp = dbsnpLines.filter(!_.startsWith("#"))

		val dictLines = Source.fromFile(dictFile).getLines

		// Create a map of chromosome names mapping to their corresponding index.
		// Map[String, Int]
		val chromosome2index = dictLines.map(_.split("\t")).filter(_(1).matches("SN:chr([\\d]+|[MXY])$"))
			.map{case Array(_, c:String, _*) => c.substring(3)}.zipWithIndex.toMap

		// Group all records by chromosome and position.
		// Rdd[(chromosome: String, region: Int, variants: CompactBuffer(Array(chromosome: String, position: Int)))]
		val groupedByChrPos = dbsnp.map(_.split("\t").take(2)).groupBy(x => (x(0), x(1)))
		// groupedByChrPos.foreach(println(_))

		val grouped1 = groupedByChrPos.groupBy(x => x._1._1)
		// grouped1.foreach(println(_))

		val grouped2 = grouped1.map(
			x => (x._1, x._2.grouped(regionSize).map(_.foldLeft(0)((a, b) => (a + b._2.size))).toList.zipWithIndex)
		).flatMapValues(x => x)
		// grouped2.foreach(println(_))

		val table = grouped2.map(x => (x._1, chromosome2index(x._1), x._2._2, x._2._1)).sortBy(x => (x._2, x._3))
		// table.foreach(println(_))

		// Map table to RDD of lines.
		// Rdd[line: String]
		val lines = table.map(_.productIterator.mkString("\t"))
		lines.setName("rdd_lines")

		// Write to text file.
		val writer = new PrintWriter("output/vardensity.txt")
		lines.collect().map(writer.println(_))
		writer.close()

		sc.stop()

		val et = (System.currentTimeMillis - t0) / 1000
		println("{Time taken = %d mins %d secs}".format(et / 60, et % 60))
	}
}
