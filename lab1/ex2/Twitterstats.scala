import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.io.{File}
import java.nio.file.{Files,Paths,StandardOpenOption}
import java.util.Locale
import java.text._
import java.net._
import java.util.Calendar
import org.apache.tika.language.LanguageIdentifier
import java.util.regex.Matcher
import java.util.regex.Pattern
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.parsers.DocumentBuilder
import org.w3c.dom.Document

// Check example https://github.com/apache/bahir/blob/master/streaming-twitter/examples/src/main/scala/org/apache/spark/examples/streaming/twitter/TwitterPopularTags.scala

object Twitterstats
{
	def getTimeStamp() : String =
	{
		return new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime())
	}

	def getLang(s: String) : String =
	{
		val inputStr = s.replaceFirst("RT", "").replaceAll("@\\p{L}+", "").replaceAll("https?://\\S+\\s?", "")
		var langCode = new LanguageIdentifier(inputStr).getLanguage

		// Detect if japanese
		var pat = Pattern.compile("\\p{InHiragana}")
		var m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ja"
		// Detect if korean
		pat = Pattern.compile("\\p{IsHangul}");
		m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ko"

		return langCode
	}

	def getLangNameFromCode(code: String) : String =
	{
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}


	def main(args: Array[String])
	{
		val file = new File("cred.xml")
		val documentBuilderFactory = DocumentBuilderFactory.newInstance
		val documentBuilder = documentBuilderFactory.newDocumentBuilder
		val document = documentBuilder.parse(file);

		// Configure Twitter credentials
		val consumerKey = document.getElementsByTagName("consumerKey").item(0).getTextContent
		val consumerSecret = document.getElementsByTagName("consumerSecret").item(0).getTextContent
		val accessToken = document.getElementsByTagName("accessToken").item(0).getTextContent
		val accessTokenSecret = document.getElementsByTagName("accessTokenSecret").item(0).getTextContent

		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
		Logger.getRootLogger.setLevel(Level.OFF)

		// Set the system properties so that Twitter4j library used by twitter stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
		System.setProperty("twitter4j.oauth.accessToken", accessToken)
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

		val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[8]")

		val ssc = new StreamingContext(sparkConf, Seconds(2))
		//Set up the input stream
		val stream = TwitterUtils.createStream(ssc, None)
		//Set up the windowing.
		val wStream = stream.window(Seconds(120), Seconds(10))
		// Insert your code here
		//Filter the tweets on language
		val englishStream = wStream.filter(status => status.getLang == "en" || status.getLang == "no")
		// Filter all the non retweet tweets and reformat.
		val englishStatuses = englishStream.filter(status => !status.isRetweet).map(x=> (x.getId,(x.getUser.getName,x.getText,x.getHashtagEntities.map(x=>x.getText))))
		// Get tweets with hashtags and turn the pairs around
		val englishStatusesHashtags = englishStatuses.filter(x=> !x._2._3.isEmpty).map(x=> (x._2._3(0),x._1))
		// Get all retweets and reformat the tuples
		val retweetedStatuses = englishStream.filter(status => status.isRetweet).map(x=> (x.getRetweetedStatus.getId,(x.getRetweetedStatus.getUser.getName,x.getRetweetedStatus.getText)))
		//Count all the retweets for the original tweet ids.
		val retweetCounts = retweetedStatuses.map(x=> (x._1,1)).reduceByKey(_+_)
		//Remap hashtags to ids.
		val hashtags = englishStatuses.map(status=>(status._1,status._2._3))
		//Count global hashtags and sort them
		val globalHashtags = hashtags.flatMap(status=>status._2).map(hashtag=>(hashtag,1)).reduceByKey(_+_).transform(x=>x.sortBy(y=>y._2, true))
		// Add the global hashtag info to all tweets.
		val englishStatusesHashtagsCounts = englishStatusesHashtags.join(globalHashtags).map(x=>(x._2._1, (x._1,x._2._2)))

		// Reformat the tuples and filter out all unique hashtag containing tweets plus sort by popular hashtags.
		val countedRetweets = englishStatuses.leftOuterJoin(retweetCounts).leftOuterJoin(englishStatusesHashtagsCounts).map({
			case (id,(((username,text,hashtags),None),Some((hashtag,hashtagcount)))) => (id,(hashtagcount,hashtag,username,0,text)) //keep tweets with non-unique hashtags regardless of retweets
			case (id,(((username,text,hashtags),Some(retweetcount)),Some((hashtag,hashtagcount)))) => (id,(hashtagcount,hashtag,username,retweetcount,text)) //keep tweets with non-unique hashtags regardless of retweets
			case (id,(((username,text,hashtags),None),None)) => (id,(0,"None",username,0,text)) //keep tweets without hashtags
			case (id,(((username,text,hashtags),Some(retweetcount)),None)) => (id,(0,"None",username,retweetcount,text)) //keep tweets without hashtags			
		}).filter(x=> x._2._1!=1 || x._2._4>0 ).transform(rdd=>rdd.sortBy(x=>x._2._2,true).sortBy(x=>x._2._1,false))
		
		
		var rowNumber:Int = 0;

		
		// Empty file or create it.
		val createWriter = Files.newBufferedWriter(Paths.get("tweets.txt"))
		createWriter.close()
		println("File tweets.txt created or truncated. First results in 10 seconds.")
		//Print and 
		countedRetweets.foreachRDD({ rdd=>
			var rddRowNumber: Int = 0;
			var currentHashtag: String = "";
			var currentHashtagCount: Int = 0;
			var printedDots: Boolean = false;
			val writer = Files.newBufferedWriter(Paths.get("tweets.txt"),StandardOpenOption.APPEND)
			try	{
				println("==================================================\n Time: "+Calendar.getInstance().getTime() + "\n==================================================")
				writer.write("==================================================\n Time: "+Calendar.getInstance().getTime() + "\n==================================================\n")
				val count = rdd.count()
				val array = rdd.collect().foreach({
					x=>		
						//println(x)	
						if(currentHashtag == x._2._2) {
							currentHashtagCount += 1
						}
						else {
							currentHashtag = x._2._2
							currentHashtagCount = 0
							printedDots = false
						}
						
						if(currentHashtagCount<3 || x._2._2 == "None"){
							rowNumber += 1
							writer.write(rowNumber + ". " + x._2._1 + " " + x._2._2 + ":" + x._2._3 + ":" + x._2._4 + " " + x._2._5 + "\n--------------------------------------------------\n")
							if(rddRowNumber<10){
						    	println(rowNumber + ". " + x._2._1 + " " + x._2._2 + ":" + x._2._3 + ":" + x._2._4 + " " + x._2._5 + "\n--------------------------------------------------")
							}
							
						} else if(!printedDots){
							writer.write("..................................................\n")
							if(rddRowNumber<10)
								println("..................................................")	
							printedDots = true					
						}
						rddRowNumber += 1

				})
			} 
			finally
				writer.close()
		})
		//hashtags.foreachRDD(rdd=>rdd.foreach(x=>println(x._1 + "; Hashtags: " + x._2.mkString(", "))))
		//hashtagsRetweets.foreachRDD(rdd=>rdd.foreach(x=>println(x._1 + "; Retweets: " + x._2._2 + "; Hashtags: " + x._2._1.mkString(", "))))

		ssc.start()
		ssc.awaitTermination()
	}
}

