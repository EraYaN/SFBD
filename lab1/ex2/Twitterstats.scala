import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.io._
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
	
  def returnFirstString(a: Array[String]): Option[String] = {
    if (a.isEmpty) {
      None
    }
    else {
      Some("#"+a(0))
    }
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
		val stream = TwitterUtils.createStream(ssc, None)

		val wStream = stream.window(Seconds(120), Seconds(10))
		// Insert your code here
		//Filter the tweets on language
		val englishStream = wStream.filter(status => status.getLang == "en" || status.getLang == "no")
		// Filter all the non retweet tweets and reformat.
		val englishStatuses = englishStream.filter(status => !status.isRetweet).map(x=> (x.getId,(x.getUser.getName,x.getText,x.getHashtagEntities.map(x=>x.getText))))
		// Get tweets with hashtags and turn the pairs around
		val englishStatusesHashtags = englishStatuses.filter(x=> !x._2._3.isEmpty).map(x=> (x._2._3(0),x._1))
		val retweetedStatuses = englishStream.filter(status => status.isRetweet).map(x=> (x.getRetweetedStatus.getId,(x.getRetweetedStatus.getUser.getName,x.getRetweetedStatus.getText)))
		
		val retweetCounts = retweetedStatuses.map(x=> (x._1,1)).reduceByKey(_+_)
		val hashtags = englishStatuses.map(status=>(status._1,status._2._3))
		val globalHashtags = hashtags.flatMap(status=>status._2).map(hashtag=>(hashtag,1)).reduceByKey(_+_).transform(x=>x.sortBy(y=>y._2, true))

		val englishStatusesHashtagsCounts = englishStatusesHashtags.join(globalHashtags).map(x=>(x._2._1, (x._1,x._2._2)))

		//val englishStatusesGCounts = englishStatuses.map(x=> (x._1,(x._2._1,x._2._2,x._2._3.leftOuterJoin(globalHashtags))))
		//val hashtagsRetweets = hashtags.join(retweetCounts)

		//val englishStatusesHCounts = englishStatuses.map(x=>(x._1,x._2,x._3,x._4.join(globalHashtags)))

		//(914147961235484672,(((Damilola,Happy life #ayoms2017,[Ljava.lang.String;@2fc377),None),Some((ayoms2017,1))))
		val countedRetweets = englishStatuses.leftOuterJoin(retweetCounts).leftOuterJoin(englishStatusesHashtagsCounts).map({
			case (id,(((username,text,hashtags),None),Some((hashtag,1)))) => None //remove tweets with unique hashtag and no retweets
			case (id,(((username,text,hashtags),None),Some((hashtag,hashtagcount)))) => (id,(hashtagcount,hashtag,username,0,text)) //keep tweets with non-unique hashtags regardless of retweets
			case (id,(((username,text,hashtags),Some(retweetcount)),Some((hashtag,hashtagcount)))) => (id,(hashtagcount,hashtag,username,retweetcount,text)) //keep tweets with non-unique hashtags regardless of retweets
			case (id,(((username,text,hashtags),None),None)) => (id,(0,"None",username,0,text)) //keep tweets without hashtags
			case (id,(((username,text,hashtags),Some(retweetcount)),None)) => (id,(0,"None",username,retweetcount,text)) //keep tweets without hashtags
			case _ => None
		})
		//val countedHashtagRetweets = countedRetweets.join(hashtags)
		var count = 0;
		//englishStatuses.foreachRDD(rdd=>rdd.foreach(x=>println()))
		countedRetweets.foreachRDD({ rdd=>
			
			println("==================================================")
			println("Time: "+Calendar.getInstance().getTime())
			println("==================================================")
			count += 1
			rdd.foreach({
				x=>		
					println(x)			
					//println(count + ". " + "?TODO?" + " " + returnFirstString(x._2._1._3) + ":" + x._2._1._1 + ":" + x._2._2 + " " + x._2._1._2)
					println("--------------------------------------------------")

			})
		})
		//hashtags.foreachRDD(rdd=>rdd.foreach(x=>println(x._1 + "; Hashtags: " + x._2.mkString(", "))))
		//hashtagsRetweets.foreachRDD(rdd=>rdd.foreach(x=>println(x._1 + "; Retweets: " + x._2._2 + "; Hashtags: " + x._2._1.mkString(", "))))

		ssc.start()
		ssc.awaitTermination()
	}
}

