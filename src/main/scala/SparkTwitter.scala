import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._

import scala.collection.JavaConversions._

object SparkTwitter {

  def main(args: Array[String]) {
    val sparkProperties: Properties = new Properties
    sparkProperties.load(new FileInputStream("spark.properties"))

    val twitterProperties: Properties = new Properties
    twitterProperties.load(new FileInputStream("twitter.properties"))

    for (name <- twitterProperties.stringPropertyNames) {
      System.setProperty(name, twitterProperties.getProperty(name))
    }

    val sc = new SparkConf().setAppName(sparkProperties.getProperty("app.name")).setMaster(sparkProperties.getProperty("master"))

    val ssc = new StreamingContext(sc, Seconds(1))

    val stream = TwitterUtils.createStream(ssc, None)

    val hashTags = stream.flatMap(status => {
      status.getText.split(" ").filter(_.startsWith("#"))
    })

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    ssc.start
    ssc.awaitTermination
  }

}
