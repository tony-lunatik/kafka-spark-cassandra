import java.io.FileInputStream
import java.util.{Date, Properties}

import com.datastax.driver.core.{BoundStatement, Cluster}
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy, DefaultRetryPolicy}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._

import scala.collection.JavaConversions._

object SparkTwitter {

  def main(args: Array[String]) {
    val sparkProperties = new Properties
    sparkProperties.load(SparkTwitter.getClass.getResourceAsStream("spark.properties"))

    val twitterProperties = new Properties
    twitterProperties.load(SparkTwitter.getClass.getResourceAsStream("twitter.properties"))

    val cassandraProperties = new Properties
    cassandraProperties.load(SparkTwitter.getClass.getResourceAsStream("cassandra.properties"))

    for (name <- twitterProperties.stringPropertyNames) {
      System.setProperty(name, twitterProperties.getProperty(name))
    }

    //val master = sparkProperties.getProperty("master")
    val appName = sparkProperties.getProperty("app.name")

    val sc = new SparkConf()
      .setAppName(appName)
      //.setMaster(master)
      //.setMaster("local[2]")
      //.set("spark.executor.extraClassPath", "/root/lib/*")

    val ssc = new StreamingContext(sc, Seconds(1))

    val stream = TwitterUtils.createStream(ssc, None)

    val tweetsByUser = stream
      //.filter(_.getUser.getLang == "en")
      .map(status => Tweet(status.getId, status.getCreatedAt, status.getUser.getId, status.getUser.getScreenName, null, status.getText))

    val tweetsByHashtag = tweetsByUser
      .flatMap(tweet => {
        tweet.text
          .split("\\s")
          .filter(_.startsWith("#"))
          .map(Tweet(tweet.id, tweet.createdAt, tweet.userId, null, _, tweet.text))
      })

    tweetsByUser.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {

        val cluster = Cluster.builder()
          .addContactPoint(cassandraProperties.getProperty("host"))
          //.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
          //.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
          .build()

        val session = cluster.connect("twitter")

        partition.foreach(tweet => {

          val stmt = session.prepare("INSERT INTO tweets_by_user (user_id, created_at, text)  VALUES (?, ?, ?);")
          val boundStmt = new BoundStatement(stmt)
          session.execute(boundStmt.bind(tweet.userId, tweet.createdAt, tweet.text))

        })

        cluster.close()

      })
    })

    tweetsByHashtag.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {

        val cluster = Cluster.builder()
          .addContactPoint(cassandraProperties.getProperty("host"))
          //.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
          //.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
          .build()

        val session = cluster.connect("twitter")

        partition.foreach(tweet => {

          val stmt = session.prepare("INSERT INTO tweets_by_hashtag (hashtag, created_at, text)  VALUES (?, ?, ?);")
          val boundStmt = new BoundStatement(stmt)
          session.execute(boundStmt.bind(tweet.hashtag, tweet.createdAt, tweet.text))

        })

        cluster.close()

      })
    })

    /*

    val topUserCounts = byUser.map(x => (x._1, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map{case (user, count) => (count, user)}
      .transform(_.sortByKey(ascending = false))

    val topHashtagCounts = byHashtag.map(x => (x._1, 1)).reduceByKeyAndWindow(_ + _, Seconds(30))
      .map{case (tag, count) => (count, tag)}
      .transform(_.sortByKey(ascending = false))

    // Print popular hashtags
    topHashtagCounts.foreachRDD(rdd => {
      val topList = rdd.take(5)
      println("\nPopular topics in last 30 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topUserCounts.foreachRDD(rdd => {
      val topList = rdd.take(5)
      println("\nActive users in last minute (%s total):".format(rdd.count()))
      topList.foreach{case (count, (id, name)) => println("%s (%s tweets)".format(name, count))}
    })

    */

    ssc.start()
    ssc.awaitTermination()
  }

}