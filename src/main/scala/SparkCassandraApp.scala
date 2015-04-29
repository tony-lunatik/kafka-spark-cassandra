import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._

object SparkCassandraApp {

  def main(args: Array[String]) {

    val cassandraProperties = new Properties()
    cassandraProperties.load(new FileInputStream("cassandra.properties"))

    val sparkProperties = new Properties()
    sparkProperties.load(new FileInputStream("spark.properties"))

    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraProperties.getProperty("host"))

    val sc = new SparkContext(sparkProperties.getProperty("master"), sparkProperties.getProperty("app.name"), conf)

    val rdd = sc.cassandraTable(cassandraProperties.getProperty("keyspace"), "tweets_by_user")

    rdd.cache()

    println(rdd.count)

    // Much faster due to caching RDD
    println(rdd.count)

  }

}
