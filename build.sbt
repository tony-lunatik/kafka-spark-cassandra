name := "kafka-spark-cassandra"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.1.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.1.1"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" %  "1.1.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.1.1"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1"

libraryDependencies += "commons-io" % "commons-io" % "2.4"

