lazy val root = (project in file(".")).
  settings(
    name := "kafka-spark-cassandra",
    version := "1.0",
    scalaVersion := "2.10.5"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.1.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.1.1" % "provided",
  "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.1.1",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.1.1",
  "com.datastax.spark" % "spark-cassandra-connector_2.10" %  "1.1.1",
  "javax.servlet" % "javax.servlet-api" % "3.0.1",
  "commons-io" % "commons-io" % "2.4"
)

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}