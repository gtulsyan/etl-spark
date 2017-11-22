
name := "etl-spark"
scalaVersion := "2.11.8"

organization := "com.blackbuck"
version      := "0.1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.1",
  "org.apache.kafka" % "kafka_2.11" % "0.10.2.1",
  "org.apache.kafka" % "kafka-clients" % "0.10.2.1",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.1",
  "org.apache.spark" % "spark-streaming_2.11" % "2.1.1",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.1",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.1.1",
  "com.amazonaws" % "aws-java-sdk" % "1.11.228",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.228",
  "com.amazonaws" % "aws-java-sdk-core" % "1.11.228",
  "org.apache.hadoop" % "hadoop-aws" % "2.6.0",
  "com.101tec" % "zkclient" % "0.10"



)

unmanagedBase := baseDirectory.value / "lib"



assemblyMergeStrategy in assembly ~= { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}
        