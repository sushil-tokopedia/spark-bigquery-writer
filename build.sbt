name := "spark-bigquery-writer"

organization := "com.tokopedia"

version := "0.2"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",
  "com.google.cloud" % "google-cloud-bigquery" % "1.103.0",
  "org.scalatest" %% "scalatest" % "3.1.0" % "test"
)
