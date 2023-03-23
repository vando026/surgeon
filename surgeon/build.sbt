scalaVersion := "2.12.13"
version := "0.0.2"
name := "surgeon"
organization := "conviva"

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "com.conviva.packetbrain" % "parquet-pb" % "9.1.0",
  "org.scalameta" %% "munit" % "0.7.29" % Test,
 )

publishArtifact := false
testFrameworks += TestFramework("munit.TestFramework")

// unmanagedBase := new java.io.File("/Users/avandormael/miniconda3/envs/dbconnect/lib/python3.9/site-packages/pyspark/jars")
