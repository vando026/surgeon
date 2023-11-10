scalaVersion := "2.12.17"
version := "0.0.6"
name := "surgeon"
organization := "conviva"

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

libraryDependencies ++= List(
  "org.apache.spark" % "spark-sql_2.12" % "3.4.0" 
    exclude("org.slf4j", "slf4j-log4j12"),
  "org.scalameta" %% "munit" % "0.7.29" % Test,
  "com.conviva.packetbrain" % "parquet-pb" % "9.1.0",
  "com.conviva.3d" % "3dReports_2.12" % "2.234.0.6914" 
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("com.conviva", "connectionMetadata_2.12")
    exclude("com.conviva", "deviceMetadata_mapAdaptor_2.12")
    exclude("com.twitter", "algebird-core_2.12")
    exclude("com.conviva.platform", "utils_2.12")
    exclude("org.scala", "scala-parser-combinators_2.12"),
)

dependencyOverrides ++= Seq(
  "org.scala-lang.modules" % "scala-parser-combinators_2.12" % "1.0.4",
)

// publishArtifact := false
testFrameworks += TestFramework("munit.TestFramework")
// publishTo := Some(Resolver.file("local-git",
// file("/Users/avandormael/Documents/ConvivaRepos/surgeon/releases")))
