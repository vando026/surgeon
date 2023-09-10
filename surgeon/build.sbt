scalaVersion := "2.12.17"
version := "0.0.5"
name := "surgeon"
organization := "conviva"

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "3.4.0" exclude("org.slf4j", "slf4j-log4j12"),
  "org.scalameta" %% "munit" % "0.7.29" % Test,
  "com.conviva.packetbrain" % "parquet-pb" % "9.1.0",
  "com.conviva.3d" %% "3dReports" % "2.234.0.6914" exclude("org.slf4j", "slf4j-log4j12"),
  // "org.apache.parquet" % "parquet-encoding" % "1.7.0",
  // "org.apache.parquet" % "parquet-common" % "1.7.0",
  // "com.conviva.packetbrain" % "messages" % "9.1.0",
  // "com.conviva.packetbrain" % "log-utils" % "9.1.0",
  // "com.google.guava" % "guava" % "12.0.1"
  "com.twitter" %% "algebird-core" % "0.13.8",
  "com.conviva" %% "deviceMetadata_mapAdaptor" % "4.9.0",
  "com.conviva" %% "connectionMetadata" % "4.9.0"
)


dependencyOverrides += "org.scala-lang.modules" % "scala-parser-combinators_2.12" % "1.0.4"

// publishArtifact := false
testFrameworks += TestFramework("munit.TestFramework")
// publishTo := Some(Resolver.file("local-git",
// file("/Users/avandormael/Documents/ConvivaRepos/surgeon/releases")))
