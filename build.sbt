scalaVersion := "2.12.17"
version := "0.0.6"
name := "surgeon"
organization := "conviva"

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

lazy val surgeon = (
  Project("surgeon", file("surgeon"))
   .settings(
    libraryDependencies ++= List(
      "org.apache.spark" %% "spark-sql" % "3.4.0" 
        exclude("org.slf4j", "slf4j-log4j12") 
        exclude("org.apache.parquet", "parquet.column")
        exclude("org.apache.parquet", "parquet-hadoop")
        exclude("org.apache.parquet", "parquet-column")
        exclude("com.twitter", "parquet-hadoop")
        exclude("com.twitter", "parquet-avro"),
      "com.conviva.vma" %% "hadoopApps" %  "2.240.0.6940" 
        exclude("org.apache.hadoop", "hadoop-core")
        exclude("org.apache.hadoop", "hadoop-client")
        exclude("com.conviva.vma", "setupTools")
        exclude("com.conviva.vma", "utils")
        exclude("com.conviva.jobConsoleCommons", "jobConsoleCommons")
        exclude("com.typesafe.scala-logging", "scala-logging"),
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      // "com.conviva.vma" %% "stdMetrics" % "2.241.0.36178"
      "com.conviva" %% "parquet-pb" % "2.158.0.37159"
      // "com.conviva.packetbrain" % "parquet-pb" % "9.1.0",
      // "com.conviva.3d" %% "3dReports" % "2.234.0.6914" exclude("org.slf4j", "slf4j-log4j12"),
      // "com.twitter" %% "algebird-core" % "0.13.8",
      // "com.conviva" %% "deviceMetadata_mapAdaptor" % "4.9.0",
      // "com.conviva" %% "connectionMetadata" % "4.9.0",
     )
   )
)

dependencyOverrides ++= Seq(
  "org.scala-lang.modules" % "scala-parser-combinators_2.12" % "1.0.4",
//   "com.google.guava" % "guava" % "12.0.1",
//   "org.apache.yetus" % "audience-annotations" % "0.5.0",
//   "org.apache.parquet" % "parquet-encoding" % "1.7.0",
//   "org.apache.parquet" % "parquet-common" % "1.7.0",
//   "org.apache.parquet" % "parquet-column" % "1.7.0",
//   "com.conviva.packetbrain" % "messages" % "9.1.0",
//   "com.conviva.packetbrain" % "log-utils" % "9.1.0",
//   "io.netty" % "netty-transport-native-epoll" % "4.1.63.Final",
//   "io.netty" % "netty-transport" % "4.1.63.Final",
//   "io.netty" % "netty-common" % "4.1.63.Final",
//   "io.netty" % "netty-transport-native-unix-common" % "4.1.63.Final",
//   "io.netty" % "netty-handler" % "4.1.63.Final",
//   "io.netty" % "netty-buffer" % "4.1.63.Final",
//   "io.netty" % "netty-codec" % "4.1.63.Final",
//   "io.netty" % "netty-resolver" % "4.1.63.Final",
//   "com.google.code.findbugs" % "jsr305" % "1.3.9",
//   "com.google.protobuf" % "protobuf-java" % "2.5.0",
//   "org.slf4j" % "slf4j-api" % "1.6.1",
//   "com.conviva" %% "connectionMetadata" % "4.9.0",
)


publishArtifact := false
testFrameworks += TestFramework("munit.TestFramework")
// Global / semanticdbEnabled := true

lazy val docs = project
  .in(file("conviva-surgeon.wiki"))
  .dependsOn(surgeon)
  .enablePlugins(MdocPlugin)
