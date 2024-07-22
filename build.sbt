ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version := "0.1.6"
ThisBuild / organization := "conviva"
name := "surgeon"

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

lazy val surgeon = project 
   .in(file("surgeon"))
   .settings(
      name := "surgeon",
      libraryDependencies ++= List(
        "org.apache.spark" % "spark-sql_2.12" % "3.4.0",
        "org.scalameta" %% "munit" % "0.7.29" % Test,
        // "com.google.guava" % "guava" % "31.0.1-jre" ,
        // "com.conviva.packetbrain" % "pbutils" % "10.0.2" ,
        "com.conviva.packetbrain" % "parquet-pb" % "10.0.2" ,
        // "com.conviva.packetbrain" % "log-utils" % "10.0.2" ,
        // "com.conviva.packetbrain" % "messages" % "10.0.2" ,
        // "com.conviva" % "packetbrain" % "10.0.2" ,
        // "org.apache.parquet" % "parquet-common" % "1.13.1" ,
        // "org.apache.parquet" % "parquet-encoding" % "1.13.1" ,
        // "com.conviva.3d" % "3dReports_2.12" % "2.246.0.6962" 
        //   exclude("org.apache.kafka", "kafka_2.12")
        //   exclude("org.slf4j", "slf4j-log4j12")
        //   exclude("com.conviva", "connectionMetadata_2.12")
        //   exclude("com.conviva", "deviceMetadata_mapAdaptor_2.12")
        //   exclude("com.twitter", "algebird-core_2.12")
        //   exclude("com.conviva.platform", "utils_2.12"),
     ), 
)



publishArtifact := false
testFrameworks += TestFramework("munit.TestFramework")
// Global / semanticdbEnabled := true

lazy val docs = project
  .in(file("conviva-surgeon.wiki"))
  .settings(
    mdocVariables := Map("VERSION" -> version.value)
    )
  .dependsOn(surgeon)
  .enablePlugins(MdocPlugin)
