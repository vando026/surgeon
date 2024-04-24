ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version := "0.1.3"
ThisBuild / organization := "conviva"
name := "surgeon"

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

lazy val surgeon = project 
   .in(file("surgeon"))
   .settings(
      name := "surgeon",
      libraryDependencies ++= List(
        "org.apache.spark" % "spark-sql_2.12" % "3.4.0" 
          exclude("org.slf4j", "slf4j-log4j12"),
        "org.scalameta" %% "munit" % "0.7.29" % Test,
        "com.conviva.packetbrain" % "parquet-pb" % "9.1.0" ,
        "com.conviva.3d" % "3dReports_2.12" % "2.246.0.6962" 
          exclude("org.apache.kafka", "kafka_2.12")
          exclude("org.slf4j", "slf4j-log4j12")
          exclude("com.conviva", "connectionMetadata_2.12")
          exclude("com.conviva", "deviceMetadata_mapAdaptor_2.12")
          exclude("com.twitter", "algebird-core_2.12")
          exclude("com.conviva.platform", "utils_2.12"),
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
