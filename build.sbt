scalaVersion := "2.12.17"
version := "0.4.5"
name := "surgeon"
organization := "conviva"

lazy val surgeon = (
  Project("surgeon", file("surgeon"))
   .settings(
    libraryDependencies ++= List(
      "org.apache.spark" %% "spark-sql" % "3.4.0" exclude("org.slf4j", "slf4j-log4j12"),
      "org.scalameta" %% "munit" % "0.7.29" % Test,
     )
   )
)

publishArtifact := false
testFrameworks += TestFramework("munit.TestFramework")
// Global / semanticdbEnabled := true

lazy val docs = project
  .in(file("conviva-surgeon.wiki"))
  .dependsOn(surgeon)
  .enablePlugins(MdocPlugin)
