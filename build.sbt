ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version := "0.1.7"
ThisBuild / organization := "org"
name := "surgeon"

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

lazy val surgeon = project 
   .in(file("surgeon"))
   .settings(
      name := "surgeon",
      libraryDependencies ++= List(
        "org.apache.spark" % "spark-sql_2.12" % "3.4.0",
        "org.scalameta" %% "munit" % "0.7.29" % Test,
     ), 
)



publishArtifact := false
testFrameworks += TestFramework("munit.TestFramework")
// Global / semanticdbEnabled := true

lazy val docs = project
  .in(file("org-surgeon.wiki"))
  .settings(
    mdocVariables := Map("VERSION" -> version.value)
    )
  .dependsOn(surgeon)
  .enablePlugins(MdocPlugin)
