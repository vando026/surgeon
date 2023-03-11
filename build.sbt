scalaVersion := "2.12.13"
version := "0.0.1"
name := "surgeon"
organization := "conviva"

lazy val common = (
  Project("common", file("common"))
   .settings(
    libraryDependencies ++= List(
      "org.apache.spark" %% "spark-sql" % "3.3.1",
      "org.scalameta" %% "munit" % "0.7.29" % Test,
     )
   )
)

// resolvers ++= Seq(
//   "convivamaven" at "https://usconviva.jfrog.io/artifactory/eng-mvn-release-local"
// )

// unmanagedBase := baseDirectory.value / "jars"

publishArtifact := false
testFrameworks += TestFramework("munit.TestFramework")
// Global / semanticdbEnabled := true

lazy val docs = project
  .in(file("surgeon_docs"))
  .dependsOn(common)
  .enablePlugins(MdocPlugin)
