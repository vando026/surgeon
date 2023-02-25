scalaVersion := "2.12.13"
version := "0.0.1"
name := "surgeon"
organization := "conviva"

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "org.scalameta" %% "munit" % "0.7.29" % Test,
  "com.databricks" %% "dbutils-api" % "0.0.6"
)

// resolvers ++= Seq(
//   "convivamaven" at "https://usconviva.jfrog.io/artifactory/eng-mvn-release-local"
// )

// unmanagedBase := baseDirectory.value / "jars"

publishArtifact := false
testFrameworks += TestFramework("munit.TestFramework")
// Global / semanticdbEnabled := true

// https://github.com/cchantep/sbt-scaladoc-compiler/
// This compiles code in examples
// resolvers ++= Seq(
  // "Tatami Releases" at "https://raw.github.com/cchantep/tatami/master/releases")

// addSbtPlugin("cchantep" % "sbt-scaladoc-compiler" % "0.3")
