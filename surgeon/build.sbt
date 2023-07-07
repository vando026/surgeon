scalaVersion := "2.12.17"
version := "0.0.5"
name := "surgeon"
organization := "conviva"

// resolvers ++= Seq(
//   Resolver.mavenLocal,
// )

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "3.4.0" exclude("org.slf4j", "slf4j-log4j12"),
  "org.scalameta" %% "munit" % "0.7.29" % Test,
)

// publishArtifact := false
testFrameworks += TestFramework("munit.TestFramework")
// publishTo := Some(Resolver.file("local-git",
// file("/Users/avandormael/Documents/ConvivaRepos/surgeon/releases")))
