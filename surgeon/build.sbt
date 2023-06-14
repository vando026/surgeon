scalaVersion := "2.12.15"
version := "0.0.4.4"
name := "surgeon"
organization := "conviva"

// resolvers ++= Seq(
//   Resolver.mavenLocal,
// )

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "3.3.0" exclude("org.slf4j", "slf4j-log4j12"),
  "org.scalameta" %% "munit" % "0.7.29" % Test,
)

publishArtifact := false
testFrameworks += TestFramework("munit.TestFramework")
