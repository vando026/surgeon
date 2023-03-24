scalaVersion := "2.12.13"
version := "0.0.2"
name := "surgeon"
organization := "conviva"

resolvers ++= Seq(
  Resolver.mavenLocal,
  DefaultMavenRepository,
  "sbt-ivy-releases" at "https://repo1.maven.org/maven2/",
  "sbt-maven-releases" at "https://repo1.maven.org/maven2/",
  "usconviva-sbt-ivy-releases" at "https://usconviva.jfrog.io/usconviva/sbt-ivy-releases/",
  "usconviva-eng-mvn-all" at "https://usconviva.jfrog.io/usconviva/eng-mvn-all/"
)

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "com.conviva.packetbrain" % "parquet-pb" % "9.1.0",
  "com.conviva.3d" %% "3dReports" % "2.228.0.6879",
  "com.twitter" %% "algebird-core" % "0.13.9", 
  "com.conviva" %% "deviceMetadata_mapAdaptor" % "4.9.0",
  "com.conviva" %% "connectionMetadata" % "4.9.0",
  "org.scalameta" %% "munit" % "0.7.29" % Test,
)

publishArtifact := false
testFrameworks += TestFramework("munit.TestFramework")

// unmanagedBase := new java.io.File("/Users/avandormael/miniconda3/envs/dbconnect/lib/python3.9/site-packages/pyspark/jars")
