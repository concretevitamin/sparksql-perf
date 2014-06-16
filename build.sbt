import AssemblyKeys._

assemblySettings

name := "sparksql-perf"

version := "0.1"

scalaVersion := "2.10.3"

// TODO: change to SNAPSHOT for local testing.
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.0.0",
  "org.apache.spark" %% "spark-catalyst" % "1.0.0",
  "org.apache.spark" %% "spark-hive" % "1.0.0",
  "org.apache.spark" %% "spark-sql" % "1.0.0"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/",
  "Spray" at "http://repo.spray.cc"
)
