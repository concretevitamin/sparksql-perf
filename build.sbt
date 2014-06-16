import AssemblyKeys._

assemblySettings

name := "sparksql-perf"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % "1.1.0-SNAPSHOT",
  "org.apache.spark" %% "catalyst"        % "1.1.0-SNAPSHOT",
  "org.apache.spark" %% "spark-hive"      % "1.1.0-SNAPSHOT",
  "org.apache.spark" %% "spark-sql"       % "1.1.0-SNAPSHOT",
  "org.scalatest"    %% "scalatest"       % "1.9.1"             % "test"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/",
  "Spray" at "http://repo.spray.cc"
)
