import sbt.Keys._

lazy val commonSettings = Seq(
  organization := "de.l3s",
  version := "0.1.0",
  scalaVersion := "2.10.5",
  fork := true
)

lazy val archivespark = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "archivespark",
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.3.0",
      "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
      "org.apache.hadoop" % "hadoop-client" % "2.5.0" % "provided",
      "com.github.nscala-time" %% "nscala-time" % "2.0.0",
      "org.netpreserve.commons" % "webarchive-commons" % "1.1.5",
      "org.json4s" %% "json4s-native" % "3.2.11"
    ),
    resolvers ++= Seq(
      "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
      "internetarchive" at "http://builds.archive.org:8080/maven2"
    )
  )

assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheOutput = false)