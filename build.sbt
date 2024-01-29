import sbt._
import sbt.Keys._

lazy val commonSettings = Seq(
  name := "archivespark",
  organization := "org.archive.webservices",
  version := "3.3.8-SNAPSHOT",
  scalaVersion := "2.12.8",
  fork := true,
  exportJars := true
)

lazy val archivespark = (project in file("."))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
      "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided",
      "org.archive.webservices" %% "sparkling" % "0.3.8-SNAPSHOT" % "provided",
      "edu.stanford.nlp" % "stanford-corenlp" % "4.3.1" % "provided"
    ),
    homepage := Some(url("https://github.com/internetarchive/ArchiveSpark")),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/internetarchive/ArchiveSpark"),
        "scm:git@github.com:internetarchive/ArchiveSpark.git"
      )
    ),
    developers := List(
      Developer(
        id = "helgeho",
        name = "Helge Holzmann",
        email = "helge@archive.org",
        url = url("http://www.HelgeHolzmann.de")
      )
    ),
    licenses := Seq(
      "MIT" -> url("http://www.opensource.org/licenses/mit-license.php")
    )
  )

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
