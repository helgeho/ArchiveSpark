import sbt.{ExclusionRule, url}
import sbt.Keys._

lazy val commonSettings = Seq(
  name := "archivespark",
  organization := "com.github.helgeho",
  version := "3.0.3-MMISIEWICZ-SNAPSHOT",
  scalaVersion := "2.11.12",
  fork := true,
  exportJars := true
)

val circeVersion = "0.10.0"

lazy val archivespark = (project in file("."))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % "2.7.2" % "provided",
      "org.apache.hadoop" % "hadoop-aws" % "2.7.2" % "provided",
      "org.apache.spark" %% "spark-core" % "2.4.4" % "provided",
      "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided",
      "joda-time" % "joda-time" % "2.10",
      "org.apache.httpcomponents" % "httpclient" % "4.5.6",
      "org.netpreserve.commons" % "webarchive-commons" % "1.1.8" excludeAll(
        ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-core"),
        ExclusionRule(organization = "com.google.guava", name = "guava"),
        ExclusionRule(organization = "org.apache.httpcomponents", name = "httpcore"),
        ExclusionRule(organization = "org.apache.httpcomponents", name = "httpclient"),
        ExclusionRule(organization = "joda-time", name = "joda-time")),
      "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" % "provided"
    ) ++ Seq(
      "io.circe" %% "circe-core",
      "io.circe" %% "circe-generic",
      "io.circe" %% "circe-parser"
    ).map(_ % circeVersion),
    publishTo := Some(
      if (isSnapshot.value)
        Opts.resolver.sonatypeSnapshots
      else
        Opts.resolver.sonatypeStaging
    ),
    publishMavenStyle := true,
    publishArtifact in Test := false,
    homepage := Some(url("https://github.com/helgeho/ArchiveSpark")),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/helgeho/ArchiveSpark"),
        "scm:git@github.com:helgeho/ArchiveSpark.git"
      )
    ),
    developers := List(
      Developer(
        id    = "helgeho",
        name  = "Helge Holzmann",
        email = "helge@archive.org",
        url   = url("http://www.HelgeHolzmann.de")
      )
    ),
    licenses := Seq("MIT" -> url("http://www.opensource.org/licenses/mit-license.php"))
  )

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false)
assemblyOption in assemblyPackageDependency := (assemblyOption in assemblyPackageDependency).value.copy(includeScala = false)
