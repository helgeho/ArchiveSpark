import sbt.{ExclusionRule, url}
import sbt.Keys._

lazy val commonSettings = Seq(
  name := "archivespark",
  organization := "com.github.helgeho",
  version := "3.0.2",
  scalaVersion := "2.12.8",
  fork := true,
  exportJars := true
)

val circeVersion = "0.10.0"

val guava = "com.google.guava" % "guava" % "29.0-jre"

lazy val archivespark = (project in file("."))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      guava,
      "org.apache.commons" % "commons-compress" % "1.14",
      "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
      "org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
      "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided",
      "joda-time" % "joda-time" % "2.10",
      "org.apache.httpcomponents" % "httpclient" % "4.5.6",
      "org.netpreserve.commons" % "webarchive-commons" % "1.1.8" excludeAll (ExclusionRule(
        organization = "org.apache.hadoop",
        name = "hadoop-core"
      ),
      ExclusionRule(
        organization = "org.apache.httpcomponents",
        name = "httpcore"
      ),
      ExclusionRule(
        organization = "org.apache.httpcomponents",
        name = "httpclient"
      ),
      ExclusionRule(organization = "joda-time", name = "joda-time")),
      "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" % "provided",
      "org.brotli" % "dec" % "0.1.2"
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

assemblyShadeRules in assembly := Seq(
  ShadeRule
    .rename("com.google.common.**" -> "archivespark.shade.@0")
    .inLibrary(guava)
    .inProject
)

assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(includeScala = false, includeDependency = false)
assemblyOption in assemblyPackageDependency := (assemblyOption in assemblyPackageDependency).value
  .copy(includeScala = false)
