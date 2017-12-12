import sbt.{ExclusionRule, url}
import sbt.Keys._

lazy val commonSettings = Seq(
  name := "archivespark",
  organization := "com.github.helgeho",
  version := "2.7.1",
  scalaVersion := "2.11.7",
  fork := true
)

lazy val archivespark = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.2.0" % "provided" excludeAll(
        ExclusionRule(organization = "org.apache.httpcomponents", name = "httpclient"),
        ExclusionRule(organization = "org.apache.httpcomponents", name = "httpcore")),
      "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided"  excludeAll(
        ExclusionRule(organization = "org.apache.httpcomponents", name = "httpclient"),
        ExclusionRule(organization = "org.apache.httpcomponents", name = "httpcore")),
      "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" % "provided",
      "org.elasticsearch" %% "elasticsearch-spark" % "2.2.0" % "provided",
      "org.json4s" %% "json4s-native" % "3.5.3",
      "org.apache.httpcomponents" % "httpclient" % "4.2.2",
      "org.apache.httpcomponents" % "httpcore" % "4.2.2",
      "org.netpreserve.commons" % "webarchive-commons" % "1.1.8" excludeAll(
        ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-core"),
        ExclusionRule(organization = "com.google.guava", name = "guava"),
        ExclusionRule(organization = "org.apache.httpcomponents", name = "httpcore")),
      "org.scalatest" %% "scalatest" % "2.2.6" % Test,
      "org.jsoup" % "jsoup" % "1.11.2"
    ),
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
        email = "holzmann@L3S.de",
        url   = url("http://www.HelgeHolzmann.de")
      )
    ),
    licenses := Seq("MIT" -> url("http://www.opensource.org/licenses/mit-license.php"))
  )

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
