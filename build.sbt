import sbt.Keys._

lazy val commonSettings = Seq(
  name := "archivespark",
  organization := "de.l3s",
  version := "0.1.0",
  scalaVersion := "2.10.5",
  fork := true
)

lazy val archivespark = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.3.0" % "provided" excludeAll(
        ExclusionRule(organization = "org.apache.hadoop"),
        ExclusionRule(organization = "org.scala-lang")),
      "org.apache.hadoop" % "hadoop-client" % "2.5.0" % "provided" excludeAll(
        ExclusionRule(organization = "org.apache.spark"),
        ExclusionRule(organization = "org.spark-project"),
        ExclusionRule(organization = "org.scala-lang")),
      "com.github.nscala-time" %% "nscala-time" % "2.0.0" excludeAll(
        ExclusionRule(organization = "org.apache.spark"),
        ExclusionRule(organization = "org.spark-project"),
        ExclusionRule(organization = "org.scala-lang"),
        ExclusionRule(organization = "org.apache.hadoop")),
      "org.netpreserve.commons" % "webarchive-commons" % "1.1.5" excludeAll(
        ExclusionRule(organization = "org.apache.spark"),
        ExclusionRule(organization = "org.spark-project"),
        ExclusionRule(organization = "org.scala-lang"),
        ExclusionRule(organization = "org.apache.hadoop")),
      "org.json4s" %% "json4s-native" % "3.2.11" excludeAll(
        ExclusionRule(organization = "org.apache.spark"),
        ExclusionRule(organization = "org.spark-project"),
        ExclusionRule(organization = "org.scala-lang"),
        ExclusionRule(organization = "org.apache.hadoop"))
    ),
    resolvers ++= Seq(
      "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
      "internetarchive" at "http://builds.archive.org:8080/maven2"
    )
  )

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, cacheOutput = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}