import sbt.Keys._

lazy val commonSettings = Seq(
  name := "archivespark-benchmarking",
  organization := "de.l3s",
  version := "0.1.0",
  scalaVersion := "2.10.5",
  fork := true
)

lazy val archivesparkBenchmarking = (project in file(".")).
  settings(commonSettings: _*).dependsOn(file("../..")).
  settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.5.2" % "provided" excludeAll(
        ExclusionRule(organization = "org.apache.hadoop"),
        ExclusionRule(organization = "org.scala-lang"),
        ExclusionRule(organization = "com.google.guava")),
      "org.apache.hadoop" % "hadoop-client" % "2.5.0" % "provided",
      "org.apache.hbase" % "hbase" % "1.1.2" % "provided",
      "org.apache.hbase" % "hbase-common" % "1.1.2" % "provided",
      "org.apache.hbase" % "hbase-client" % "1.1.2" % "provided",
      "org.apache.hbase" % "hbase-server" % "1.1.2" % "provided",
      "org.apache.hbase" % "hbase-protocol" % "1.1.2" % "provided"
    )
  )

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}