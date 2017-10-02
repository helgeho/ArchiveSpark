import sbt.ExclusionRule
import sbt.Keys._

lazy val commonSettings = Seq(
  name := "archivespark",
  organization := "de.l3s",
  version := "2.5",
  scalaVersion := "2.11.7",
  fork := true
)

lazy val archivespark = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.2.0" % "provided" excludeAll(
        ExclusionRule(organization = "org.apache.hadoop"),
        ExclusionRule(organization = "org.scala-lang"),
        ExclusionRule(organization = "com.google.guava")),
      "org.apache.hadoop" % "hadoop-client" % "2.5.0" % "provided",
      "org.netpreserve.commons" % "webarchive-commons" % "1.1.5" excludeAll(
        ExclusionRule(organization = "org.apache.hadoop"),
        ExclusionRule(organization = "com.google.guava")),
      "commons-io" % "commons-io" % "2.4",
      "org.json4s" %% "json4s-native" % "3.2.11" excludeAll ExclusionRule(organization = "org.scala-lang"),
      "org.jsoup" % "jsoup" % "1.10.1",
      "com.google.protobuf" % "protobuf-java" % "2.6.1",
      "org.apache.httpcomponents" % "httpclient" % "4.5.1",
      "org.apache.httpcomponents" % "httpcore" % "4.4.4",
      "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" % "provided",
      "org.elasticsearch" %% "elasticsearch-spark" % "2.2.0" % "provided",
      "org.scalatest" %% "scalatest" % "2.2.6" % Test
    ),
    resolvers ++= Seq(
      "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
      "internetarchive" at "http://builds.archive.org/maven2"
    ),
    publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository"))),
    pomExtra :=
      <licenses>
        <license>
          <name>MIT License</name>
          <url>http://www.opensource.org/licenses/mit-license.php</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
  )

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
