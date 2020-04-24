name := "Spark-Kinesis-Test"
version := "0.1"
scalaVersion := "2.11.0"
libraryDependencies ++= {
  val sparkVer = "2.3.1"
  Seq(
    "org.apache.spark" %% "spark-streaming-kinesis-asl" % sparkVer,
    "org.apache.spark" %% "spark-streaming"  % sparkVer  % "provided",
    "org.apache.spark" %% "spark-sql"  % sparkVer  % "provided",
    "org.apache.spark" %% "spark-core" % sparkVer  % "provided"
  )
}
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("main", "scala", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("org", "joda", xs@_*) => MergeStrategy.discard
  case PathList("org", "json", xs@_*) => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "plugin.xml" => MergeStrategy.last
  case "overview.html" => MergeStrategy.last
  case x => MergeStrategy.last
}
