
name := "spark-scala"
 
version := "1.0"
 
scalaVersion := "2.11.8"


 
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.1" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.1",
  "org.apache.spark" % "spark-hive_2.11" % "2.2.1",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.1",
  "org.apache.spark" % "spark-streaming-flume_2.11" % "2.2.1",
  "org.apache.spark" % "spark-mllib_2.11" % "2.2.1",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.1",
  "org.apache.commons" % "commons-lang3" % "3.7",
  "mysql" % "mysql-connector-java" % "5.1.46"
)

assemblyMergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
    case "about.html"  => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
}



