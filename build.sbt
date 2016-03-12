name := "spark-mlib-project"

version := "1.0"

mainClass := Some("ActivityTracker")


scalaVersion := "2.10.2"
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.0",
  "mysql" % "mysql-connector-java" % "5.1.34",
  "org.apache.commons" % "commons-lang3" % "3.3.2"
)
