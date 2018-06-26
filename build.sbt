name := "sparkgtfscleaner"
version := "1.0"
scalaVersion := "2.11.8"

val scalaBinaryVersion = "2.11"
val sparkVersion = "2.1.0"
val hadoopVersion = "2.8.3"
val jmockitVersion = "1.34"
val typeSafeConfigVersion = "1.3.3"
val scalaLoggingVersion = "3.9.0"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided",
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion  % "provided",
    "org.jmockit" % "jmockit" % jmockitVersion % "test",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    "com.typesafe" % "config" % typeSafeConfigVersion,
    "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion
)