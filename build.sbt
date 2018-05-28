name := "sparkgtfscleaner"
version := "1.0"
scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.11" % "2.1.0",
    "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
    //"org.scala-lang" % "scala-reflect" % "2.11.8",
    //"org.onebusaway" % "onebusaway-gtfs" % "1.3.4",
    //"org.onebusaway" % "onebusaway-gtfs-modules" % "1.3.4",
    //"org.onebusaway" % "onebusaway-gtfs-merge" % "1.3.4",
    //"org.onebusaway" % "onebusaway-gtfs-transformer" % "1.2.0",
    //"ch.qos.logback" % "logback-classic" % "1.1.3",
    "org.apache.hadoop" % "hadoop-common" % "3.0.0" % "provided",
    "org.jmockit" % "jmockit" % "1.34" % "test",
    "org.apache.spark" %% "spark-hive" % "2.1.0"
)