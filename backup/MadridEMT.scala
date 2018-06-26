package com.alstom.Launcher.Configurations

import com.alstom.GTFSOperations.IOOperations.{fs, getFileNameAndExtFromPath}
import com.alstom.GTFSOperations.{GTFSMethods, IOOperations, UDFS}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, first, last, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MadridEMT  {

 def Configure (workpath: String, urlfile: String): Unit = {

  val DownloadURL = urlfile
  val ResultURL = "stif/stif_gtfs_clean.zip"
  val WorkPath = workpath
  val DownloadFileName = "stif_gtfs.zip"
  val ProcessedFileName = "stif_gtfs_clean.zip"
}

 def Process(workPath: String, backupPath: String, sourcesPath: String, rawPath: String, urlfile: String, spark: SparkSession) = {

   println("MadridEMT GTFS Processing")
   println("URL: " + urlfile)
   println("-----------------------------------------------")
   println("")

   println("Start Clean environment")
   IOOperations.CleanWorkingDirectory(workPath)
   println("Finish Clean environment")
   println("")
   IOOperations.ExtractFiles(urlfile, "madrid_emt_gtfs", workPath, backupPath, sourcesPath, spark)

   var dataframes = FixOperations(sourcesPath, "madrid_emt_gtfs", spark)

   println("Start Uploading result to Azure Storage")
   UploadAzure(dataframes, rawPath, spark)
   println("Finish Uploading result to Azure Storage")

}

  def FixOperations(sourcesPath: String, fileName: String, spark: SparkSession) : List[DataFrame] = {

    val sources_path = sourcesPath.concat("GTFSCLEAN/" + fileName + "/")
    var dataframes = new mutable.ListBuffer[DataFrame]

    println("Start Remove Duplicates")
    val routes = spark
      .read.parquet(sources_path.concat("routes.parquet")).dropDuplicates()
    val trips = spark
      .read.parquet(sources_path.concat("trips.parquet")).dropDuplicates()
    val stop_times = spark
      .read.parquet(sources_path.concat("stop_times.parquet")).dropDuplicates()
    val stops = spark
      .read.parquet(sources_path.concat("stops.parquet")).dropDuplicates()
    val agency = spark
      .read.parquet(sources_path.concat("agency.parquet")).dropDuplicates()
    val calendar_dates = spark
      .read.parquet(sources_path.concat("calendar_dates.parquet")).dropDuplicates()
    val calendar = spark
      .read.parquet(sources_path.concat("calendar.parquet")).dropDuplicates()
    var shapes = spark
      .read.parquet(sources_path.concat("shapes.parquet")).dropDuplicates()
    val frequencies = spark
      .read.parquet(sources_path.concat("frequencies.parquet")).dropDuplicates()

    println("Finish Remove Duplicates")
    println("")

    println("Start Remove invalid route colors")
    val routes_fixColor = routes.withColumn("route_color", when(col("route_color").equalTo("-16680772"), null).
      otherwise(col("route_color")))
    println("Finish Remove invalid route colors")
    println("")

    println("Start Apply random color to route based on id")

    var routes_newDF = routes_fixColor.withColumn("route_color", when(
      col("route_color").equalTo("000000") ||
        col("route_color").equalTo("-16777216") ||
        col("route_color").isNull, UDFS.hexToLong(col("route_color"))).
      otherwise(col("route_color")))
    //routes_newDF = routes_newDF.withColumn("route_color", UDFS.fromStrToHex(col("route_color")))

    println("Finish Apply random color to route based on id")
    println("")

    dataframes += (routes_newDF,stops,trips,stop_times,agency,calendar_dates,calendar,shapes, frequencies)

    dataframes.toList

  }

  def UploadAzure(dataframes: List[DataFrame], rawPath: String, spark: SparkSession) = {

    import org.apache.hadoop.fs.{FileSystem, Path}
    val raw_path = rawPath.concat("GTFSCLEAN/" + "madrid_emt_gtfs/")
    val routes = dataframes(0)
    val stops = dataframes(1)
    val trips = dataframes(2)
    val stop_times = dataframes(3)
    val agency = dataframes(4)
    val calendar_dates = dataframes(5)
    val calendar = dataframes(6)
    val shapes = dataframes(7)
    val frequencies = dataframes(8)

    import org.apache.hadoop.fs.FileStatus
    import java.io.IOException
    import java.util

    @throws[IOException]
    def copyMerge(srcFS: FileSystem, srcDir: Path, dstFS: FileSystem, dstFile: Path, deleteSource: Boolean, conf: Configuration, addString: String) = {


      val out = dstFS.create(dstFile)
      try {
        val contents = srcFS.listStatus(srcDir)
        var i = 0
        while ( {
          i < contents.length
        }) {
          if (contents(i).isFile) {
            val in = srcFS.open(contents(i).getPath)
            try {
              org.apache.hadoop.io.IOUtils.copyBytes(in, out, conf, false)
              if (addString != null) out.write(addString.getBytes("UTF-8"))
            } finally in.close
          }

          {
            i += 1; i - 1
          }
        }
      } finally out.close
      if (deleteSource) srcFS.delete(srcDir, true)
      else true
    }

    def merge(srcPath: ArrayBuffer[String], dstPath: String): Unit =  {

      val hadoopConfig = new Configuration()
      val hdfs = FileSystem.get(hadoopConfig)
      for(df <- srcPath) {
        val (fileName, fileExtension) = getFileNameAndExtFromPath(df.toString)
        copyMerge(hdfs, new Path(df), hdfs, new Path(dstPath + fileName + ".csv"), true, hadoopConfig, null)
      }
      // the "true" setting deletes the source files once they are merged into the new output
    }

    routes.coalesce(1).write.mode("overwrite").parquet(raw_path + "routes.parquet")
    println("routes uploaded")
    stops.coalesce(1).write.mode("overwrite").parquet(raw_path + "stops.parquet")
    println("stops uploaded")
    trips.coalesce(1).write.mode("overwrite").parquet(raw_path + "trips.parquet")
    println("trips uploaded")

    stop_times.write.mode("overwrite").parquet(raw_path + "stop_times.parquet")
    println("stop_times uploaded")
    shapes.orderBy("shape_id","shape_pt_sequence").write.mode("overwrite").parquet(raw_path + "shapes.parquet")
    println("shapes uploaded")

    agency.coalesce(1).write.mode("overwrite").parquet(raw_path + "agency.parquet")
    println("agency uploaded")
    calendar_dates.coalesce(1).write.mode("overwrite").parquet(raw_path + "calendar_dates.parquet")
    println("calendar_dates uploaded")
    calendar.coalesce(1).write.mode("overwrite").parquet(raw_path + "calendar.parquet")
    println("calendar uploaded")
    // Write in raw path

    frequencies.coalesce(1).write.mode("overwrite").parquet(raw_path + "frequencies.parquet")
    println("frequencies uploaded")

    /*
        fare_attributes match {
          case Some(fare) => fare.coalesce(1).write.mode("overwrite").parquet(raw_path + "fare_attributes.parquet")
            println("fare_attributes uploaded")

          case None => println("No fare_attributes found")
        }

        fare_rules match {
          case Some(fareRule) => fareRule.coalesce(1).write.mode("overwrite").parquet(raw_path + "fare_rules.parquet")
            println("fare_rules uploaded")

          case None => println("No fare_rules found")
        }

        feed_info match {
          case Some(feed) => feed.coalesce(1).write.mode("overwrite").parquet(raw_path + "feed_info.parquet")
            println("feed_info uploaded")

          case None => println("No feed_info found")
        }

        frequencies match {
          case Some(freq) => freq.coalesce(1).write.mode("overwrite").parquet(raw_path + "frequencies.parquet")
            println("frequencies uploaded")

          case None => println("No frequencies found")
        }


        val listOfFiles = fs.listStatus(new Path(raw_path))
        val filesPaths_list = ArrayBuffer[String]()
        listOfFiles.foreach(x => filesPaths_list += x.getPath.toString)
        merge(filesPaths_list, "adl:///data/normal/")
        */
  }

}

