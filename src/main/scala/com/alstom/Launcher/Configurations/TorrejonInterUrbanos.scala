package com.alstom.Launcher.Configurations

import com.alstom.GTFSOperations.IOOperations._
import com.alstom.GTFSOperations.{IOOperations, UDFS}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class TorrejonInterUrbanos  {


 def Process(workPath: String, backupPath: String, sourcesPath: String, rawPath: String, urlfile: String, spark: SparkSession) = {

   println("Torrejon InterUrbanos GTFS Processing")
   println("URL: " + urlfile)
   println("-----------------------------------------------")
   println("")

   println("Start Clean environment")
   IOOperations.CleanWorkingDirectory(workPath)
   println("Finish Clean environment")
   println("")
   IOOperations.ExtractFiles(urlfile, "torrejon_interurbanos_gtfs", workPath, backupPath, sourcesPath, spark)

   var dataframes = FixOperations(sourcesPath, "torrejon_interurbanos_gtfs", spark)

   println("Start Uploading result to Azure Storage")
   UploadAzure(dataframes, rawPath, spark)
   println("Finish Uploading result to Azure Storage")

}

  def FixOperations(sourcesPath: String, fileName:String, spark: SparkSession) : List[DataFrame] = {
    import spark.implicits._
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
    var fare_attributes = spark
      .read.parquet(sources_path.concat("fare_attributes.parquet")).dropDuplicates()
    var fare_rules = spark
      .read.parquet(sources_path.concat("fare_rules.parquet")).dropDuplicates()
    var feed_info = spark
      .read.parquet(sources_path.concat("feed_info.parquet")).dropDuplicates()
    var frequencies = spark
      .read.parquet(sources_path.concat("frequencies.parquet")).dropDuplicates()

    println("Finish Remove Duplicates")
    println("")

    println("Start Apply random color to route based on id and filterin based on selected routes")
    val routeList = List[String]("9__2__148_",
      "9__3__148_",
      "9__4__148_",
      "9__6__148_",
      "9__1_A_148_",
      "9__1_B_148_",
      "8__223___",
      "8__224___",
      "8__224_A__",
      "8__226___",
      "8__824___",
      "8__220___",
      "8__251___",
      "8__252___",
      "8__261___",
      "8__340___",
      "8_N_202___"
    )
    var routes_newDF = routes.filter($"route_id".isin(routeList:_*)).withColumn("route_color", when(
      col("route_color").equalTo("000000") ||
        col("route_color").equalTo("-16777216") ||
        col("route_color").isNull, UDFS.hexToLong(col("route_color"))).
      otherwise(col("route_color")))
    //routes_newDF = routes_newDF.withColumn("route_color", UDFS.fromStrToHex(col("route_color")))

    println("Finish Apply random color to route based on id")
    println("")

    println("Leaving only C2 and C7 routes")

    val trips_newDF = trips.filter($"route_id".isin(routeList:_*))
    dataframes += (routes_newDF,stops,trips_newDF,stop_times,agency,calendar_dates,calendar,shapes,fare_attributes,fare_rules,feed_info,frequencies)

    dataframes.toList

  }

  def UploadAzure(dataframes: List[DataFrame], rawPath: String, spark: SparkSession) = {

    import org.apache.hadoop.fs.{FileSystem, Path}
    val raw_path = rawPath.concat("GTFSCLEAN/" + "torrejon_interurbanos_gtfs/")
    val routes = dataframes(0)
    val stops = dataframes(1)
    val trips = dataframes(2)
    val stop_times = dataframes(3)
    val agency = dataframes(4)
    val calendar_dates = dataframes(5)
    val calendar = dataframes(6)
    val shapes = dataframes(7)
    val fare_attributes = dataframes(8)
    val fare_rules = dataframes(9)
    val feed_info = dataframes(10)
    val frequencies = dataframes(11)

    import java.io.IOException


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

    fare_attributes.coalesce(1).write.mode("overwrite").parquet(raw_path + "fare_attributes.parquet")
    println("fare_attributes uploaded")

    fare_rules.coalesce(1).write.mode("overwrite").parquet(raw_path + "fare_rules.parquet")
    println("fare_rules uploaded")

    feed_info.coalesce(1).write.mode("overwrite").parquet(raw_path + "feed_info.parquet")
    println("feed_info uploaded")

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

