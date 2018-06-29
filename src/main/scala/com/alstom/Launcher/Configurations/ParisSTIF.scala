package com.alstom.Launcher.Configurations

import com.alstom.GTFSOperations.IOOperations.{getFileNameAndExtFromPath, _}
import com.alstom.GTFSOperations.{GTFSMethods, IOOperations, UDFS}
import com.alstom.utils.paris.ParisUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

  class ParisSTIF {

   def Process(workPath: String, backupPath: String, sourcesPath: String, rawPath: String, urlfile: String, geoJsonPath: List[(String,String)], spark: SparkSession) = {

     println("STIF GTFS Processing")
     println("URL: " + urlfile)
     println("-----------------------------------------------")
     println("")
     println("Start Clean environment")

     IOOperations.CleanWorkingDirectory(workPath)
     //IOOperations.CleanWorkingDirectory(backupPath)
     println("Finish Clean environment")
     println("")

     //fs.delete(new Path("adl:///data/staging/GTFSCLEAN/GeoJson/"), true)

     IOOperations.ExtractFiles(urlfile, "stif_gtfs", workPath, backupPath, sourcesPath, spark)

     var dataframes = FixOperations(sourcesPath, "stif_gtfs", spark)
     println("Start Complete shapes point by point")
     dataframes = CompleteShapesPointByPoint(dataframes, "paris", spark)
     println("Finish Complete shapes point by point")
     println("")

     println("Start download GeoJson")
     val outputPath = workPath.concat("GeoJson")
     try {
       createDirectory(outputPath)

       for(geojson <- geoJsonPath) {
         IOOperations.DownloadGeoJson(geojson._1, geojson._2, outputPath)
       }
     }
     catch {
       case e: Throwable => println("Could not download geojson file"+ e)
         sys.exit(1)
     }
     println("Finish download GeoJson")
     println("")

     println("Start Add Shapes from GeoJson")
     val geoJsonPathsList = getGeoJsonPathList(outputPath)
     val originalShapes = dataframes(9)
     for (geojsonPath <- geoJsonPathsList) {

       val (fileName, fileExtension) = IOOperations.getFileNameAndExtFromPath(geojsonPath.toString)
       println(fileName)

       dataframes = GTFSMethods.AddGeoJsonToReplaceShapes(dataframes, fileName,  geojsonPath, originalShapes, spark)

     }
     println("Finish Add Shapes from GeoJson")

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
      val transfers = spark
        .read.parquet(sources_path.concat("transfers.parquet")).dropDuplicates()
      val stop_extensions = spark
        .read.parquet(sources_path.concat("stop_extensions.parquet")).dropDuplicates()

      println("Finish Remove Duplicates")
      println("")

      println("Start Fix Data")

      // new StopPoint:8727147:810:B,"GARE DE BLANC MESNIL","",48.932305,2.475674,3,,0,StopArea:8727147,1
      //replace por (este es el malo) ->StopPoint:8700147:810:B|0            |49.004687|2.570589

      val stops_newDF =
        stops.withColumn("stop_lat", when(col("stop_id").equalTo("StopPoint:49:49590"), "48.562536").otherwise(col("stop_lat")))
          .withColumn("stop_lon", when(col("stop_id").equalTo("StopPoint:49:49590"), "2.298378").otherwise(col("stop_lon")))
          .withColumn("stop_lat", when(col("stop_id").equalTo("StopPoint:49:53971"), "48.522447").otherwise(col("stop_lat")))
          .withColumn("stop_lon", when(col("stop_id").equalTo("StopPoint:49:53971"), "2.261807").otherwise(col("stop_lon")))
          .withColumn("stop_lat", when(col("stop_id").equalTo("StopArea: 49:49590"), "48.562536").otherwise(col("stop_lat")))
          .withColumn("stop_lon", when(col("stop_id").equalTo("StopArea: 49:49590"), "2.298378").otherwise(col("stop_lon")))
          .withColumn("stop_lat", when(col("stop_id").equalTo("StopArea: 49:53971"), "48.522447").otherwise(col("stop_lat")))
          .withColumn("stop_lon", when(col("stop_id").equalTo("StopArea: 49:53971"), "2.261807").otherwise(col("stop_lon")))

      println("Finish Fix Data")
      println("")

      println("Start Apply random color to route based on id")

      val routes_newDF = routes.withColumn("route_color", when(
        col("route_color").equalTo("000000") ||
          col("route_color").equalTo("-16777216") ||
          col("route_color").isNull, UDFS.hexToLong(col("route_color"))).
        otherwise(col("route_color")))
      //routes_newDF = routes_newDF.withColumn("route_color", UDFS.fromStrToHex(col("route_color")))

      println("Finish Apply random color to route based on id")
      println("")

      dataframes += (routes_newDF,stops_newDF,trips,stop_times,agency,calendar_dates,calendar,transfers,stop_extensions)

      dataframes.toList

    }

    def CompleteShapesPointByPoint(dataframes: List[org.apache.spark.sql.DataFrame], location: String, spark: SparkSession) : List[org.apache.spark.sql.DataFrame] = {

      var dataframesOutput = new mutable.ListBuffer[DataFrame]

      import spark.implicits._
      val routes = dataframes(0)
      val stops = dataframes(1)
      val trips = dataframes(2)
      val stop_times = dataframes(3)
      val agency = dataframes(4)
      val calendar_dates = dataframes(5)
      val calendar = dataframes(6)
      val transfers = dataframes(7)
      val stop_extensions = dataframes(8)

      // Select util fields
      val routes_selected_fields = routes.select("route_id","route_type")
      val trips_selected_fields = trips.select("route_id","trip_id")
      val stop_times_selected_fields = stop_times.select("trip_id","stop_id","stop_sequence")
      val stops_selected_fields = stops.select("stop_id","stop_lat","stop_lon")

      // Dict trips & routes
      val dict_trips_routes = trips_selected_fields.select("trip_id","route_id")

      // Dict tripId-shapeId

      val tmp_trips = stop_times_selected_fields.withColumn("stop_sequence", 'stop_sequence.cast("int")).
        withColumn("stopId_stopSequence", concat_ws(";",$"stop_sequence",$"stop_id")).
        withColumn("listToHash", collect_list("stopId_stopSequence").over(Window.partitionBy("trip_id")
          .orderBy("stop_sequence"))).
        withColumn("listsToMapSize", size($"listToHash"))

      val tripMaxSize = tmp_trips.groupBy("trip_id").agg(max("listsToMapSize")alias("ListsToMapSize"))
      val joinMaxSizeTrips = tripMaxSize.join(tmp_trips,Seq("trip_id","ListsToMapSize"))
      val dict_tripId_shapeId = joinMaxSizeTrips.select("trip_id","listToHash").
        withColumn("shape_id", hash(concat_ws(";", $"listToHash"))).
        select("trip_id","shape_id")

      val wSpec2 = Window.partitionBy("shape_id").orderBy("stop_sequence")
      val stopTimes_WithShapeId = stop_times_selected_fields.join(dict_tripId_shapeId, "trip_id").
        select("stop_id","stop_sequence","shape_id").distinct()

      // Shape Id filtered by route_type = 2
      val shapesId_type2 = routes_selected_fields.filter($"route_type" === 2).join(trips_selected_fields,"route_id").
        select("trip_id").distinct().join(dict_tripId_shapeId, "trip_id").select("shape_id").distinct()

      val dict_origin_destiny = stopTimes_WithShapeId.join(shapesId_type2,"shape_id").
        withColumn("stop_sequence", 'stop_sequence.cast("int")).
        withColumn("stop_id_dest" ,lag("stop_id", -1,null).over(wSpec2)).
        withColumn("stop_sequence_dest" ,lag("stop_sequence", -1,null).over(wSpec2))

      val orig_dest_dist = dict_origin_destiny.select($"shape_id",$"stop_id",$"stop_sequence").
        join(dict_origin_destiny.select($"shape_id",
          $"stop_id".alias("stop_id_dest"),
          $"stop_sequence".alias("stop_sequence_dest")),"shape_id").filter($"stop_sequence"<$"stop_sequence_dest")

      val orig_dest_maxdist = orig_dest_dist.withColumn("Dist",$"stop_sequence_dest"-$"stop_sequence").
        groupBy($"stop_id",$"stop_id_dest").agg(max($"dist").alias("maxdist"))

      // Recover shape id
      val maxshapesidstrip = orig_dest_dist.join(orig_dest_maxdist,Seq("stop_id","stop_id_dest")).
        select("stop_id","stop_id_dest","shape_id","maxdist").dropDuplicates("stop_id","stop_id_dest")

      val ren = maxshapesidstrip.select($"stop_id".
        alias("stop_id_a"),$"stop_id_dest".
        alias("stop_id_dest_a"),$"shape_id".
        alias("shape_id_prop"))

      val shapeprop = dict_origin_destiny.join(ren,
        dict_origin_destiny("stop_id")===ren("stop_id_a")&&
          dict_origin_destiny("stop_id_dest")===ren("stop_id_dest_a"),"leftouter").
        drop("stop_id_a","stop_id_dest_a","stop_sequence","stop_sequence_dest")

      //val dict_origin_destiny_cached = dict_origin_destiny.cache()
      //dict_origin_destiny_cached.count()
      //println("dict_origin_destiny_cached")
      //val shapeprop_cached = shapeprop.drop("stop_sequence","stop_sequence_dest").cache()
      //shapeprop_cached.count()

      val dict_origin_destiny_cloned = dict_origin_destiny.toDF("shape_id","stop_id","stop_sequence","stop_id_dest","stop_sequence_dest")
      println("dict_origin_destiny_cloned")
      val shapeprop_cloned = shapeprop.toDF("shape_id_a","stop_id_a","stop_id_dest_a","shape_id_prop")
      println("shapeprop_cloned")
      //shapeprop_cloned.checkpoint(true)
      //Stops between origin and final to proposed shape
      val dictprop = dict_origin_destiny_cloned.join(shapeprop_cloned,
        dict_origin_destiny_cloned("stop_id")<=>shapeprop_cloned("stop_id_a")&&
          dict_origin_destiny_cloned("stop_id_dest")<=>shapeprop_cloned("stop_id_dest_a")&&
          dict_origin_destiny_cloned("shape_id")<=>shapeprop_cloned("shape_id_a"),"leftouter").
        drop("stop_id_a","stop_id_dest_a","shape_id_a")
      println("dictprop")
      // BP
      //dictprop.checkpoint()

      val wSpec3 = Window.partitionBy("shape_id","stop_id").orderBy("distance").rowsBetween(Long.MinValue, 0)

      val master_shapes_dist = stopTimes_WithShapeId.withColumn("stop_sequence",'stop_sequence.cast("int")).
        select("shape_id","stop_id","stop_sequence").distinct().
        join(stopTimes_WithShapeId.withColumn("stop_sequence",'stop_sequence.cast("int")).
          select($"shape_id",$"stop_id".alias("stop_id_dest"),$"stop_sequence".
            alias("stop_sequence_dest")),Seq("shape_id")).
        filter($"stop_sequence" < $"stop_sequence_dest").
        withColumn("distance", $"stop_sequence_dest"-$"stop_sequence").
        dropDuplicates().orderBy("shape_id")

      // master_shapes_dist.checkpoint()
      println("master shapes")
      @transient val finalt = master_shapes_dist.orderBy($"stop_sequence",$"stop_sequence_dest",$"distance").
        withColumn("ZZZ", collect_list("stop_id_dest").over(wSpec3)).
        withColumnRenamed("shape_id","shape_id_a").
        withColumnRenamed("stop_id_dest","stop_id_dest_a").
        withColumnRenamed("stop_id","stop_id_a").
        withColumnRenamed("stop_sequence","stop_sequence_a").
        withColumnRenamed("stop_sequence_dest","stop_sequence_dest_a")
      println("finalt")
      val tabfinal = dictprop.join(finalt,dictprop("stop_id")===finalt("stop_id_a")&&
        dictprop("stop_id_dest")===finalt("stop_id_dest_a")&&
        dictprop("shape_id_prop")===finalt("shape_id_a"),"leftouter").
        filter($"distance" < 25).
        drop("stop_id_a","stop_id_dest_a","stop_sequence_a","stop_sequence_dest_a","shape_id_a")
      println("tabfinal")
      val wSpec5 = Window.partitionBy("shape_id").orderBy("stop_sequence").rowsBetween(Long.MinValue, 0)
      val wSpec6 = Window.partitionBy("shape_id").orderBy("stop_sequence_a").rowsBetween(Long.MinValue, 0)

      val tabtofile=tabfinal.
        withColumn("stop_sequence",'stop_sequence.cast("int")).
        orderBy("shape_id","stop_sequence").coalesce(1).withColumn("incremental",lit(1)).withColumn("ZZZ",when($"ZZZ".isNull, array($"stop_id")).otherwise($"ZZZ"))
        .withColumn("test1", explode($"ZZZ")).
        withColumn("Test", sum($"incremental").over(wSpec5))
      println("tabtofile")
      val dict_final = tabtofile.select("shape_id","test1","Test").withColumnRenamed("test1","stop_id").withColumnRenamed("Test","stop_sequence").
        union(tabtofile.filter($"stop_sequence"===0).select("shape_id","stop_id","stop_sequence")).
        groupBy("shape_id","stop_id").agg(min("stop_sequence").alias("stop_sequence_a")).coalesce(1).withColumn("incremental",lit(1)).
        withColumn("stop_sequence", sum($"incremental").over(wSpec6))
        .drop("stop_sequence_a","incremental").orderBy("shape_id","stop_sequence").
        withColumn("ordering", $"stop_sequence" - 1).drop("stop_sequence").withColumnRenamed("ordering", "stop_sequence")

      println("dict_final")

      //Generate shapesDF

      val shapesEnv = dict_tripId_shapeId.
        join(stop_times_selected_fields,Seq("trip_id")).
        join(stops_selected_fields,Seq("stop_id")).
        withColumnRenamed("stop_lat","shape_pt_lat").
        withColumnRenamed("stop_lon","shape_pt_lon").
        withColumnRenamed("stop_sequence","shape_pt_sequence").
        orderBy("shape_id","trip_id","shape_pt_sequence").
        withColumn("shape_pt_sequence",      'shape_pt_sequence.cast("Int")).
        orderBy("shape_id","trip_id","shape_pt_sequence").drop("trip_id").
        distinct().
        orderBy("shape_id","shape_pt_sequence").
        select("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence")
      println("shapes env")


      val preJoin = dict_final.join(stops_selected_fields,"stop_id").
        withColumnRenamed("stop_sequence", "shape_pt_sequence").
        withColumnRenamed("stop_lat", "shape_pt_lat").
        withColumnRenamed("stop_lon", "shape_pt_lon").
        orderBy("shape_id","shape_pt_sequence").
        select("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence")
      println("prejoin")


      import org.apache.spark.sql.functions
      val wDist = Window.partitionBy("shape_id").orderBy("shape_pt_sequence")
      val shapesDF = shapesEnv.alias("shapesEnv").join(preJoin.select("shape_id").alias("preJoin"), Seq("shape_id"), "leftouter").where($"preJoin.shape_id".isNull).drop($"preJoin.shape_id").union(preJoin).orderBy("shape_id","shape_pt_sequence").
        withColumn("shape_pt_sequence",      'shape_pt_sequence.cast("Int")).
        withColumn("shape_pt_lat",      'shape_pt_lat.cast("Double")).
        withColumn("shape_pt_lon",      'shape_pt_lon.cast("Double")).
        withColumn("stop_lat_prev", lag("shape_pt_lat", 1,0).over(wDist)).withColumn("stop_lon_prev", lag("shape_pt_lon", 1,0).over(wDist)).
        withColumn("a", functions.sin(functions.radians($"stop_lat_prev" - $"shape_pt_lat") / 2) * functions.sin(functions.radians($"stop_lat_prev" - $"shape_pt_lat") / 2) + functions.cos(functions.radians($"shape_pt_lat")) * functions.cos(functions.radians($"stop_lat_prev")) * functions.sin(functions.radians($"stop_lon_prev" - $"shape_pt_lon") / 2) * functions.sin(functions.radians($"stop_lon_prev" - $"shape_pt_lon") / 2)).withColumn("c", functions.atan2(functions.sqrt($"a"), functions.sqrt(-$"a" + 1)) * 2)
        .withColumn("shape_dist_traveled",  when($"shape_pt_sequence" === 0, 0).otherwise($"c" * 6371000)).
        select("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled")
      println("shapesDF")
      // Generte stop_times DF

      val shapes_new = shapesDF.withColumnRenamed("shape_pt_lat", "stop_lat").
        withColumnRenamed("shape_pt_lon", "stop_lon").
        withColumnRenamed("shape_pt_sequence", "stop_sequence")

      // Generate tripsDF & stop_times
      var tripsDF = trips.select("route_id")
      var stop_timesDF = stop_times.select("trip_id")

      location match {
        case "paris" =>

          tripsDF = trips.select("route_id","service_id","trip_id","trip_headsign","trip_short_name",
            "direction_id","block_id","wheelchair_accessible","bikes_allowed","trip_desc").
            join(dict_tripId_shapeId,Seq("trip_id")).select("route_id","service_id","trip_id","trip_headsign","trip_short_name",
            "direction_id","block_id","wheelchair_accessible","bikes_allowed","shape_id","trip_desc").orderBy("shape_id").
            select("trip_id","route_id","service_id","trip_headsign","trip_short_name","direction_id","block_id","shape_id","wheelchair_accessible","trip_desc")

          stop_timesDF = shapes_new.join(stops.filter($"stop_id".contains("StopPoint")), Seq("stop_lat", "stop_lon")).
            select("shape_id","stop_id","stop_sequence","stop_lat","stop_lon").
            join(dict_tripId_shapeId,"shape_id").orderBy("trip_id","stop_sequence").join(stop_times.
            select("trip_id","arrival_time","departure_time","stop_id", "stop_time_desc","pickup_type","drop_off_type"), Seq("trip_id","stop_id")).
            orderBy("trip_id","stop_sequence").select("trip_id","arrival_time","departure_time","stop_id","stop_sequence","stop_time_desc","pickup_type","drop_off_type")

        case "lyon" =>

          tripsDF = trips.select("route_id","service_id","trip_id","trip_headsign","direction_id","block_id").
            join(dict_tripId_shapeId,Seq("trip_id")).select("route_id","service_id","trip_id","trip_headsign","direction_id","block_id","shape_id").orderBy("shape_id").
            select("trip_id","route_id","service_id","trip_headsign","direction_id","block_id","shape_id")
          println("tripsDF")

          stop_timesDF = shapes_new.join(stops.filter($"stop_id".contains("StopPoint")), Seq("stop_lat", "stop_lon")).
            select("shape_id","stop_id","stop_sequence","stop_lat","stop_lon").
            join(dict_tripId_shapeId,"shape_id").orderBy("trip_id","stop_sequence").join(stop_times.
            select("trip_id","arrival_time","departure_time","stop_id", "stop_headsign","pickup_type","drop_off_type"), Seq("trip_id","stop_id")).
            orderBy("trip_id","stop_sequence").
            select("trip_id","arrival_time","departure_time","stop_id","stop_sequence","stop_headsign","pickup_type","drop_off_type")
          println("stop_timesDF")
      }

      dataframesOutput += (routes,stops,tripsDF,stop_timesDF,agency,calendar_dates,calendar,transfers,stop_extensions,shapesDF, dict_tripId_shapeId)

      dataframesOutput.toList

    }

    def UploadAzure(dataframes: List[DataFrame], rawPath: String, spark: SparkSession) = {

      import org.apache.hadoop.fs.{FileSystem, Path}
      val raw_path = rawPath.concat("GTFSCLEAN/" + "stif_gtfs/")
      val routes = dataframes(0)
      val stops = dataframes(1)
      val trips = dataframes(2)
      val stop_times = dataframes(3)
      val agency = dataframes(4)
      val calendar_dates = dataframes(5)
      val calendar = dataframes(6)
      val transfers = dataframes(7)
      val stop_extensions = dataframes(8)
      val shapes = dataframes(9)
      import java.io.IOException

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
      stop_times.repartition(20).write.mode("overwrite").parquet(raw_path + "stop_times.parquet")
      println("stop_times uploaded")
      agency.coalesce(1).write.mode("overwrite").parquet(raw_path + "agency.parquet")
      println("agency uploaded")
      calendar_dates.coalesce(1).write.mode("overwrite").parquet(raw_path + "calendar_dates.parquet")
      println("calendar_dates uploaded")
      calendar.coalesce(1).write.mode("overwrite").parquet(raw_path + "calendar.parquet")
      println("calendar uploaded")
      transfers.coalesce(1).write.mode("overwrite").parquet(raw_path + "transfers.parquet")
      println("transfers uploaded")
      stop_extensions.coalesce(1).write.mode("overwrite").parquet(raw_path + "stop_extensions.parquet")
      println("stop_extensions uploaded")
      // Write in raw path
      shapes.repartition(34).orderBy("shape_id","shape_pt_sequence").write.mode("overwrite").parquet(raw_path + "shapes.parquet")
      println("shapes uploaded")

      /*
      val listOfFiles = fs.listStatus(new Path(raw_path))
      val filesPaths_list = ArrayBuffer[String]()
      listOfFiles.foreach(x => filesPaths_list += x.getPath.toString)
      merge(filesPaths_list, "adl:///data/normal/")
      */
    }

  }

