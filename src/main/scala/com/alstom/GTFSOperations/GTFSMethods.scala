package com.alstom.GTFSOperations

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object GTFSMethods {


    def getFileNameAndExtFromPath(path: String): (String, String) = {

      val ext = path.split("/").last.split('.').last
      println(ext)
      val fileName = path.split("/").last.split('.').head
      return (fileName, ext)

    }

    def AddGeoJsonToReplaceShapes (dataframes: List[DataFrame], geoJsonName: String, geoJsonPath: String, originalShapes: DataFrame, spark: SparkSession) : List[DataFrame] = {

      geoJsonName match {

        case "01SystemXGeoJson" => AddSystemX(dataframes,geoJsonPath,spark)
        case "01TclLigneBusGeoJson" => AddTcl(dataframes,geoJsonPath,originalShapes,spark)
        case "02TclLigneMetroGeojson" => AddTcl(dataframes,geoJsonPath,originalShapes,spark)
        case "03TclLigneTramGeoJson" => AddTcl(dataframes,geoJsonPath,originalShapes,spark)
        case "04GoogleRailWayLyonGeoJson" => AddGoogle(dataframes,geoJsonPath,originalShapes,spark)
        case _ => println("No GeoJson found");sys.exit(1)

      }

    }

    def AddSystemX(dataframes: List[DataFrame], geoJsonPath: String, spark: SparkSession) : List[DataFrame] = {

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
      val shapes = dataframes(9)
      val dict_tripId_shapeId = dataframes(10)

      // Match geojson file with dict_routes_filtered
      val geojsonDF = spark
        .read
        .json(geoJsonPath).drop("_corrupt_record").na.drop()

      val dict_geoJsonRoutes = geojsonDF.select($"properties.INDICE_VOY", $"properties.INDICE_INT",
        UDFS.rgbToInt($"properties.COULEURRVB") as "COULEURRVB", $"properties.SENS", $"properties.NOM_MAJ", $"geometry.coordinates").
        withColumn("INDICE_INT", $"INDICE_INT".cast("int").cast("string")).
        withColumn("first_case", concat($"INDICE_INT", $"INDICE_VOY")).
        withColumn("third_case", when($"first_case" === "352Roissybus", "100100352:ROISSYB").
          otherwise(when($"first_case" === "541Le Buséolien", "100100541:EOLIEN").
            otherwise(when($"first_case" === "929Spécial RER A", "").otherwise("No match"))))

      // Match GTFS Routes with GeoJson Routes
      val dict_routes_filtered = routes.filter($"agency_id" === "442" || $"agency_id" === "56" || $"agency_id" === "440").
        withColumn("first_case", UDFS.completeCode($"route_short_name", $"route_id")).
        withColumn("second_case", UDFS.IntCode($"route_id"))

      val routesJoinGeo_first = dict_routes_filtered.join(dict_geoJsonRoutes, "first_case").distinct().
        select("route_id","route_short_name","route_long_name","second_case","INDICE_VOY","INDICE_INT","COULEURRVB", "SENS", "NOM_MAJ", "coordinates", "third_case")
      val routesJoinGeo_second = dict_routes_filtered.join(dict_geoJsonRoutes, dict_routes_filtered("route_long_name") === dict_geoJsonRoutes("INDICE_INT") &&
        dict_routes_filtered("second_case") === dict_geoJsonRoutes("INDICE_INT")
      ).distinct().select("route_id","route_short_name","route_long_name","second_case","INDICE_VOY","INDICE_INT","COULEURRVB","SENS", "NOM_MAJ", "coordinates", "third_case")

      val routesJoinGeo_third =  dict_routes_filtered.join(dict_geoJsonRoutes,
        dict_geoJsonRoutes("third_case") === dict_routes_filtered("route_id")).distinct().
        select("route_id","route_short_name","route_long_name","second_case","INDICE_VOY","INDICE_INT","COULEURRVB","SENS", "NOM_MAJ", "coordinates", "third_case")

      val routesDictionaryFromGTFSToJoinGeoJson = routesJoinGeo_first.union(routesJoinGeo_second).union(routesJoinGeo_third).distinct().
        select("route_id","route_short_name","route_long_name","second_case","INDICE_VOY","INDICE_INT","COULEURRVB","SENS", "NOM_MAJ", "coordinates").
        orderBy("INDICE_INT")

      // Update routes df color with geojson provided
      val routes_newDF = routes.join(routesDictionaryFromGTFSToJoinGeoJson.dropDuplicates("route_id","route_short_name","route_long_name").
        select("route_id","route_short_name","route_long_name","COULEURRVB"), Seq("route_id","route_short_name","route_long_name"), "leftouter").
        withColumn("route_color", when($"COULEURRVB".isNotNull,$"COULEURRVB").otherwise($"route_color")).drop("COULEURRVB")

      val ShapesByRouteAndDirection = routesDictionaryFromGTFSToJoinGeoJson.
        withColumn("IND_INTVOY", concat($"INDICE_INT", $"INDICE_VOY")).
        withColumn("direction_id", when($"SENS" === "ALLER", "0").otherwise("1"))
        .drop("coor_flat","SENS","route_short_name","route_long_name","second_case","INDICE_VOY","INDICE_INT","COULEURRVB")
        .select("route_id","direction_id","IND_INTVOY","NOM_MAJ","coordinates").
        orderBy("route_id","direction_id")

      val shapes_new = shapes.withColumnRenamed("shape_pt_lat", "stop_lat").
        withColumnRenamed("shape_pt_lon", "stop_lon").
        withColumnRenamed("shape_pt_sequence", "stop_sequence")

      // Master dict

      val master_dict =  shapes_new.join(stops.filter($"stop_id".contains("StopPoint")), Seq("stop_lat", "stop_lon")).
        select("shape_id","stop_id","stop_sequence","stop_lat","stop_lon").
        join(dict_tripId_shapeId,"shape_id").orderBy("trip_id","stop_sequence").
        join(stop_times.select("trip_id","arrival_time","departure_time","stop_id", "stop_time_desc","pickup_type","drop_off_type"), Seq("trip_id","stop_id")).
        orderBy("trip_id","stop_sequence").select("trip_id","stop_id","shape_id","stop_sequence","stop_lat","stop_lon")

      val prev_dictionary = ShapesByRouteAndDirection.groupBy("route_id","direction_id").agg(collect_list("coordinates").alias("coor_list")).
        join(trips.select("trip_id","route_id","direction_id"), Seq("route_id","direction_id")).join(dict_tripId_shapeId, "trip_id").
        select("route_id","direction_id","coor_list","shape_id").distinct().
        withColumn("size",size($"coor_list"))

      val master_dict_cloned = master_dict.toDF("trip_id","stop_id","shape_id","stop_sequence","stop_lat","stop_lon")
      var listShapesBySeq = prev_dictionary.join(master_dict_cloned.select("shape_id","stop_sequence","stop_lat","stop_lon").distinct(),"shape_id").
        orderBy("shape_id","stop_sequence").withColumn("forceReverse", lit("false"))

      // Add those GeoJson routes which have the oppossite direction in GTFS

      val possibleOpposites = listShapesBySeq.filter($"direction_id" === 0).
        withColumn("direction_id", lit(1)).select("route_id","direction_id").
        join(listShapesBySeq.select("route_id","direction_id"), Seq("route_id","direction_id"),"leftanti").distinct().
        join(trips.select("trip_id","route_id","direction_id"), Seq("route_id","direction_id")).join(dict_tripId_shapeId, "trip_id").
        select("route_id","direction_id","shape_id").distinct().join(master_dict_cloned.
        select("shape_id","stop_sequence","stop_lat","stop_lon").distinct(),"shape_id").
        orderBy("shape_id","stop_sequence").withColumn("forceReverse", lit("true"))

      listShapesBySeq = listShapesBySeq.select("route_id","coor_list").distinct().join(possibleOpposites, "route_id").orderBy("shape_id","stop_sequence").
        withColumn("size",size($"coor_list")).
        select("shape_id","route_id","direction_id","coor_list","size","stop_sequence","stop_lat","stop_lon","forceReverse").union(listShapesBySeq)

      val wDistInit = Window.partitionBy("shape_id").orderBy("stop_sequence")
      //Define new window partition to operate on row frame
      val wDistLast = Window.partitionBy("shape_id").orderBy("stop_sequence").rowsBetween(Window.currentRow, Window.unboundedFollowing)
      val shapesRev = listShapesBySeq.
        withColumn("stop_lat_init", first("stop_lat").over(wDistInit)).
        withColumn("stop_lon_init", first("stop_lon").over(wDistInit)).
        withColumn("stop_lat_last", last("stop_lat").over(wDistLast)).
        withColumn("stop_lon_last", last("stop_lon").over(wDistLast)).
        withColumn("coor_flat", explode($"coor_list"))

      // Get Valid GeoJson Shape for each GTFS Shape
      val wShapes = Window.partitionBy("shape_id").orderBy(asc("distance"))
      val dict_shapesInGJ = shapesRev.withColumn("distance", UDFS.validShape($"coor_flat", $"stop_lat_init", $"stop_lon_init", $"stop_lat_last", $"stop_lon_last", $"forceReverse")).
        withColumnRenamed("coor_flat","selected_shapes").
        select("shape_id","route_id","direction_id","stop_sequence","stop_lat","stop_lon","stop_lat_init","stop_lat_last","stop_lon_init","stop_lon_last","selected_shapes","distance","forceReverse").
        withColumn("rn", min($"distance").over(wShapes)).
        filter($"distance" === $"rn").drop("distance","rn")

      // Get shapes for stops
      val wSpec2 = Window.partitionBy("shape_id").orderBy("stop_sequence")
      val pointsProp = dict_shapesInGJ.withColumn("stop_lat_dest" ,lag("stop_lat", -1,null).over(wSpec2)).
        withColumn("stop_lon_dest" ,lag("stop_lon", -1,null).over(wSpec2)).
        withColumn("pointsBetweenStops", UDFS.pointsBetweenStops($"selected_shapes",$"stop_lat_init",$"stop_lon_init",$"stop_lat",$"stop_lon",$"stop_lat_dest",$"stop_lon_dest",$"forceReverse")).
        withColumn("size", size($"pointsBetweenStops")).
        filter($"size" > 1).drop("size","selected_shapes").dropDuplicates()

      // Reducted shapes
      val reductedShapes = pointsProp.withColumn("reduced", UDFS.reduceShapes($"pointsBetweenStops"))
        .select("shape_id","stop_sequence","pointsBetweenStops","reduced").
        withColumn("points", explode($"reduced")).select("shape_id","stop_sequence","points").withColumn("shape_pt_lat", $"points._1").
        withColumn("shape_pt_lon", $"points._2")

      // Add sequence and dist travelled
      val wDist = Window.partitionBy("shape_id").orderBy("shape_pt_sequence")
      val shapeDFlastDist = reductedShapes.withColumn("RowN", row_number().over(Window.partitionBy($"shape_id").orderBy("stop_sequence"))).
        withColumn("shape_pt_sequence", $"RowN" - 1).
        withColumn("shape_pt_sequence",      'shape_pt_sequence.cast("Int")).
        withColumn("stop_lat_prev", lag("shape_pt_lat", 1,0).over(wDist)).withColumn("stop_lon_prev", lag("shape_pt_lon", 1,0).over(wDist)).
        withColumn("a", functions.sin(functions.radians($"stop_lat_prev" - $"shape_pt_lat") / 2) * functions.sin(functions.radians($"stop_lat_prev" - $"shape_pt_lat") / 2) + functions.cos(functions.radians($"shape_pt_lat")) * functions.cos(functions.radians($"stop_lat_prev")) * functions.sin(functions.radians($"stop_lon_prev" - $"shape_pt_lon") / 2) * functions.sin(functions.radians($"stop_lon_prev" - $"shape_pt_lon") / 2)).withColumn("c", functions.atan2(functions.sqrt($"a"), functions.sqrt(-$"a" + 1)) * 2)
        .withColumn("shape_dist_traveled",  when($"shape_pt_sequence" === 0, 0).otherwise($"c" * 6371000)).
        select("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled")

      // Generate final shape df
      val shapes_cloned = shapes.toDF("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled")
      val shapesDFlast = shapes_cloned.alias("shapeDF").join(shapeDFlastDist.select("shape_id").alias("shapeDFlastDist"), Seq("shape_id"), "leftouter").
        where($"shapeDFlastDist.shape_id".isNull).drop($"shapeDFlastDist.shape_id").union(shapeDFlastDist).orderBy("shape_id","shape_pt_sequence")
      shapesDFlast.checkpoint()
      dataframesOutput += (routes_newDF,stops,trips,stop_times,agency,calendar_dates,calendar,transfers,stop_extensions,shapesDFlast)
      dataframesOutput.toList

    }

    def AddTcl(dataframes: List[DataFrame], geoJsonPath: String, originalShape: DataFrame, spark: SparkSession) : List[DataFrame] = {

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
      val shapes = originalShape
      val dict_tripId_shapeId = dataframes(9)

      // Match geojson file with dict_routes_filtered
      val geojsonDF = spark
        .read
        .json(geoJsonPath).drop("_corrupt_record").na.drop()

      //1.GetroutesDictionaryFromGTFSToJoinGeoJson

      val dict_geoJsonRoutes = geojsonDF.select($"properties.code_titan", $"properties.ligne", $"properties.sens", $"properties.libelle",$"geometry.coordinates",$"properties.infos",
        UDFS.rgbToIntTcl($"properties.couleur") as "couleur")


      val dict_routes_filtered = routes.filter($"agency_id" === "tcl" || $"agency_id" === "sncv" || $"agency_id" === "rhonexpress")

      val routesDictionaryFromGTFSToJoinGeoJson = dict_routes_filtered.
        join(dict_geoJsonRoutes, dict_geoJsonRoutes("ligne") <=> dict_routes_filtered("route_short_name")).
        withColumn("route_color", $"couleur")

      val ShapesByRouteAndDirection = routesDictionaryFromGTFSToJoinGeoJson.
        withColumn("direction_id", when($"sens" === "Aller", "0").otherwise("1"))
        .drop("sens","route_short_name","route_long_name","route_url","route_desc","route_text_color").
        orderBy("route_id","direction_id").
        withColumn("infos", when($"infos".isNull,"nulo").otherwise($"infos"))


      // Update routes df color with geojson provided
      //val routes_newDF= routesDictionaryFromGTFSToJoinGeoJson.select("route_id","agency_id","route_short_name","route_long_name",
      //"route_desc","route_type","route_url","route_color")

      // Prev dictionary with coordinates grouped by dir and route id
      var prev_dictionary = ShapesByRouteAndDirection.groupBy("route_id","direction_id","infos").
        agg(collect_list("coordinates").as("coor_list")).withColumn("size",size($"coor_list")).
        withColumn("coor_flat", explode($"coor_list"))
      var prev_dictionary_cloned = prev_dictionary.withColumnRenamed("coor_flat","coor_retour").
        withColumnRenamed("direction_id","direction_retour")

      //main from geojson with correct coordenates reverzed in special bus case
      var dict_directions_reverse = prev_dictionary.join(prev_dictionary_cloned, Seq("route_id","infos")).
        filter($"direction_id" === 0 && $"direction_retour" === "1").drop("size","coor_list").
        withColumn("distance", UDFS.calcDistances($"coor_flat",$"coor_retour")).
        withColumn("coor_retour", when($"distance" < 200, UDFS.reverseRetour($"coor_retour")).
          otherwise($"coor_retour")).drop("distance")

      val coorsDFUnion = dict_directions_reverse.drop("direction_id","coor_flat").
        withColumnRenamed("direction_retour", "direction_id").
        withColumnRenamed("coor_retour","coor_flat").
        select("route_id","direction_id","infos","coor_flat")

      dict_directions_reverse = dict_directions_reverse.
        select("route_id","direction_id","infos","coor_flat").
        union(coorsDFUnion).orderBy("route_id","direction_id").
        withColumnRenamed("coor_flat","coor_list").distinct()

      // Recover all cases
      val largeDF = ShapesByRouteAndDirection.
        withColumnRenamed("coordinates", "coor_list").
        select("route_id","direction_id","infos","coor_list")
      var allCasesDict = largeDF.alias("large").
        select("route_id","infos","coor_list").
        join(dict_directions_reverse.alias("dict_directions_reverse"), Seq("route_id"),"leftouter").
        where($"dict_directions_reverse.route_id".isNull).drop($"dict_directions_reverse.route_id").
        join(largeDF.alias("good").select("route_id","direction_id","infos","coor_list"), Seq("route_id","infos","coor_list")).
        select($"large.route_id",$"good.direction_id",$"large.infos",$"large.coor_list").union(dict_directions_reverse).distinct()

      val shapes_new = shapes.withColumnRenamed("shape_pt_lat", "stop_lat").
        withColumnRenamed("shape_pt_lon", "stop_lon").
        withColumnRenamed("shape_pt_sequence", "stop_sequence")

      shapes_new.checkpoint()

      // Master Dict
      val master_dict =  shapes_new.join(stops.filter($"stop_id".contains("StopPoint")), Seq("stop_lat", "stop_lon")).
        select("shape_id","stop_id","stop_sequence","stop_lat","stop_lon").
        join(dict_tripId_shapeId,"shape_id").orderBy("trip_id","stop_sequence").
        join(stop_times.select("trip_id","arrival_time","departure_time","stop_id","pickup_type","drop_off_type"), Seq("trip_id","stop_id")).
        orderBy("trip_id","stop_sequence").select("trip_id","stop_id","shape_id","stop_sequence","stop_lat","stop_lon").distinct()
      val master_dict_cloned = master_dict.toDF("trip_id","stop_id","shape_id","stop_sequence","stop_lat","stop_lon")

      // Routes from gtfs in geojson with their shape id
      val dict_routesGTFS = allCasesDict.groupBy("route_id","direction_id","infos").
        agg(collect_list("coor_list").alias("coor_list")).
        join(trips.select("trip_id","route_id"), Seq("route_id")).join(dict_tripId_shapeId, "trip_id").
        select("route_id","direction_id","infos","coor_list","shape_id").distinct()

      // Routes from gtfs in geojson with their shape id and stops
      var listShapesBySeq = dict_routesGTFS.join(master_dict_cloned.select("shape_id","stop_sequence","stop_lat","stop_lon").distinct(),"shape_id").
        orderBy("shape_id","stop_sequence").withColumn("forceReverse", lit("false")).
        orderBy("shape_id","route_id","direction_id","stop_sequence")

      // Add those GeoJson routes which have the oppossite direction in GTFS

      /*val possibleOpposites = listShapesBySeq.filter($"direction_id" === 0).
        withColumn("direction_id", lit(1)).select("route_id","direction_id").
        join(listShapesBySeq.select("route_id","direction_id"), Seq("route_id","direction_id"),"leftanti").distinct().
        join(trips.select("trip_id","route_id","direction_id"), Seq("route_id","direction_id")).
        join(dict_tripId_shapeId, "trip_id").select("route_id","direction_id","shape_id").distinct().join(master_dict_cloned.select("shape_id","stop_sequence","stop_lat","stop_lon").distinct(),"shape_id").orderBy("shape_id","stop_sequence").withColumn("forceReverse", lit("true"))
      val possibleOppositesCount = possibleOpposites.count()

      if (possibleOppositesCount > 0) {
        listShapesBySeq = listShapesBySeq.select("route_id","coor_list").distinct().
          join(possibleOpposites, "route_id").orderBy("shape_id","stop_sequence").
          select("shape_id","route_id","direction_id","coor_list","stop_sequence","stop_lat","stop_lon","forceReverse").union(listShapesBySeq)
      }
      */

      val wDistInit = Window.partitionBy("shape_id").orderBy("stop_sequence")
      //Define new window partition to operate on row frame
      val wDistLast = Window.partitionBy("shape_id").orderBy("stop_sequence").rowsBetween(Window.currentRow, Window.unboundedFollowing)

      val shapesRev = listShapesBySeq.
        withColumn("stop_lat_init", first("stop_lat").over(wDistInit)).
        withColumn("stop_lon_init", first("stop_lon").over(wDistInit)).
        withColumn("stop_lat_last", last("stop_lat").over(wDistLast)).
        withColumn("stop_lon_last", last("stop_lon").over(wDistLast)).
        withColumn("coor_flat", explode($"coor_list")).orderBy("shape_id","route_id","direction_id","stop_sequence").distinct()

      shapesRev.checkpoint()

      // Get Valid GeoJson Shape for each GTFS Shape
      val wShapes = Window.partitionBy("shape_id").orderBy(asc("distance"))
      var dict_shapesInGJ = shapesRev.withColumn("distance", UDFS.validShapeTcl($"coor_flat", $"infos", $"direction_id", $"stop_lat_init", $"stop_lon_init", $"stop_lat_last", $"stop_lon_last", $"forceReverse")).
        withColumnRenamed("coor_flat","selected_shapes").
        select("shape_id","route_id","direction_id","infos","stop_sequence","stop_lat","stop_lon","stop_lat_init","stop_lat_last","stop_lon_init","stop_lon_last","selected_shapes","distance","forceReverse").
        withColumn("rn", min($"distance").over(wShapes)).
        filter($"distance" === $"rn").drop("distance","rn")

      dict_shapesInGJ = dict_shapesInGJ.
        withColumn("test1", lit("normal")).
        withColumn("test2", lit("reverse")).
        withColumn("distOneDir", when($"infos" === "Circulaire", UDFS.GetProgressiveDistanceUsingShapes($"selected_shapes", $"stop_lat", $"stop_lon",$"test1")).otherwise(null)).
        withColumn("distOppoDir", when($"infos" === "Circulaire", UDFS.GetProgressiveDistanceUsingShapes($"selected_shapes", $"stop_lat", $"stop_lon",$"test2")).otherwise(null)).
        withColumn("SumDistOneDir", sum("distOneDir").over(Window.partitionBy("shape_id","route_id"))).
        withColumn("SumDistOppoDir", sum("distOppoDir").over(Window.partitionBy("shape_id","route_id"))).
        withColumn("selected_shapes", when($"SumDistOppoDir" <= $"SumDistOneDir", UDFS.reverseRetour($"selected_shapes")).otherwise($"selected_shapes")).drop("test1","test2","distOneDir","distOppoDir","SumDistOneDir","SumDistOppoDir")

      val wSpec2 = Window.partitionBy("shape_id").orderBy("stop_sequence")
      // Get shapes for stops
      val pointsProp = dict_shapesInGJ.withColumn("stop_lat_dest" ,lag("stop_lat", -1,null).over(wSpec2)).
        withColumn("stop_lon_dest" ,lag("stop_lon", -1,null).over(wSpec2)).
        withColumn("pointsBetweenStops", UDFS.pointsBetweenStops($"selected_shapes",$"stop_lat_init",$"stop_lon_init",$"stop_lat",$"stop_lon",$"stop_lat_dest",$"stop_lon_dest",$"forceReverse")).
        withColumn("size", size($"pointsBetweenStops")).
        filter($"size" > 1).drop("size","selected_shapes").dropDuplicates()

      // Reducted shapes
      val reductedShapes = pointsProp.withColumn("reduced", UDFS.reduceShapes($"pointsBetweenStops"))
        .select("shape_id","stop_sequence","pointsBetweenStops","reduced").
        withColumn("points", explode($"reduced")).select("shape_id","stop_sequence","points").withColumn("shape_pt_lat", $"points._1").
        withColumn("shape_pt_lon", $"points._2")

      // Add sequence and dist travelled
      val wDist = Window.partitionBy("shape_id").orderBy("shape_pt_sequence")
      val shapeDFlastDist = reductedShapes.withColumn("RowN", row_number().over(Window.partitionBy($"shape_id").orderBy("stop_sequence"))).
        withColumn("shape_pt_sequence", $"RowN" - 1).
        withColumn("shape_pt_sequence",      'shape_pt_sequence.cast("Int")).
        withColumn("stop_lat_prev", lag("shape_pt_lat", 1,0).over(wDist)).withColumn("stop_lon_prev", lag("shape_pt_lon", 1,0).over(wDist)).
        withColumn("a", functions.sin(functions.radians($"stop_lat_prev" - $"shape_pt_lat") / 2) * functions.sin(functions.radians($"stop_lat_prev" - $"shape_pt_lat") / 2) + functions.cos(functions.radians($"shape_pt_lat")) * functions.cos(functions.radians($"stop_lat_prev")) * functions.sin(functions.radians($"stop_lon_prev" - $"shape_pt_lon") / 2) * functions.sin(functions.radians($"stop_lon_prev" - $"shape_pt_lon") / 2)).withColumn("c", functions.atan2(functions.sqrt($"a"), functions.sqrt(-$"a" + 1)) * 2)
        .withColumn("shape_dist_traveled",  when($"shape_pt_sequence" === 0, 0).otherwise($"c" * 6371000)).
        select("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled")

      // Generate shape final

      val shapes_cloned = shapes.toDF("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled")
      var shapesDFlast = shapes_cloned.alias("shapeDF").join(shapeDFlastDist.select("shape_id").alias("shapeDFlastDist"), Seq("shape_id"), "leftouter").where($"shapeDFlastDist.shape_id".isNull).drop($"shapeDFlastDist.shape_id")
      shapesDFlast.union(shapeDFlastDist).orderBy("shape_id","shape_pt_sequence")

      shapesDFlast.checkpoint()

      dataframesOutput += (routes,stops,trips,stop_times,agency,calendar_dates,calendar,transfers,shapesDFlast,dict_tripId_shapeId)
      dataframesOutput.toList

    }

    def AddGoogle(dataframes: List[DataFrame], geoJsonPath: String, originalShape: DataFrame, spark: SparkSession) : List[DataFrame] = {

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
      val shapes = originalShape
      val dict_tripId_shapeId = dataframes(9)

      // Match geojson file with dict_routes_filtered
      // Due to this geojson is generated by Raul and it has multiline option, we have to read it as follows:
      val geojsonDF = spark.read
        .json(spark.sparkContext.wholeTextFiles(geoJsonPath).map(_._2))

      val dict_geoJsonRoutes = geojsonDF.select($"features.properties.routeId", $"features.properties.code", $"features.properties.direction", $"features.properties.description", $"features.geometry.coordinates").withColumnRenamed("routeId","route_id").withColumnRenamed("description","infos").
        withColumn("item", lit(1)).withColumn("vars", explode(UDFS.zip4($"route_id", $"code",$"direction",$"infos",$"coordinates"))).select(
        $"vars._1".alias("route_id"), $"vars._2".alias("code"), $"vars._3".alias("direction"),$"vars._4".alias("infos"),$"vars._5".alias("coordinates"))

      val routesDictionaryFromGTFSToJoinGeoJson = routes.select("route_id","agency_id","route_short_name","route_long_name","route_desc","route_type","route_url").
        join(dict_geoJsonRoutes, Seq("route_id")).distinct().withColumnRenamed("direction","direction_id")

      val ShapesByRouteAndDirection = routesDictionaryFromGTFSToJoinGeoJson.
        drop("route_short_name","route_long_name","route_url","route_desc").
        orderBy("route_id","direction_id")

      // Prev dictionary with coordinates grouped by dir and route id
      var prev_dictionary = ShapesByRouteAndDirection.groupBy("route_id","direction_id","infos").
        agg(collect_list("coordinates").as("coor_list")).withColumn("size",size($"coor_list")).
        withColumn("coor_flat", explode($"coor_list"))
      var prev_dictionary_cloned = prev_dictionary.withColumnRenamed("coor_flat","coor_retour").
        withColumnRenamed("direction_id","direction_retour")

      //main from geojson with correct coordenates reverzed in special bus case
      var dict_directions_reverse = prev_dictionary.join(prev_dictionary_cloned, Seq("route_id","infos")).
        filter($"direction_id" === 0 && $"direction_retour" === "1").drop("size","coor_list").
        withColumn("distance", UDFS.calcDistances($"coor_flat",$"coor_retour")).
        withColumn("coor_retour", when($"distance" < 200, UDFS.reverseRetour($"coor_retour")).
          otherwise($"coor_retour")).drop("distance")

      val coorsDFUnion = dict_directions_reverse.drop("direction_id","coor_flat").
        withColumnRenamed("direction_retour", "direction_id").
        withColumnRenamed("coor_retour","coor_flat").
        select("route_id","direction_id","infos","coor_flat")

      dict_directions_reverse = dict_directions_reverse.
        select("route_id","direction_id","infos","coor_flat").
        union(coorsDFUnion).orderBy("route_id","direction_id").
        withColumnRenamed("coor_flat","coor_list").distinct()

      // Recover all cases
      val largeDF = ShapesByRouteAndDirection.
        withColumnRenamed("coordinates", "coor_list").
        select("route_id","direction_id","infos","coor_list")
      var allCasesDict = largeDF.alias("large").
        select("route_id","infos","coor_list").
        join(dict_directions_reverse.alias("dict_directions_reverse"), Seq("route_id"),"leftouter").
        where($"dict_directions_reverse.route_id".isNull).drop($"dict_directions_reverse.route_id").
        join(largeDF.alias("good").select("route_id","direction_id","infos","coor_list"), Seq("route_id","infos","coor_list")).
        select($"large.route_id",$"good.direction_id",$"large.infos",$"large.coor_list").union(dict_directions_reverse).distinct()

      val shapes_new = shapes.withColumnRenamed("shape_pt_lat", "stop_lat").
        withColumnRenamed("shape_pt_lon", "stop_lon").
        withColumnRenamed("shape_pt_sequence", "stop_sequence")

      // Master Dict
      val master_dict =  shapes_new.join(stops.filter($"stop_id".contains("StopPoint")), Seq("stop_lat", "stop_lon")).
        select("shape_id","stop_id","stop_sequence","stop_lat","stop_lon").
        join(dict_tripId_shapeId,"shape_id").orderBy("trip_id","stop_sequence").
        join(stop_times.select("trip_id","arrival_time","departure_time","stop_id","pickup_type","drop_off_type"), Seq("trip_id","stop_id")).
        orderBy("trip_id","stop_sequence").select("trip_id","stop_id","shape_id","stop_sequence","stop_lat","stop_lon").distinct()
      val master_dict_cloned = master_dict.toDF("trip_id","stop_id","shape_id","stop_sequence","stop_lat","stop_lon")

      // Routes from gtfs in geojson with their shape id
      val dict_routesGTFS = allCasesDict.groupBy("route_id","direction_id","infos").
        agg(collect_list("coor_list").alias("coor_list")).
        join(trips.select("trip_id","route_id"), Seq("route_id")).join(dict_tripId_shapeId, "trip_id").
        select("route_id","direction_id","infos","coor_list","shape_id").distinct()
      dict_routesGTFS.count()

      // Routes from gtfs in geojson with their shape id and stops
      var listShapesBySeq = dict_routesGTFS.join(master_dict_cloned.select("shape_id","stop_sequence","stop_lat","stop_lon").distinct(),"shape_id").
        orderBy("shape_id","stop_sequence").withColumn("forceReverse", lit("false")).
        orderBy("shape_id","route_id","direction_id","stop_sequence")

      // Add those GeoJson routes which have the oppossite direction in GTFS

      /*val possibleOpposites = listShapesBySeq.filter($"direction_id" === 0).
        withColumn("direction_id", lit(1)).select("route_id","direction_id").
        join(listShapesBySeq.select("route_id","direction_id"), Seq("route_id","direction_id"),"leftanti").distinct().
        join(trips.select("trip_id","route_id","direction_id"), Seq("route_id","direction_id")).
        join(dict_tripId_shapeId, "trip_id").select("route_id","direction_id","shape_id").distinct().join(master_dict_cloned.select("shape_id","stop_sequence","stop_lat","stop_lon").distinct(),"shape_id").orderBy("shape_id","stop_sequence").withColumn("forceReverse", lit("true"))
      val possibleOppositesCount = possibleOpposites.count()

      if (possibleOppositesCount > 0) {
        listShapesBySeq = listShapesBySeq.select("route_id","coor_list").distinct().
          join(possibleOpposites, "route_id").orderBy("shape_id","stop_sequence").
          select("shape_id","route_id","direction_id","coor_list","stop_sequence","stop_lat","stop_lon","forceReverse").union(listShapesBySeq)
      }
  */
      val wDistInit = Window.partitionBy("shape_id").orderBy("stop_sequence")
      //Define new window partition to operate on row frame
      val wDistLast = Window.partitionBy("shape_id").orderBy("stop_sequence").rowsBetween(Window.currentRow, Window.unboundedFollowing)

      val shapesRev = listShapesBySeq.
        withColumn("stop_lat_init", first("stop_lat").over(wDistInit)).
        withColumn("stop_lon_init", first("stop_lon").over(wDistInit)).
        withColumn("stop_lat_last", last("stop_lat").over(wDistLast)).
        withColumn("stop_lon_last", last("stop_lon").over(wDistLast)).
        withColumn("coor_flat", explode($"coor_list")).orderBy("shape_id","route_id","direction_id","stop_sequence").distinct()

      // Get Valid GeoJson Shape for each GTFS Shape
      val wShapes = Window.partitionBy("shape_id").orderBy(asc("distance"))
      var dict_shapesInGJ = shapesRev.withColumn("distance", UDFS.validShapeTcl($"coor_flat", $"infos", $"direction_id", $"stop_lat_init", $"stop_lon_init", $"stop_lat_last", $"stop_lon_last", $"forceReverse")).
        withColumnRenamed("coor_flat","selected_shapes").
        select("shape_id","route_id","direction_id","infos","stop_sequence","stop_lat","stop_lon","stop_lat_init","stop_lat_last","stop_lon_init","stop_lon_last","selected_shapes","distance","forceReverse").
        withColumn("rn", min($"distance").over(wShapes)).
        filter($"distance" === $"rn").drop("distance","rn")

      dict_shapesInGJ = dict_shapesInGJ.
        withColumn("test1", lit("normal")).
        withColumn("test2", lit("reverse")).
        withColumn("distOneDir", when($"infos" === "Circulaire", UDFS.GetProgressiveDistanceUsingShapes($"selected_shapes", $"stop_lat", $"stop_lon",$"test1")).otherwise(null)).
        withColumn("distOppoDir", when($"infos" === "Circulaire", UDFS.GetProgressiveDistanceUsingShapes($"selected_shapes", $"stop_lat", $"stop_lon",$"test2")).otherwise(null)).
        withColumn("SumDistOneDir", sum("distOneDir").over(Window.partitionBy("shape_id","route_id"))).
        withColumn("SumDistOppoDir", sum("distOppoDir").over(Window.partitionBy("shape_id","route_id"))).
        withColumn("selected_shapes", when($"SumDistOppoDir" <= $"SumDistOneDir", UDFS.reverseRetour($"selected_shapes")).otherwise($"selected_shapes")).drop("test1","test2","distOneDir","distOppoDir","SumDistOneDir","SumDistOppoDir")

      val wSpec2 = Window.partitionBy("shape_id").orderBy("stop_sequence")
      // Get shapes for stops
      val pointsProp = dict_shapesInGJ.withColumn("stop_lat_dest" ,lag("stop_lat", -1,null).over(wSpec2)).
        withColumn("stop_lon_dest" ,lag("stop_lon", -1,null).over(wSpec2)).
        withColumn("pointsBetweenStops", UDFS.pointsBetweenStops($"selected_shapes",$"stop_lat_init",$"stop_lon_init",$"stop_lat",$"stop_lon",$"stop_lat_dest",$"stop_lon_dest",$"forceReverse")).
        withColumn("size", size($"pointsBetweenStops")).
        filter($"size" > 1).drop("size","selected_shapes").dropDuplicates()

      // Reducted shapes
      val reductedShapes = pointsProp.withColumn("reduced", UDFS.reduceShapes($"pointsBetweenStops"))
        .select("shape_id","stop_sequence","pointsBetweenStops","reduced").
        withColumn("points", explode($"reduced")).select("shape_id","stop_sequence","points").withColumn("shape_pt_lat", $"points._1").
        withColumn("shape_pt_lon", $"points._2")

      // Add sequence and dist travelled
      val wDist = Window.partitionBy("shape_id").orderBy("shape_pt_sequence")
      val shapeDFlastDist = reductedShapes.withColumn("RowN", row_number().over(Window.partitionBy($"shape_id").orderBy("stop_sequence"))).
        withColumn("shape_pt_sequence", $"RowN" - 1).
        withColumn("shape_pt_sequence",      'shape_pt_sequence.cast("Int")).
        withColumn("stop_lat_prev", lag("shape_pt_lat", 1,0).over(wDist)).withColumn("stop_lon_prev", lag("shape_pt_lon", 1,0).over(wDist)).
        withColumn("a", functions.sin(functions.radians($"stop_lat_prev" - $"shape_pt_lat") / 2) * functions.sin(functions.radians($"stop_lat_prev" - $"shape_pt_lat") / 2) + functions.cos(functions.radians($"shape_pt_lat")) * functions.cos(functions.radians($"stop_lat_prev")) * functions.sin(functions.radians($"stop_lon_prev" - $"shape_pt_lon") / 2) * functions.sin(functions.radians($"stop_lon_prev" - $"shape_pt_lon") / 2)).withColumn("c", functions.atan2(functions.sqrt($"a"), functions.sqrt(-$"a" + 1)) * 2)
        .withColumn("shape_dist_traveled",  when($"shape_pt_sequence" === 0, 0).otherwise($"c" * 6371000)).
        select("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled")

      // Generate shape final

      val shapes_cloned = shapes.toDF("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled")
      var shapesDFlast = shapes_cloned.alias("shapeDF").join(shapeDFlastDist.select("shape_id").alias("shapeDFlastDist"), Seq("shape_id"), "leftouter").where($"shapeDFlastDist.shape_id".isNull).drop($"shapeDFlastDist.shape_id")
      shapesDFlast.union(shapeDFlastDist).orderBy("shape_id","shape_pt_sequence")

      shapesDFlast.checkpoint()

      dataframesOutput += (routes,stops,trips,stop_times,agency,calendar_dates,calendar,transfers,shapesDFlast)
      dataframesOutput.toList

    }

    def ExportGTFSFromGeoJson(dataframes: List[DataFrame],spark: SparkSession) : List[DataFrame] = {

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
      val shapes = dataframes(9)

      var dataframesOutput = new mutable.ListBuffer[DataFrame]

      var routesToGenerateGTFSZipFile = Seq("213113460:460",
        "213113027:27",
        "100110001:1",
        "100112012:T2",
        "100100565:565",
        "100100563:563",
        "100100564:564",
        "100100541:EOLIEN",
        "100100560:LIGNE-R",
        "100100559:LIGNE-B",
        "100100544:AS-SURE",
        "100100073:73",
        "100100072:72",
        "100100360:360",
        "100100367:367",
        "100100378:378",
        "100100052:52",
        "100100459:459",
        "100100467:467",
        "100100093:93",
        "100100176:176",
        "100100175:175",
        "100100157:157",
        "100100158:158",
        "100100159:159",
        "100100163:163",
        "100100160:160",
        "100100141:141",
        "100100144:144",
        "100100304:304",
        "100100276:276",
        "100100259:259",
        "100100258:258",
        "100100241:241",
        "100100244:244",
        "810:A",
        "800:L",
        "800:J",
        "800:U",
        "213113460:460",
        "213113027:27").toDF("route_id")

      val valid_routes = routes.join(routesToGenerateGTFSZipFile, Seq("route_id")).distinct()
      val valid_agency = agency.join(valid_routes.select("agency_id"), Seq("agency_id")).distinct()
      val valid_trips = trips.join(valid_routes.select("route_id"), Seq("route_id")).distinct()
      val valid_stop_times = stop_times.join(valid_trips.select("trip_id"), Seq("trip_id")).distinct()
      val valid_stops = stops.join(valid_stop_times.select("stop_id"), Seq("stop_id")).distinct()
      var res5 = stops.join(transfers, valid_stops("stop_id") <=> transfers("from_stop_id"))
      var res6= stops.join(transfers, valid_stops("stop_id") <=> transfers("to_stop_id"))
      val valid_transfers = res5.union(res6).select("from_stop_id",
        "to_stop_id",
        "transfer_type",
        "min_transfer_time").distinct()
      val valid_calendar = calendar.join(valid_trips.select("service_id"), Seq("service_id")).distinct()
      val valid_calendar_dates = calendar_dates.join(valid_calendar.select("service_id"), Seq("service_id")).distinct()
      val valid_shapes = shapes.join(trips.select("shape_id"), Seq("shape_id")).distinct()

      dataframesOutput += (valid_routes,valid_stops,valid_trips,valid_stop_times,valid_agency,valid_calendar_dates,valid_calendar,valid_transfers,stop_extensions,valid_shapes)
      dataframesOutput.toList
    }

    def RemoveUnwantedRoutes(dataframes: ListBuffer[DataFrame],spark: SparkSession) : List[DataFrame] = {

      import spark.implicits._
      val routes = dataframes(0)
      val stops = dataframes(1)
      val trips = dataframes(2)
      val stop_times = dataframes(3)
      val agency = dataframes(4)
      val calendar_dates = dataframes(5)
      val calendar = dataframes(6)
      val transfers = dataframes(7)
      //val shapes = dataframes(9)
      var dataframesOutput = new mutable.ListBuffer[DataFrame]

      val desiredAgencies = Seq("TCL Sytral", "SNCF", "Rhônexpress").toDF("agency_name")
      // Clean routes
      var valid_agency = agency.join(desiredAgencies, Seq("agency_name")).distinct()
      var valid_routes = routes.join(valid_agency.select("agency_id"), Seq("agency_id")).distinct()
      valid_routes = routes.join(valid_routes.select("route_id"), Seq("route_id")).distinct()
      //Random colours for routes
      valid_routes = valid_routes.withColumn("route_color", UDFS.hexToLongTcl(col("route_color")))
      //valid_routes.checkpoint()
      var valid_trips = trips.join(valid_routes.select("route_id"), Seq("route_id")).distinct()
      var valid_stop_times = stop_times.join(valid_trips.select("trip_id"), Seq("trip_id")).distinct()
      var valid_stops = stops.join(valid_stop_times.select("stop_id"), Seq("stop_id")).distinct()
      var res5 = stops.join(transfers, valid_stops("stop_id") <=> transfers("from_stop_id"))
      var res6= stops.join(transfers, valid_stops("stop_id") <=> transfers("to_stop_id"))
      var valid_transfers = res5.union(res6).select("from_stop_id",
        "to_stop_id",
        "transfer_type",
        "min_transfer_time").distinct()
      var valid_calendar = calendar.join(valid_trips.select("service_id"), Seq("service_id")).distinct()
      var valid_calendar_dates = calendar_dates.join(valid_calendar.select("service_id"), Seq("service_id")).distinct()
      //val valid_shapes = shapes.join(trips.select("shape_id"), Seq("shape_id")).distinct()

      println("Start Remove Type Bus And RailWay Except The Ones Which Pass Through Lyon Of SNCF")
      var routesToRemove =  valid_routes.filter($"agency_id" === "sncf" && ($"route_type" === "2" || $"route_type" === "3")).select("route_id")
      var railwayServiceRoutesInLyon = valid_routes.
        filter($"agency_id" === "sncf" && upper($"route_long_name").contains("LYON")  && $"route_type" === "2").select("route_id")
      routesToRemove = routesToRemove.join(railwayServiceRoutesInLyon.alias("rem"), Seq("route_id"), "leftouter").where($"rem.route_id".isNull).drop($"rem.route_id")
      valid_routes = valid_routes.join(routesToRemove.alias("rem2"),Seq("route_id"),"leftouter").where($"rem2.route_id".isNull).drop($"rem2.route_id")
      println("Finish Remove Type Bus And RailWay Except The Ones Which Pass Through Lyon Of SNCF")

      //Removing again unwanted trips, stops...
      println("Routes === " + valid_routes.count())
      valid_trips = valid_trips.join(valid_routes.select("route_id"), Seq("route_id")).distinct()


      valid_stop_times = valid_stop_times.join(valid_trips.select("trip_id"), Seq("trip_id")).distinct()


      valid_stops = valid_stops.join(valid_stop_times.select("stop_id"), Seq("stop_id")).distinct()


      res5 = valid_stops.join(valid_transfers, valid_stops("stop_id") <=> valid_transfers("from_stop_id"))
      res6= valid_stops.join(valid_transfers, valid_stops("stop_id") <=> valid_transfers("to_stop_id"))
      valid_transfers = res5.union(res6).select("from_stop_id",
        "to_stop_id",
        "transfer_type",
        "min_transfer_time").distinct()


      valid_calendar = valid_calendar.join(valid_trips.select("service_id"), Seq("service_id")).distinct()


      valid_calendar_dates = valid_calendar_dates.join(valid_calendar.select("service_id"), Seq("service_id")).distinct()

      println("Start Apply random color to route based on id")

      /*
      valid_routes.withColumn("route_color", when(
        col("route_color").equalTo("000000") ||
          col("route_color").equalTo("-16777216") ||
          col("route_color").isNull, UDFS.hexToLong(col("route_color"))).
        otherwise(col("route_color")))
      */
      // new approach: in hexadecimal
      valid_routes = valid_routes.withColumn("route_color", UDFS.toHexadecimal(col("route_color")))


      println("Finish Apply random color to route based on id")
      println("")


      dataframesOutput += (valid_routes,valid_stops,valid_trips,valid_stop_times,valid_agency,valid_calendar_dates,valid_calendar,valid_transfers)
      dataframesOutput.toList
    }

    /*

      def GTFSFeedStatus(store: GtfsDaoImpl, file: String) : GtfsDaoImpl = {

        val sb = new StringBuilder
        sb.append(" Routes: " + store.getAllRoutes.size())
        sb.append(" Trips: " + store.getAllTrips.size())
        sb.append(" Shapes: " + store.getAllShapePoints.size())
        sb.append(" StopTimes: " + store.getAllStopTimes.size())
        sb.append(" Stops: " + store.getAllStops.size())
        sb.append(" Calendar: " + store.getAllCalendars.size())
        sb.append(" Calendar Dates: " + store.getAllCalendarDates.size())
        sb.append(" Transfers: " + store.getAllTransfers.size())
        sb.append(" FareRules: " + store.getAllFareRules.size())
        sb.append(" FareAttributes: " + store.getAllFareAttributes.size())
        sb.append(" Frequency: " + store.getAllFrequencies.size())
        println(sb.toString())
        return store
      }

    */

    /*
    ANTIGUO
    val main_join = trips.join(stop_times,"trip_id").distinct().join(routes,"route_id").distinct().join(stops,"stop_id").distinct().select("route_id","direction_id","trip_id","route_type","stop_id","stop_sequence","stop_lat","stop_lon","service_id","trip_headsign","trip_short_name","block_id","wheelchair_accessible")
      val main_join_cached = main_join.cache()
      // generating maxtrips

      val to_hash = main_join_cached.select("trip_id","route_id","direction_id","stop_id","stop_sequence").distinct().
        withColumn("stop_sequence", 'stop_sequence.cast("int")).
        withColumn("shape_concat", concat_ws(";",$"stop_sequence",$"route_id",$"direction_id",$"stop_id")).
        withColumn("Test", collect_list("shape_concat").over(Window.partitionBy("trip_id")
          .orderBy("stop_sequence")))

      val maxtrip = to_hash.groupBy($"trip_id").agg(max($"stop_sequence").alias("stop_sequence")).orderBy("trip_id","stop_sequence")
      val map_tripidtest = maxtrip.join(to_hash.select("trip_id","stop_sequence","Test"),Seq("trip_id","stop_sequence")).
        withColumn("shape_id", hash(concat_ws(";", $"Test"))).drop("stop_sequence","Test")

      // master

      val master = main_join_cached.join(map_tripidtest,"trip_id")

      // Calculate shape_dist_traveled

      val wSpec2 = Window.partitionBy("route_id","trip_id").orderBy("stop_sequence")

      val withLonLats = master.withColumn("stop_sequence",      'stop_sequence.cast("Int")).
        withColumn("stop_lat_prev", lag("stop_lat", 1,0).over(wSpec2)).withColumn("stop_lon_prev", lag("stop_lon", 1,0).over(wSpec2))

      // Generate master_shapes

      val master_shapes = withLonLats.withColumn("a", sin(radians($"stop_lat_prev" - $"stop_lat") / 2) * sin(radians($"stop_lat_prev" - $"stop_lat") / 2) + cos(radians($"stop_lat")) * cos(radians($"stop_lat_prev")) * sin(radians($"stop_lon_prev" - $"stop_lon") / 2) * sin(radians($"stop_lon_prev" - $"stop_lon") / 2)).withColumn("c", atan2(sqrt($"a"), sqrt(-$"a" + 1)) * 2)
        .withColumn("shape_dist_travelled",  when($"stop_sequence" === 0, 0).otherwise($"c" * 6371000)).select("route_id","direction_id","trip_id","route_type","stop_id","stop_sequence","stop_lat","stop_lon", "shape_id","shape_dist_travelled","service_id","trip_headsign","trip_short_name","block_id","wheelchair_accessible")

      val master_shapes_filtered = master_shapes.filter($"route_type" === 2)
      val master_shapes_filtered_cached = master_shapes_filtered .cache()

      // group by max combinations

      val wSpec3 = Window.partitionBy("shape_id","stop_id").orderBy("distance").rowsBetween(Long.MinValue, 0)

      val master_shapes_dist = master_shapes_filtered.select("shape_id","stop_id","stop_sequence").distinct().
        join(master_shapes_filtered.select($"shape_id",$"stop_id".alias("stop_id_prev"),$"stop_sequence".alias("stop_sequence_prev")),Seq("shape_id")).
        filter($"stop_sequence" > $"stop_sequence_prev").
        withColumn("distance", $"stop_sequence"-$"stop_sequence_prev").dropDuplicates().orderBy("shape_id")
      val finalt = master_shapes_dist.orderBy($"stop_sequence",$"stop_sequence_prev",$"distance").withColumn("ZZZ", collect_list("stop_id_prev").over(wSpec3))
      val testmax = finalt.groupBy($"stop_id",$"stop_id_prev").agg(max(size($"ZZZ")).alias("distance_a")).withColumnRenamed("stop_id","stop_id_a").withColumnRenamed("stop_id_prev","stop_id_prev_a")
      val testmax1 = testmax.join(finalt,
        testmax("stop_id_a")<=>finalt("stop_id")&&
          testmax("stop_id_prev_a")<=>finalt("stop_id_prev")&&
          testmax("distance_a")<=>finalt("distance"),"leftouter").dropDuplicates()

      // Final table with combinations

      val tmp1 = master_shapes_filtered.withColumn("stop_id_prev", lag("stop_id", 1,0).over(wSpec2)).
        withColumnRenamed("stop_id_prev","stop_id_final").
        withColumnRenamed("stop_id","stop_id_init").
        select("shape_id","stop_id_init","stop_id_final").
        distinct()//.filter($"stop_id"==="StopPoint:8754730:800:C" && $"stop_id_prev"==="StopPoint:8739303:800:C")

      val tmp2 = testmax1.drop("shape_id").distinct()//.filter($"stop_id"==="StopPoint:8754730:800:C" && $"stop_id_prev"==="StopPoint:8739303:800:C")
      // UDF: sort array in reverse order

      import scala.collection.mutable.WrappedArray
      val sortArr = udf { arr: WrappedArray[String] =>
        arr.sortBy(- _.size).reverse
      }

      val finaltabla =  tmp1.join(tmp2,tmp1("stop_id_init") <=> tmp2("stop_id_a")
        && tmp1("stop_id_final") <=> tmp2("stop_id_prev_a"), "leftouter").drop("stop_id_a","stop_id_prev_a","distance_a","stop_sequence","stop_sequence_prev", "stop_id","stop_id_prev").
        dropDuplicates().
        filter(size($"ZZZ")>1).select($"shape_id",$"stop_id_init",$"stop_id_final", sortArr($"ZZZ") as "stops_include").withColumnRenamed("shape_id","shape_otro")

      val table_seq = master_shapes_filtered_cached.join(finaltabla,
        master_shapes_filtered_cached("shape_id") === finaltabla("shape_otro") &&
          master_shapes_filtered_cached("stop_id") === finaltabla("stop_id_final") ,"leftouter").orderBy("stop_sequence").
        drop("shape_otro").
        select("route_id","shape_id","trip_id", "stop_id", "stop_sequence","stop_id_init","stop_id_final", "stops_include","direction_id","route_type","stop_lat","stop_lon","service_id","trip_headsign","trip_short_name","block_id","wheelchair_accessible")

      val table_final = table_seq.withColumn("stops_include", when($"stops_include".isNull, array($"stop_id")).otherwise($"stops_include")).withColumn("stops_include", explode($"stops_include")).
        coalesce(1).orderBy("stop_sequence")
        .withColumn("stop_sequence2", monotonically_increasing_id)
        .drop("stop_sequence","stop_id_init","stop_id_final","stop_id")
        .withColumnRenamed("stop_sequence2","stop_sequence")
        .withColumnRenamed("stops_include","stop_id")

      val combinedDF = table_final.withColumn("Row_Num", lit(Long.MaxValue))


      val resultDF = combinedDF.select(
        col("route_id"), col("trip_id"),col("shape_id"),col("stop_id"), col("stop_sequence"), col("route_type"), col("direction_id"), col("service_id"), col("trip_headsign"), col("trip_short_name"),  col("block_id"), col("wheelchair_accessible"), row_number().over(
          Window.partitionBy(col("trip_id")).orderBy(col("Row_Num"))
        ).alias("New_Row_Num")).withColumn("stops", $"New_Row_Num"-1)
        .withColumnRenamed("stops", "stop_sequencea").drop("New_Row_Num","stop_sequence").withColumnRenamed("stop_sequencea", "stop_final")

      val tables_join = resultDF.join(master,Seq("stop_id","trip_id","route_id","shape_id","direction_id","route_type","service_id","trip_headsign","trip_short_name","block_id","wheelchair_accessible"),"leftouter").drop("stop_sequence").orderBy("stop_final").dropDuplicates().withColumnRenamed("stop_final", "stop_sequence").drop("stop_lat","stop_lon").join(stops,"stop_id").select("stop_id","trip_id","route_id","shape_id","direction_id","route_type","service_id","trip_headsign","trip_short_name","block_id","wheelchair_accessible","stop_lat","stop_lon","stop_sequence")

      val wSpec4 = Window.partitionBy("route_id","trip_id").orderBy("stop_sequence")

      val LonLats = tables_join.withColumn("stop_sequence",      'stop_sequence.cast("Int")).
        withColumn("stop_lat_prev", lag("stop_lat", 1,0).over(wSpec2)).withColumn("stop_lon_prev", lag("stop_lon", 1,0).over(wSpec4))

      val master_table = LonLats.withColumn("a", sin(radians($"stop_lat_prev" - $"stop_lat") / 2) * sin(radians($"stop_lat_prev" - $"stop_lat") / 2) + cos(radians($"stop_lat")) * cos(radians($"stop_lat_prev")) * sin(radians($"stop_lon_prev" - $"stop_lon") / 2) * sin(radians($"stop_lon_prev" - $"stop_lon") / 2)).withColumn("c", atan2(sqrt($"a"), sqrt(-$"a" + 1)) * 2)
        .withColumn("shape_dist_travelled",  when($"stop_sequence" === 0, 0).otherwise($"c" * 6371000)).select("route_id","direction_id","route_type","trip_id","stop_id","stop_sequence","stop_lat","stop_lon", "shape_id","shape_dist_travelled","service_id","trip_headsign","trip_short_name","block_id","wheelchair_accessible")

      val shapes_df = master_table.orderBy("trip_id").withColumnRenamed("stop_lat", "shape_pt_lat").withColumnRenamed("stop_lon", "shape_pt_lon").withColumnRenamed("stop_sequence", "shape_pt_sequence").withColumnRenamed("shape_dist_travelled", "shape_dist_traveled").orderBy("shape_id","shape_pt_sequence").dropDuplicates().
        select("shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence","shape_dist_traveled").distinct()

      val trips_df = master_table.orderBy("trip_id").select("trip_id","route_id","service_id","trip_headsign","trip_short_name","direction_id","block_id","shape_id","wheelchair_accessible").distinct()

      shapes_df.coalesce(1).write.mode("overwrite").parquet(raw_path + "shapes.parquet")
      trips_df.coalesce(1).write.mode("overwrite").parquet(raw_path + "trips.parquet")
      agency.coalesce(1).write.mode("overwrite").parquet(raw_path + "agency.parquet")
      calendar.coalesce(1).write.mode("overwrite").parquet(raw_path + "calendar.parquet")
      calendar_dates.coalesce(1).write.mode("overwrite").parquet(raw_path + "calendar_dates.parquet")
      stops.coalesce(1).write.mode("overwrite").parquet(raw_path + "stops.parquet")
      routes.coalesce(1).write.mode("overwrite").parquet(raw_path + "routes.parquet")
      stop_times.coalesce(1).write.mode("overwrite").parquet(raw_path + "stop_times.parquet")
      stop_extensions.coalesce(1).write.mode("overwrite").parquet(raw_path + "stop_extensions.parquet")
      transfers.coalesce(1).write.mode("overwrite").parquet(raw_path + "transfers.parquet")

     */


}
