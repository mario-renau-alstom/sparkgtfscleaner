package com.alstom.GTFSOperations

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.WrappedArray

object UDFS {

  /*
  * UDF -> Change color_routes field
   */
  def uniqueColor (hex_word: String) : String = {
    val seed = 342984
    val hash =  hex_word.toCharArray
    var res = 0
    for (z <- hash) {
      res = res + z
    }
    res = (res * seed) % 10000000
    val hex = res.toHexString.toUpperCase
    implicit def hex2int (hex: String): Int = Integer.parseInt(hex, 16)
    val intAgain = hex2int(hex)
    return intAgain.toHexString.toUpperCase
  }
  def hexToLong = udf((hex: String) => uniqueColor(hex))

  /*
  * UDF -> Change color_routes field by geojson ones (SystemX)
   */
  def getColor (rgb_string: String) : Int = {
    val redIndex = rgb_string.split("#")(0).toInt
    val greenIndex = rgb_string.split("#")(1).toInt
    val blueIndex = rgb_string.split("#")(2).toInt
    val rgb = List(redIndex,greenIndex,blueIndex)
    val hex = String.format("%2s%2s%2s",
      rgb(0).toHexString.toUpperCase,
      rgb(1).toHexString.toUpperCase,
      rgb(2).toHexString.toUpperCase).replace(' ','0').toString

    return Integer.parseInt(hex, 16)
  }
  def rgbToInt = udf((rgb: String) => getColor(rgb))

  /*
  * UDF -> First case get code
   */
  def getCode (name: String, id:String) : String = {

    val route_intCode = id.split(":").head.substring(id.split(":").head.size - 3)
    val code = (route_intCode + name).replaceFirst("^0+(?!$)", "")
    return code
  }
  def completeCode = udf((route_name: String, route_id:String) => getCode(route_name,route_id))

  /*
  * UDF -> Second case get code
   */
  def getIntCode (id:String) : String = {

    val route_intCode = id.split(":").head.substring(id.split(":").head.size - 3)
    return route_intCode.toString
  }
  def IntCode = udf((route_id:String) => getIntCode(route_id))

  /*
   * UDF -> Get Valid shape
    */

  def getValidShape (array: collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]],
                     stopp_lat_init: Double, stopp_lon_init: Double,
                     _stopp_lat_last : Double,
                     _stopp_lon_last: Double,
                     _reverse:String) : Double = {

    val lons = array.map(_.head)
    val lats = array.map(_.last)
    val shape_first = (stopp_lat_init,stopp_lon_init)
    val shape_last = (_stopp_lat_last,_stopp_lon_last)
    val lessDistanceFound = Double.MaxValue


  var distancesForFirstStop = new ListBuffer[(Double,Double,Double)]()
  var distancesForLastStop = new ListBuffer[(Double,Double,Double)]()

  var tupleReturn : collection.mutable.WrappedArray [(Double,Double)]= collection.mutable.WrappedArray.empty[(Double,Double)]
    if (_reverse == "false") {
      tupleReturn = lats.zip(lons)
    }
    if (_reverse == "true") {
      tupleReturn = lats.zip(lons).reverse
    }

  import scala.math._
  for (point <- tupleReturn) {

    distancesForFirstStop += ((point._1,point._2,(sqrt(pow(point._1 - shape_first._1, 2) + pow(point._2 - shape_first._2, 2)))))
    distancesForLastStop +=  ((point._1,point._2,(sqrt(pow(point._1 - shape_last._1, 2) + pow(point._2 - shape_last._2, 2)))))
  }
  distancesForFirstStop = distancesForFirstStop.sortBy(_._3)
  distancesForLastStop = distancesForLastStop.sortBy(_._3)



  var a = sin(toRadians(distancesForFirstStop.toList.head._1 - shape_first._1) / 2) * sin(toRadians(distancesForFirstStop.toList.head._1 - shape_first._1) / 2)
    + cos(toRadians(shape_first._1)) * cos(toRadians(distancesForFirstStop.toList.head._1)) *
      sin(toRadians(distancesForFirstStop.toList.head._2 - shape_first._2) / 2) *
      sin(toRadians(distancesForFirstStop.toList.head._2 - shape_first._2) / 2)

  var b = sin(toRadians(distancesForLastStop.toList.head._1 - shape_last._1) / 2) *
    sin(toRadians(distancesForLastStop.toList.head._1 - shape_last._1) / 2) + cos(toRadians(shape_last._1)) *
    cos(toRadians(distancesForFirstStop.toList.head._1)) * sin(toRadians(distancesForLastStop.toList.head._2 - shape_last._2) / 2) *
    sin(toRadians(distancesForLastStop.toList.head._2 - shape_last._2) / 2)

  var distFromFirstStopToGJ = (atan2(sqrt(a), sqrt(-a + 1)) * 2) * 6371000
  var distFromLastStopToGJ = (atan2(sqrt(b), sqrt(-b + 1)) * 2) * 6371000

  distFromFirstStopToGJ + distFromLastStopToGJ
}
 def validShape = udf((array: collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]], _stop_lat_init: Double, stop_lon_init: Double, _stop_lat_last : Double, _stop_lon_last: Double, reverse: String) => getValidShape(array,_stop_lat_init,stop_lon_init,_stop_lat_last,_stop_lon_last, reverse))

  /*
   * UDF -> Integrate shapes between stops
    */

  def getPoints (array: collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]], _stop_lat_init: Double, _stop_lon_init: Double, _stop_lat : Double, _stop_lon:Double, _stop_lat_dest: Double, _stop_lon_dest: Double, _reverse:String) : List[(Double,Double)] = {
    val lons = array.map(_.head)
    val lats = array.map(_.last)
    var distancesForFirstStop = new ListBuffer[(Double,Double,Double,Int)]()
    var distancesForLastStop = new ListBuffer[(Double,Double,Double,Int)]()
    var counter = -1
    var currentStop = (_stop_lat , _stop_lon)
    var nextStop = (_stop_lat_dest, _stop_lon_dest)
    var shapesToReturn = new ListBuffer[(Double, Double)]

    var tupleReturn : collection.mutable.WrappedArray [(Double,Double)]= collection.mutable.WrappedArray.empty[(Double,Double)]
    if (_reverse == "false") {
      tupleReturn = lats.zip(lons)
    }
    if (_reverse == "true") {
      tupleReturn = lats.zip(lons).reverse
    }

    for (point <- tupleReturn) {
      import scala.math._
      counter = counter + 1
      distancesForFirstStop += ((point._1,point._2,(sqrt(pow(point._1 - currentStop._1, 2) + pow(point._2 - currentStop._2, 2))),counter))
      //groups of shapes with (lat,lon,distance)
      distancesForLastStop +=  ((point._1,point._2,(sqrt(pow(point._1 - nextStop._1, 2) + pow(point._2 - nextStop._2, 2))),counter))
    }

    var shapeCurrentStop = (distancesForFirstStop.sortBy(_._3).toList.head._1,
      distancesForFirstStop.sortBy(_._3).toList.head._2,
      distancesForFirstStop.sortBy(_._3).toList.head._3,
      distancesForFirstStop.sortBy(_._3).toList.head._4) //tuple way
    var shapeNextStop =  (distancesForLastStop.sortBy(_._3).toList.head._1,
      distancesForLastStop.sortBy(_._3).toList.head._2,
      distancesForLastStop.sortBy(_._3).toList.head._3,
      distancesForLastStop.sortBy(_._3).toList.head._4) //tuple way

    counter = -1 // Reset counter
    for (point <- tupleReturn) {
      counter = counter + 1
      if (shapeCurrentStop._4 <= counter && counter <= shapeNextStop._4) {
        shapesToReturn += point
      }
    }

    if (shapesToReturn.size > 1) {
      shapesToReturn.remove(shapesToReturn.size -1)
    }

    shapesToReturn.toList

  }
  def pointsBetweenStops = udf((array: collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]], _stop_lat_init: Double, _stop_lon_init: Double, _stop_lat : Double, _stop_lon:Double, _stop_lat_dest: Double, _stop_lon_dest: Double, reverse:String) => getPoints(array,_stop_lat_init,_stop_lon_init,_stop_lat,_stop_lon,_stop_lat_dest,_stop_lon_dest, reverse))

  /*
   * UDF -> Reduce points by applying Douglas Peucker algorythm
    */
  def reduce(data: Seq[Row], startIndex: Int, endIndex: Int): List[(Double,Double)] = {

    val row = data.map{
      case Row(x:Double,y:Double) =>
        (x,y)
    }

    import scala.annotation.tailrec
    val epsilon = 0.00012
    @tailrec
    def calcMaxDistance(index: Int, maxDist: Double, farthestIndex: Int): Tuple2[Double, Int] = {

      if (index < endIndex) {
        val distance = perpendicularDistance(row(startIndex), row(endIndex),
          row(index))
        if (distance > maxDist) {
          calcMaxDistance(index + 1, distance, index)
        } else {
          calcMaxDistance(index + 1, maxDist, farthestIndex)
        }
      } else {
        (maxDist, farthestIndex)
      }
    }

    val dist = calcMaxDistance(startIndex, 0, endIndex)
    val maxDistance = dist._1
    val index = dist._2

    if (maxDistance > epsilon && index != 0) {
      val upperResults = reduce(data, startIndex, index)
      val lowerResults = reduce(data, index + 1, endIndex)
      (upperResults.distinct ++ lowerResults.distinct)
    } else {
      List(row(startIndex), row(endIndex))
    }


  }
  def perpendicularDistance(firstPoint: (Double,Double), lastPoint: (Double,Double), point: (Double,Double)) = {

    val area = Math.abs(.5 * (firstPoint._1 * lastPoint._2 + lastPoint._1 *
      point._2 + point._1 * firstPoint._2 - lastPoint._1 * firstPoint._2 - point._1 *
      lastPoint._2 - firstPoint._1 * point._2));
    val base = Math.sqrt(Math.pow(firstPoint._1 - lastPoint._1, 2) +
      Math.pow(firstPoint._2 - lastPoint._2, 2));
    val height = area / base * 2;

    height
  }
  def reduceShapes = udf((data: Seq[Row]) => reduce(data,0,data.size-1))

  /*
    ** LYON **
   */

  /*
* UDF -> Change color_routes field by geojson ones (Tcl)
*/
  def getColorTcl (rgb_string: String) : Int = {
    val redIndex = rgb_string.split(" ")(0).toInt
    val greenIndex = rgb_string.split(" ")(1).toInt
    val blueIndex = rgb_string.split(" ")(2).toInt
    val rgb = List(redIndex,greenIndex,blueIndex)
    val hex = String.format("%2s%2s%2s",
      rgb(0).toHexString.toUpperCase,
      rgb(1).toHexString.toUpperCase,
      rgb(2).toHexString.toUpperCase).replace(' ','0').toString

    return Integer.parseInt(hex, 16)
  }
  def rgbToIntTcl = udf((rgb: String) => getColorTcl(rgb))
  /*
    * UDF -> Calculate distances
   */
  def getDistance (array_aller: collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]], array_retour: collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]]) : Double = {

    val aller_lons = array_aller.map(_.head)
    val aller_lats = array_aller.map(_.last)

    val retour_lons = array_retour.map(_.head)
    val retour_lats = array_retour.map(_.last)

    import scala.collection.mutable.ListBuffer

    import scala.math._
    var a = sin(toRadians(aller_lats.head - retour_lats.head) / 2) * sin(toRadians(aller_lats.head - retour_lats.head) / 2) + cos(toRadians(retour_lats.head)) * cos(toRadians(aller_lats.head)) * sin(toRadians(aller_lons.head - retour_lons.head) / 2) * sin(toRadians(aller_lons.head - retour_lons.head) / 2)

    var distance = (atan2(sqrt(a), sqrt(-a + 1)) * 2) * 6371000

    return distance
  }
  def calcDistances = udf((array_aller: collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]], array_retour: collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]]) => getDistance(array_aller,array_retour))

  /*
    * UDF -> Reverse retour array
   */
  def getReversed (array_retour: collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]]) : collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]] = {

    return array_retour.reverse

  }
  def reverseRetour = udf((array_retour: collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]]) => getReversed(array_retour))

  /*
    * UDF -> Get Valid shape
   */
  def getValidShapeTcl (array: collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]], infos:String, direction:String, stopp_lat_init: Double, stopp_lon_init: Double, _stopp_lat_last : Double, _stopp_lon_last: Double, _reverse:String) : Double = {

    val lons = array.map(_.head)
    val lats = array.map(_.last)
    val shape_first = (stopp_lat_init,stopp_lon_init)
    val shape_last = (_stopp_lat_last,_stopp_lon_last)
    val lessDistanceFound = Double.MaxValue
    import scala.collection.mutable.ListBuffer
    var distancesForFirstStop = new ListBuffer[(Double,Double,Double)]()
    var distancesForLastStop = new ListBuffer[(Double,Double,Double)]()

    var tupleReturn : collection.mutable.WrappedArray [(Double,Double)]= collection.mutable.WrappedArray.empty[(Double,Double)]

    if (_reverse == "false" && (!(infos == "Circulaire"))) {
      tupleReturn = lats.zip(lons)
    }
    if (_reverse == "true"  && (!(infos == "Circulaire"))) {
      tupleReturn = lats.zip(lons).reverse
    }

    if (_reverse == "false" && (infos == "Circulaire" && direction == "0")) {
      tupleReturn = lats.zip(lons)
    }
    if (_reverse == "true"  && (infos == "Circulaire" && direction == "0")) {
      tupleReturn = lats.zip(lons).reverse
    }
    import scala.math._

    if (!(tupleReturn == null)) {


      for (point <- tupleReturn) {

        distancesForFirstStop += ((point._1,point._2,(sqrt(pow(point._1 - shape_first._1, 2) + pow(point._2 - shape_first._2, 2)))))
        distancesForLastStop +=  ((point._1,point._2,(sqrt(pow(point._1 - shape_last._1, 2) + pow(point._2 - shape_last._2, 2)))))
      }
      distancesForFirstStop = distancesForFirstStop.sortBy(_._3)
      distancesForLastStop = distancesForLastStop.sortBy(_._3)



      var a = sin(toRadians(distancesForFirstStop.toList.head._1 - shape_first._1) / 2) * sin(toRadians(distancesForFirstStop.toList.head._1 - shape_first._1) / 2) + cos(toRadians(shape_first._1)) * cos(toRadians(distancesForFirstStop.toList.head._1)) * sin(toRadians(distancesForFirstStop.toList.head._2 - shape_first._2) / 2) * sin(toRadians(distancesForFirstStop.toList.head._2 - shape_first._2) / 2)

      var b = sin(toRadians(distancesForLastStop.toList.head._1 - shape_last._1) / 2) * sin(toRadians(distancesForLastStop.toList.head._1 - shape_last._1) / 2) + cos(toRadians(shape_last._1)) * cos(toRadians(distancesForFirstStop.toList.head._1)) * sin(toRadians(distancesForLastStop.toList.head._2 - shape_last._2) / 2) * sin(toRadians(distancesForLastStop.toList.head._2 - shape_last._2) / 2)

      var distFromFirstStopToGJ = (atan2(sqrt(a), sqrt(-a + 1)) * 2) * 6371000
      var distFromLastStopToGJ = (atan2(sqrt(b), sqrt(-b + 1)) * 2) * 6371000

      return distFromFirstStopToGJ + distFromLastStopToGJ
    }
    else {
      return 1000
    }
  }
  def validShapeTcl = udf((array: collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]], infos: String, direction:String, _stop_lat_init: Double, stop_lon_init: Double, _stop_lat_last : Double, _stop_lon_last: Double, reverse: String) => getValidShapeTcl(array, infos,direction, _stop_lat_init,stop_lon_init,_stop_lat_last,_stop_lon_last, reverse))

  /*
    * UDF -> Get Progressive distance (circular ones)
   */
  def getProgDist (array: collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]], stopp_lat: Double, stopp_lon: Double, infoType:String) : Double = {

    val lons = array.map(_.head)
    val lats = array.map(_.last)
    import scala.collection.mutable.ListBuffer
    var distancesForFirstStop = new ListBuffer[(Double,Double,Double)]()

    var tupleReturn : collection.mutable.WrappedArray [(Double,Double)]= collection.mutable.WrappedArray.empty[(Double,Double)]
    if (infoType == "normal") {
      tupleReturn = lats.zip(lons)
    }
    if (infoType == "reverse") {
      tupleReturn = lats.zip(lons).reverse
    }

    import scala.math._
    var count = -1
    for (point <- tupleReturn) {
      count = count + 1
      if (count>0) {
        distancesForFirstStop += ((point._1,point._2,(sqrt(pow(point._1 - stopp_lat, 2) + pow(point._2 - stopp_lon, 2)))))
      }

    }
    distancesForFirstStop = distancesForFirstStop.sortBy(_._3)

    if (!(distancesForFirstStop == null)) {


      var a = sin(toRadians(distancesForFirstStop.toList.head._1 - stopp_lat) / 2) * sin(toRadians(distancesForFirstStop.toList.head._1 - stopp_lat) / 2) + cos(toRadians(stopp_lat)) * cos(toRadians(distancesForFirstStop.toList.head._1)) * sin(toRadians(distancesForFirstStop.toList.head._2 - stopp_lon) / 2) * sin(toRadians(distancesForFirstStop.toList.head._2 - stopp_lon) / 2)

      var distance = (atan2(sqrt(a), sqrt(-a + 1)) * 2) * 6371000

      return distance
    }
    else 1000
  }
  def GetProgressiveDistanceUsingShapes = udf((array: collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]], stop_lat: Double, stop_lon: Double, infoType: String) => getProgDist(array,stop_lat,stop_lon,infoType))

  /*
    * UDF -> Get Progressive distance (circular ones)
   */
  def assertSameSize(arrs:Seq[_]*) = {
    assert(arrs.map(_.size).distinct.size==1,"sizes differ")
  }
  def zip4 = udf((xa:Seq[String],xb:Seq[String],xc:Seq[String],xd:Seq[String], xe:Seq[collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]]]) => {
    assertSameSize(xa,xb,xc,xd,xe)
    xa.indices.map(i=> (xa(i),xb(i),xc(i),xd(i),xe(i)))
  }
  )

  /*
* UDF -> Change color_routes field
 */
  def uniqueColorTcl (hex_word: String) : Int = {
    import scala.util.Random
    val ran = new Random
    val hex = Integer.toHexString(ran.nextInt(0x1000000)).toUpperCase
    implicit def hex2int (hex: String): Int = Integer.parseInt(hex, 16)
    val intAgain = hex2int(hex)
    return intAgain
  }
  def hexToLongTcl = udf((hex: String) => uniqueColorTcl(hex))

  def toHexadecimal = udf((routeNumber: Int) => routeNumber.toHexString.toUpperCase)
  def fromStrToHex = udf((routeNumber: String) => routeNumber.toInt.toHexString.toUpperCase)
}
