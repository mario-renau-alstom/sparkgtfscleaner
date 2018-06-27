package com.alstom.Launcher

import com.alstom.Launcher.Configurations._
import com.alstom.utils.ConfigUtils
import com.alstom.utils.ConfigUtils._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


object GTFSCleanerApp extends App with LazyLogging with ConfigUtils  {


  implicit val configFile = "C:\\Development\\GitHub\\sparkgtfscleaner\\src\\main\\resources\\application_dev.conf"
  implicit val config: Config = getConfig(configFile)

  // Paths ADLS
  val ONLINE_ROOT : String = config.getString(DirectoriesOnlineRoot)
  val ONLINE_BACKUP : String = config.getString(DirectoriesOnlineBackup)
  val ONLINE_SOURCE : String = config.getString(DirectoriesOnlineSource)
  val ONLINE_STAGING : String = config.getString(DirectoriesOnlineStaging)
  val WORK_PATH : String = config.getString(DirectoriesWorkPath)
  val ONLINE_RAW : String = config.getString(DirectoriesOnlineRaw)

  // GeoJson paths
    val PARIS_GJSON = List((config.getString(ParisGjsonPath01File),
      config.getString(ParisGjsonPath01Url)))

    val LYON_GJSON = List(
      (config.getString(LyionGjsonPath01File), config.getString(LyionGjsonPath01Url)),
      (config.getString(LyionGjsonPath02File), config.getString(LyionGjsonPath02Url)),
      (config.getString(LyionGjsonPath03File), config.getString(LyionGjsonPath03Url)),
      (config.getString(LyionGjsonPath04File), config.getString(LyionGjsonPath04Url)))

    var feedURL : String = null


  // Spark Session
  //implicit val spark = SparkConfigurator.getSparkSessionForEnv(config.getString(ApplicationEnv))
  System.setProperty("hadoop.home.dir", "C:/Development/winutils/hadoop-2.8.3")

  implicit val spark = SparkSession.builder
    .appName("SparkTest")
    .master("local[3]")
    //.enableHiveSupport()
    .getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", "5")
  spark.sparkContext.setLogLevel("WARN")
  spark.conf.set("hive.metastore.warehouse.dir","./target/spark-warehouse/")
   // Logging
    val logPath = "./AlstomData/Logfile.log"
    val logLevel = Level.DEBUG

  // GetOpts

  val usage = """

  [SparkGTFSCleaner Application]

  -g <value> | --gtfs <value>
    Possible values:
    --gtfs stif
    --gtfs ratp
    --gtfs madrid
    --gtfs lyon
    --help
          prints this usage text
            """

  def argsToOptionMap(args:Array[String]):Map[String,String]= {
    def nextOption(
                    argList:List[String],
                    map:Map[String, String]
                  ) : Map[String, String] = {
      val pattern       = "--(\\w+)".r // Selects Arg from --Arg
      val patternSwitch = "-(\\w+)".r  // Selects Arg from -Arg
      argList match {
        case Nil => map
        case pattern(opt)       :: value  :: tail => nextOption( tail, map ++ Map(opt->value) )
        case patternSwitch(opt) :: tail => nextOption( tail, map ++ Map(opt->null) )
        case string             :: Nil  => map ++ Map(string->null)
        case option             :: tail => {
          println("Unknown option:" + option + "Please see usage for this application")
          println("")
          println(usage)
          sys.exit(1)
        }
      }
    }
    nextOption(args.toList,Map())
  }

  argsToOptionMap(args)("gtfs") match {

    case "stif" => feedURL = "https://opendata.stif.info/explore/dataset/offre-horaires-tc-gtfs-idf/files/f24cf9dbf6f80c28b8edfdd99ea16aad/download/"
      val STIFProcess = new ParisSTIF
      STIFProcess.Process(WORK_PATH, ONLINE_BACKUP, ONLINE_SOURCE, ONLINE_RAW, feedURL, PARIS_GJSON, spark)

    case "ratp" => feedURL = "https://opendata.stif.info/explore/dataset/offre-horaires-tc-gtfs-idf/files/f24cf9dbf6f80c28b8edfdd99ea16aad/download/"
      val RATPProcess = new ParisRATP
      RATPProcess.Process(WORK_PATH, ONLINE_BACKUP, ONLINE_SOURCE, ONLINE_RAW, feedURL, PARIS_GJSON, spark)

    case "madrid" =>

      feedURL = "https://crtm.maps.arcgis.com/sharing/rest/content/items/1a25440bf66f499bae2657ec7fb40144/data"
      val MadridCercaniasProcess = new MadridCercanias
      MadridCercaniasProcess.Process(WORK_PATH, ONLINE_BACKUP, ONLINE_SOURCE, ONLINE_RAW, feedURL, spark)

      feedURL = "https://servicios.emtmadrid.es:8443/gtfs/transitemt.zip"
      val MadridEMTProcess = new MadridEMT
      MadridEMTProcess.Process(WORK_PATH, ONLINE_BACKUP, ONLINE_SOURCE, ONLINE_RAW, feedURL, spark)

      feedURL = "https://crtm.maps.arcgis.com/sharing/rest/content/items/885399f83408473c8d815e40c5e702b7/data"
      val MadridInterUrbanosProcess = new MadridInterUrbanos
      MadridInterUrbanosProcess.Process(WORK_PATH, ONLINE_BACKUP, ONLINE_SOURCE, ONLINE_RAW, feedURL, spark)

      feedURL = "https://crtm.maps.arcgis.com/sharing/rest/content/items/357e63c2904f43aeb5d8a267a64346d8/data"
      val MadridAutobusUrbanoProcess = new MadridAutobusUrbano
      MadridAutobusUrbanoProcess.Process(WORK_PATH, ONLINE_BACKUP, ONLINE_SOURCE, ONLINE_RAW, feedURL, spark)

      feedURL = "https://transitfeeds-data.s3-us-west-1.amazonaws.com/public/feeds/consorcio-regional-de-transportes-de-madrid/743/20170927/gtfs.zip"
      val MadridMetroLigeroProcess = new MadridMetroLigero
      MadridMetroLigeroProcess.Process(WORK_PATH, ONLINE_BACKUP, ONLINE_SOURCE, ONLINE_RAW, feedURL, spark)

      feedURL = "https://crtm.maps.arcgis.com/sharing/rest/content/items/5c7f2951962540d69ffe8f640d94c246/data"
      val MadridMetroProcess = new MadridMetro
      MadridMetroProcess.Process(WORK_PATH, ONLINE_BACKUP, ONLINE_SOURCE, ONLINE_RAW, feedURL, spark)


    case "lyon" => feedURL = config.getString(LyonFeedUrl)
      val LyonProcess = new Lyon
      LyonProcess.Process(WORK_PATH, ONLINE_BACKUP, ONLINE_SOURCE, ONLINE_RAW, feedURL, LYON_GJSON)

    case "torrejon" =>
      feedURL = "https://crtm.maps.arcgis.com/sharing/rest/content/items/885399f83408473c8d815e40c5e702b7/data"
      val TorrejonInterUrbanosProcess = new TorrejonInterUrbanos
      TorrejonInterUrbanosProcess.Process(WORK_PATH, ONLINE_BACKUP, ONLINE_SOURCE, ONLINE_RAW, feedURL, spark)

      feedURL = "https://crtm.maps.arcgis.com/sharing/rest/content/items/1a25440bf66f499bae2657ec7fb40144/data"
      val TorrejonCercaniasProcess = new TorrejonCercanias
      TorrejonCercaniasProcess.Process(WORK_PATH, ONLINE_BACKUP, ONLINE_SOURCE, ONLINE_RAW, feedURL, spark)

    case _ => println("No option matched")
      sys.exit(1)
  }


    /*implicit lazy val log = Logger.getLogger(getClass.getName)
    Logs.config_logger(getClass.getName, logPath, logLevel)
    log.file("[INFO] Init LOG: "+getClass.getName)
    */

    //ProcessFeed[ParisSTIF]("https://opendata.stif.info/explore/dataset/offre-horaires-tc-gtfs-idf/files/f24cf9dbf6f80c28b8edfdd99ea16aad/download/", WORK_PATH, true, "paris", spark)
    //var cki = DisplayMenu

/*
    cki match {
      case 1 =>
    }
   */

   def DisplayMenu = {

    Console.WHITE
    println("GTFS Processing Tool")
    println()
    println("1. Generate and upload Paris-STIF")
    println("2. Generate and upload Paris-RATP")
    println("3. Generate and upload Full Madrid")
    println("4. Generate and upload Lyon")
    println("5. Generate RailWay GeoJson Lyon")
    println("0. Exit")
    val result = scala.io.StdIn.readLine()
    result.toInt
  }
}
