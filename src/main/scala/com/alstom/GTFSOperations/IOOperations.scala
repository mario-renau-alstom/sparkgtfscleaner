package com.alstom.GTFSOperations

import java.io.File
import java.net.URL
import java.io.{File, FileInputStream, FileOutputStream}

import com.alstom.tools.com.cotdp.hadoop.ZipFileInputFormat
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem}
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

object IOOperations extends LazyLogging {

  val LOCAL_ENV = "local"
  val PROD_ENV = "prod"
  val ENV = LOCAL_ENV


  val isLocalEnv = ENV == LOCAL_ENV

  val fs = FileSystem.get(new Configuration())
  val hadoopConf = fs.getConf

  //System.setProperty("hadoop.home.dir", "C:\\Development\\winutils\\hadoop-2.7.0")

  def CleanWorkingDirectory(path: String) = {
    deleteDirectory(path)
    createDirectory(path)

  }

  def ExtractFiles(downloadURL: String, region: String, workPath: String, backupPath: String, sourcesPath: String, spark: SparkSession) = {

    import org.apache.hadoop.fs.Path
    val backup_path = backupPath.concat("GTFSCLEAN/" + region)
    val sources_path = sourcesPath.concat("GTFSCLEAN/" + region)


    val newFile = new File("fileName/" + region + ".zip")
    if (!newFile.getParentFile.exists) newFile.getParentFile.mkdirs
    if (!newFile.exists) newFile.createNewFile


    //val newFile = "C:\\Development\\GitHub\\sparkgtfscleaner\\src\\test\\resources\\data\\gtfs\\Lyon\\staging\\gtfs.zip"
    println("Start Download GTFS zip file")
    try {
      new URL(downloadURL) #> newFile !!; //Download file from URL
      println("Downloaded " + newFile.toString)
    }
    catch {
      case e: Throwable => println("Not downloaded file " + e)
    }
    println("Finish Download GTFS")
    println("")

    createDirectory(backup_path)
    //fs.mkdirs(new Path(backup_path))

    //fs.copyFromLocalFile(new Path(newFile.toString()), new Path(backup_path))
    copyFile(newFile.toString, backup_path)
    //fs.mkdirs(new Path(sources_path))
    createDirectory(sources_path)

    //Unzip file and copy to staging folder
    println("Start Unzip GTFS")
    IOOperations.unzip(backup_path, workPath, spark)
    println("Finish Unzip GTFS")
    println("")

    // convert to parquet files and store in sources folder

    val filesPaths_list = ArrayBuffer[String]()

    if (isLocalEnv)
      getFilesInLocalDirectory(workPath).foreach(file =>
        filesPaths_list += file.toString)
    else
      getFilesInHdfsDirectory(workPath).foreach(file => filesPaths_list += file.getPath.toString)

    println("Start Converting to Parquet files [Source]")

    for (fileX <- filesPaths_list) {
      val (fileName, fileExtension) = getFileNameAndExtFromPath(fileX.toString)
      println(s" FileName is $fileName")
      println(s" sources_path is $sources_path" )
      spark
        .read
        .option("header", "true")
        .csv(fileX.toString)
        .write
        .mode("overwrite")
        .parquet(sources_path + "/" + fileName + ".parquet")
    }
    println("Finish Converting to Parquet files [Source]")
    println("")

  }
  def DownloadGeoJson(fileName: String, geoJsonPath: String, outputPath: String) = {

    // Download file into HDFS
    //IOOperations.CleanWorkingDirectory(workPath)
    val newFile = new File(fileName)
    new URL(geoJsonPath) #> newFile !!; //Download file from URL
    import org.apache.hadoop.fs.Path
    //fs.copyFromLocalFile(new Path(newFile.toString()), outputPath)
    copyFile(newFile.toString, outputPath)

  }

  def UploadAzure(dataframes: List[DataFrame], rawPath: String, spark: SparkSession) = {

    import org.apache.hadoop.fs.{FileSystem, Path}
    val raw_path = rawPath.concat("GTFSCLEAN/")
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
    val fare_attributes: Option[DataFrame] = dataframes.lift(10)
    val fare_rules: Option[DataFrame] = dataframes.lift(11)
    val feed_info: Option[DataFrame] = dataframes.lift(12)
    val frequencies: Option[DataFrame] = dataframes.lift(13)

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
            i += 1;
            i - 1
          }
        }
      } finally out.close
      if (deleteSource) srcFS.delete(srcDir, true)
      else true
    }

    /**//*def merge(srcPath: ArrayBuffer[String], dstPath: String): Unit = {

      val hadoopConfig = new Configuration()
      val hdfs = FileSystem.get(hadoopConfig)
      for (df <- srcPath) {
        val (fileName, fileExtension) = getFileNameAndExtFromPath(df.toString)
        copyMerge(hdfs, new Path(df), hdfs, new Path(dstPath + fileName + ".csv"), true, hadoopConfig, null)
      }
      // the "true" setting deletes the source files once they are merged into the new output
    }*/

    routes.coalesce(1).write.mode("overwrite").parquet(raw_path + "routes.parquet")
    println("routes uploaded")
    stops.coalesce(1).write.mode("overwrite").parquet(raw_path + "stops.parquet")
    println("stops uploaded")
    trips.coalesce(1).write.mode("overwrite").parquet(raw_path + "trips.parquet")
    println("trips uploaded")
    stop_times.write.mode("overwrite").parquet(raw_path + "stop_times.parquet")
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
    shapes.orderBy("shape_id", "shape_pt_sequence").write.mode("overwrite").parquet(raw_path + "shapes.parquet")
    println("shapes uploaded")

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

    /*
    val listOfFiles = fs.listStatus(new Path(raw_path))
    val filesPaths_list = ArrayBuffer[String]()
    listOfFiles.foreach(x => filesPaths_list += x.getPath.toString)
    merge(filesPaths_list, "adl:///data/normal/")
    */
  }

  def getFileNameAndExtFromPath(path: String): (String, String) = {


    val ext = path.split(File.separator*2).last.split('.').last
    println(ext)
    val fileName = path.split(File.separator*2).last.split('.').head

    println(s"*** path: $path")
    println(s"*** fileName: $fileName")
    println(s"*** ext: $ext")

    return (fileName, ext)

  }

  object ProcessFile extends Serializable {
    def apply(fileName: String, records: BytesWritable, path: String): Unit = {
      import org.apache.hadoop.fs.Path
      val hadoopConf = new Configuration()
      if (records.getLength > 0) {
        val outFileStream = fs.create(new Path(path + fileName), true)
        outFileStream.write(records.getBytes)
        outFileStream.close()
      }
    }
  }

  def unzip(inputPath: String, outputPath: String, spark: SparkSession): Unit = {
    import org.apache.hadoop.fs.Path

    val fileSystem = listLeafStatuses(fs, new Path(inputPath))

    val allzip = fileSystem.filter(_.getPath.getName.endsWith("zip"))

    allzip.foreach { x =>
      val zipFileRDD = spark.sparkContext.newAPIHadoopFile(
        x.getPath.toString,
        classOf[ZipFileInputFormat],
        classOf[Text],
        classOf[BytesWritable], hadoopConf)

      zipFileRDD.foreach { y =>
        ProcessFile(y._1.toString, y._2, outputPath)
      }
    }
  }


  import org.apache.hadoop.fs.{FileSystem, Path}

  def listLeafStatuses(fs: FileSystem, basePath: Path): Seq[FileStatus] = {
    listLeafStatuses(fs, fs.getFileStatus(basePath))
  }

  def listLeafStatuses(fs: FileSystem, baseStatus: FileStatus): Seq[FileStatus] = {
    def recurse(status: FileStatus): Seq[FileStatus] = {
      val (directories, leaves) = fs.listStatus(status.getPath).partition(_.isDirectory)
      leaves ++ directories.flatMap(f => listLeafStatuses(fs, f))
    }

    if (baseStatus.isDirectory) recurse(baseStatus) else Seq(baseStatus)
  }


  type DirectoryPath = String
  type FilenamePath = String

  def createDirectory(path: DirectoryPath): Either[Boolean, Boolean] = {
    ENV match {
      case "local" =>
        new File(path).mkdir
        Right(true)
      case "hdfs" =>
        fs.mkdirs(new Path(path))
        Right(true)
    }
  }

  def deleteDirectory(path: DirectoryPath, recursive: Boolean = true): Either[Boolean, Boolean] = {
    ENV match {
      case "local" =>
        new File(path).delete
        Right(true)
      case _ =>
        fs.delete(new Path(path), recursive)
        Right(true)
    }
  }

  type SourceFilePath = String
  type DestinationFilePath = String

  def copyFile(source: SourceFilePath, dest: DestinationFilePath): Either[Boolean, Exception] = {
    ENV match {
      case "local" =>
        try {
          val sourceFile = new File(source)
          val destFile = new File(dest)

          new FileOutputStream(destFile).getChannel.transferFrom(
            new FileInputStream(sourceFile).getChannel, 0, Long.MaxValue)
          Left(true)
        } catch {
          case fnf: java.io.FileNotFoundException =>
            println(s"No ha encontrado el fichero $source ó $dest")
            Right(fnf)
        }
      case _ =>
        fs.copyFromLocalFile(new Path(source), new Path(dest))
        Left(true)
    }
  }


  // Listing files
  def getListOfFiles(dir: String): Either[List[File],Array[FileStatus]] = {
    if (isLocalEnv)
      Left(getFilesInLocalDirectory(dir))
    else
      Right(getFilesInHdfsDirectory(dir))
  }

  def getFilesInLocalDirectory(path: DirectoryPath): List[File] = {
    val d = new File(path)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
  def getFilesInHdfsDirectory(path: DirectoryPath): Array[FileStatus] = {
      fs.listStatus(new Path(path))
  }
}



