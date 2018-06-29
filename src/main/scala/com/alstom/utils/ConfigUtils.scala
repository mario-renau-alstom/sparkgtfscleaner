package com.alstom.utils

import java.io.File
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions

import com.typesafe.config.ConfigFactory
import org.json4s.{DefaultFormats, Formats}

import scala.io.Source

trait ConfigUtils {

  implicit lazy val formats: Formats = DefaultFormats
  def getConfig(path: String): com.typesafe.config.Config = {
    val file = if (path.startsWith("http://")) {
      val src = Source.fromURL(path)
      val downloadFile = Files.createTempFile("config", "config",
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
      downloadFile.toFile.deleteOnExit() // just to be sure
      Files.write(downloadFile, src.mkString("").getBytes).toFile
    }
    else new File(path)
    val currentDirectory = new java.io.File(".").getCanonicalPath
    println(currentDirectory)
    ConfigFactory.parseFile(file).resolve()
  }
}

// Companion object, for holding constants (as per the time being)
object ConfigUtils {

  // Application Configuration
  val ApplicationEnv = "application.env"
  val SparkAppName = "spark.app.name"
  val DEV_ENV = "dev"
  val PROD_ENV = "prod"

  // Spark Configuration
  val SparkCheckpointEnabled = "spark.checkpoint.enabled"
  val SparkCheckpointDir = "spark.checkpoint.dir"

  // Root directories
  val DirectoriesOnlineRoot = "directories.online.root"
  val DirectoriesOnlineBackup = "directories.online.backup"
  val DirectoriesOnlineStaging = "directories.online.staging"
  val DirectoriesOnlineSource = "directories.online.source"
  val DirectoriesWorkPath = "directories.online.work"
  val DirectoriesOnlineRaw = "directories.online.raw"

  // Paris
  val ParisGjsonPath01File = "paris.gjson.paths.01.file"
  val ParisGjsonPath01Url ="paris.gjson.paths.01.url"

  // Lyon
  val LyonFeedUrl = "lyon.feedUrl"
  val LyionGjsonPath01File = "lyon.gjson.paths.01.file"
  val LyionGjsonPath01Url ="lyon.gjson.paths.01.url"
  val LyionGjsonPath02File = "lyon.gjson.paths.02.file"
  val LyionGjsonPath02Url ="lyon.gjson.paths.02.url"
  val LyionGjsonPath03File = "lyon.gjson.paths.03.file"
  val LyionGjsonPath03Url ="lyon.gjson.paths.03.url"
  val LyionGjsonPath04File = "lyon.gjson.paths.04.file"
  val LyionGjsonPath04Url ="lyon.gjson.paths.04.url"
}

