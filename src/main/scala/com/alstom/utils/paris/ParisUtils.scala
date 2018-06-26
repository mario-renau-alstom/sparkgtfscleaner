package com.alstom.utils.paris

import java.io.File

import com.alstom.GTFSOperations.IOOperations.getListOfFiles
import org.apache.hadoop.fs.FileStatus
import scala.collection.mutable.ArrayBuffer

object ParisUtils {

  def getGeoJsonPathList(outputPath: String): ArrayBuffer[String] = {
    val listOfFiles = getListOfFiles(outputPath)
    val geoJsonPathsList = new ArrayBuffer[String]

    if (listOfFiles.isLeft) {
      listOfFiles.left.get.foreach(localFile => geoJsonPathsList += localFile.getPath)
    }
    else {
      listOfFiles.right.get.foreach(hdfsFile => geoJsonPathsList += hdfsFile.getPath.toString)
    }
    geoJsonPathsList
  }
}
