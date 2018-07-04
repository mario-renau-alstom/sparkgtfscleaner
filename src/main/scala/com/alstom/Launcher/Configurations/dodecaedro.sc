

import java.io._
import java.util.zip.{ZipEntry, ZipInputStream}
 val sourcefileDir = "C:\\Development\\GitHub\\sparkgtfscleaner\\src\\test\\resources\\data\\gtfs\\Lyon\\backup\\GTFSCLEAN\\torrejon_cercanias_gtfs"
 val destinationdir = "C:\\Development\\GitHub\\sparkgtfscleaner\\src\\test\\resources\\data\\gtfs\\Lyon\\staging"
 val backupdir = new File(sourcefileDir)
 val allzip = backupdir.listFiles.filter(_.isFile).toList.filter { file => file.getName.endsWith("zip")}
allzip.foreach { x =>

    val buffer = new Array[Byte](1024)

    try {
      //zip file content
      val zis: ZipInputStream = new ZipInputStream(new FileInputStream(x));
      //get the zipped file list entry
      var ze: ZipEntry = zis.getNextEntry();

      while (ze != null) {

        val fileName = ze.getName();
        val newFile = new File(destinationdir + File.separator + fileName);

        System.out.println("file unzip : " + newFile.getAbsoluteFile());

        //create folders
        new File(newFile.getParent()).mkdirs();

        val fos = new FileOutputStream(newFile);

        var len: Int = zis.read(buffer);

        while (len > 0) {

          fos.write(buffer, 0, len)
          len = zis.read(buffer)
        }

        fos.close()
        ze = zis.getNextEntry()
      }

      zis.closeEntry()
      zis.close()

    } catch {
      case e: IOException => println("exception caught: " + e.getMessage)
    }

  }
