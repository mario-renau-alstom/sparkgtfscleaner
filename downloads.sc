import java.io.{File, FileInputStream, FileOutputStream}
import java.net.URL
import scala.sys.process._
val region = "torrejon_cercanias_gtfs"
val downloadURL = "https://crtm.maps.arcgis.com/sharing/rest/content/items/1a25440bf66f499bae2657ec7fb40144/data"
//val downloadURL = "https://opendata.stif.info/explore/dataset/offre-horaires-tc-gtfs-idf/files/f24cf9dbf6f80c28b8edfdd99ea16aad/download/"


val t = new URL(downloadURL) #> new File("C:\\Development\\GitHub\\sparkgtfscleaner\\fileName\\" + region + ".zip") !!