package conviva.surgeon

object GeoInfo {

  import conviva.surgeon.Paths._
  import org.apache.hadoop.fs._
  import org.apache.hadoop.conf._
  import scala.xml._
  import org.apache.spark.sql.{SparkSession, Row}
    
  def getGeoTypes = Map(
      ("continent" -> "continents_dat.gp"), 
      ("country" -> "countries_dat.gp"), 
      ("countryIso" -> "iso_countries_dat.gp"), 
      ("region" -> "regions.dat"), 
      ("state" -> "states_dat.gp"), 
      ("city" -> "cities_dat.gp"),
      ("isp" -> "isp.dat"),
      ("dma" -> "us_dma.dat"), 
      ("asn" -> "asn.dat"), 
      ("asnIdToIspName" -> "asnIdToIspName.dat"), 
      ("connectionType" -> "connectionTypes_dat.gp"),
      ("customer") -> ("c3ServiceConfig_30Jan2024.csv")
  )

  // Read GEO file, convert it to Scala Map and load to geoMap
    def getGeoData(geoName: String, path: String = PathDB.geoUtil): Map[Int, String] = {
      val ss = SparkSession.builder.getOrCreate.sparkContext.hadoopConfiguration
      val xpath = new Path(path + "/" + getGeoTypes(geoName))
      val fs = xpath.getFileSystem(new Configuration)
      val dat = scala.io.Source.fromInputStream(fs.open(xpath)).getLines
      val out = geoName match {
        case "customer" => dat.map(_.stripPrefix("\uFEFF")) // Remove special character 
            .map(_.split("\\,").map(_.trim))
        case _ => dat.filterNot(_.startsWith("#")) // skip 'comment' line
            .map(_.split("\\|").map(_.trim))
      }
      fs.close
      out.filter(_.length == 2) // what if "54|czech republic|r" - skip or take?? skip for now...
        .map{case Array(id, name) => id.toInt -> name}.toMap
    }



}
