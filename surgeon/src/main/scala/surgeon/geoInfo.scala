package conviva.surgeon

object GeoInfo {

  import conviva.surgeon.Paths._
  import org.apache.hadoop.fs._
  import org.apache.hadoop.conf._
  import scala.xml._
  import org.apache.spark.sql.SparkSession
    
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
      ("resource") -> ("c3ServiceConfig_30Jan2024.csv")
  )

  // Read GEO file, convert it to Scala Map and load to geoMap
  def getGeoData(geoName: String, path: String = PathDB.geoUtil) = {
    val ss = SparkSession.builder.getOrCreate.sparkContext.hadoopConfiguration
    val dx = FileSystem.get(ss)
    val xpath = new Path(path + "/" + getGeoTypes(geoName))
    val fs = xpath.getFileSystem(new Configuration)
    val input = fs.open(xpath)
    val out = geoName match {
      case "resource" => scala.io.Source.fromInputStream(input).getLines
        .map(_.split(","))
        .filter(_.length == 2)
        .map {case Array(id, name) => id.toInt -> name}.toMap
      case _ => throw new Exception(s"${path}/${geoName} not found.")
    }
    fs.close()
    out
  }

}

