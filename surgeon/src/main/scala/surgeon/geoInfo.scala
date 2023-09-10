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
      ("resource") -> ("c3ServiceConfig_march_31_2023.xml")
  )

  // Read GEO file, convert it to Scala Map and load to geoMap
  def getGeoData(geoName: String, path: String = PathDB.geoUtil) = {
    val ss = SparkSession.builder.getOrCreate.sparkContext.hadoopConfiguration
    val dx = FileSystem.get(ss)
    val xpath = new Path(path + "/" + getGeoTypes(geoName))
    val fs = xpath.getFileSystem(new Configuration)
    val input = fs.open(xpath)
    val out = geoName match {
      case "resource" => (XML.load(input)  \\ "customers" \\ "customer")
        .map({ x => ((x.attributes("id")).toString.toInt -> (x.attributes("name")).toString.toString )}).toMap
      case _ => scala.io.Source.fromInputStream(input).getLines
          .filterNot(_.startsWith("#")) // skip 'comment' line
          .map(_.split("\\|").map(_.trim))
          .filter(_.length == 2) // what if "54|czech republic|r" - skip or take?? skip for now...
          .map{case Array(id, name) => id.toInt -> name}.toMap
    }
    fs.close()
    out
  }

}

