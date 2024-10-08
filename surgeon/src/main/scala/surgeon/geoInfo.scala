package org.surgeon

object GeoInfo {

  import org.surgeon.Paths._
  import org.apache.hadoop.fs._
  import org.apache.hadoop.conf._
  import scala.xml._
  import org.apache.spark.sql.{SparkSession, Row, Column}
  import org.apache.spark.sql.functions.{col, typedLit}
    
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
  case class GetGeoData(path: String) {
    def data(geoName: String): Map[Int, String] = {
      val fileName = getGeoTypes.getOrElse(geoName, "Unknown")
      val result = fileName match {
        // if there is no match, make an dummy map
        case "Unknown" => Map(9999 -> "Unknown")
        case _ => {
          val ss = SparkSession.builder.getOrCreate.sparkContext.hadoopConfiguration
          val xpath = new Path(s"${path}/${fileName}")
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
      result 
    }
  }

  class GeoCol(col: Column, field: String, labels: Map[Int, String]) extends Column(col.expr) {
    def label(): Column  = {
      val gLit: Column = typedLit(labels) 
      gLit(col).alias(s"${field}Label")
    }
  }

  /** Method for extracting fields from `payload.heartbeat.geoInfo`. */
  case class GeoBuilder(path: String) {
    def make(field: String): GeoCol = {
      val gcol = col(s"val.invariant.geoInfo.$field")
      val gMap = GetGeoData(path).data(field)
      new GeoCol(gcol, field, gMap)
    }
  }


  case class CustomerName(path: String) {
    def make(field: Column): Column = {
      val gMap = GetGeoData(path).data("customer")
      val gLit: Column = typedLit(gMap) 
      gLit(field).alias(s"customerName")
    }
  }



}
