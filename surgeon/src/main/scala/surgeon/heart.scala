package conviva.surgeon

import conviva.surgeon.Paths._
import conviva.surgeon.Customer._

object Surgeon {
  def main(args: Array[String]) = {
    println("Loading the Surgeon library...")
  }
}

object Heart {

  /** Path to the `Geo_Utils` folder on Databricks. */
  trait GeoUtilsPaths {
    /** The root path. */
    val geopath: String = "dbfs:/FileStore/Geo_Utils"
  }

}
