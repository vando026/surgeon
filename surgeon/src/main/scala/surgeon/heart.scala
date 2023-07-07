package conviva.surgeon

import org.apache.spark.sql.SparkSession

object Heart {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate();

  }

  case class SetPaths(
    val dbUserShare: String = "/mnt/databricks-user-share",
    val prodArchive: String = "/mnt/conviva-prod-archive-",
    val daily: String = "/mnt/conviva-prod-archive-pbss-daily/pbss/daily",
    val geoUtil: String = "dbfs:/FileStore/Geo_Utils"
  ) 

  // val PathDB = SetPaths()

  // val localEnv = false
  // val geoCustPath = if (localEnv) "./src/test/data/cust_dat.txt" else
  //   "dbfs:/FileStore/Geo_Utils/cust_dat.txt"
  // val pbssTestPath = if (localEnv)  "./src/test/data" else
  //   "/mnt/conviva-dev-convivaid0/users/avandormael/surgeon/data"

}
