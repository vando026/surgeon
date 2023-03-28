package conviva.surgeon

import org.apache.spark.sql.SparkSession
import conviva.surgeon.Customer._

object Heart {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate();

  }

  // val localEnv = false
  // val geoCustPath = if (localEnv) "./src/test/data/cust_dat.txt" else
  //   "dbfs:/FileStore/Geo_Utils/cust_dat.txt"
  // val pbssTestPath = if (localEnv)  "./src/test/data" else
  //   "/mnt/conviva-dev-convivaid0/users/avandormael/surgeon/data"

}
