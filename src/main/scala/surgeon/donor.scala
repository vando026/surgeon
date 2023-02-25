package conviva.surgeon

import conviva.surgeon.Paths.GeoUtils
import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions.{when, col, regexp_replace}

object Donor {

  val spark = SparkSession.builder().getOrCreate()

  /** Read customer data from GeoUtils. 
   *  @param prefix Remove the `c3.` prefix from customer name.
  */
  def geoUtilCustomer(prefix: Boolean = true): DataFrame = {
    val dat = spark.read
      .option("delimiter", "|")
      .option("inferSchema", "true")
      .csv(GeoUtils.root + "/cust_dat.txt")
      .toDF("customerId", "customerName")
      if (!prefix)
        dat.withColumn("customerName", 
          regexp_replace(col("customerName"), "c3.", ""))
      else dat
  }

}
