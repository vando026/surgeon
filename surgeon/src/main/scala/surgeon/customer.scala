package conviva.surgeon

import conviva.surgeon.Paths._
import conviva.surgeon.Heart._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{when, col, regexp_replace}
import org.apache.hadoop.fs._

object Customer {

  /** Read customer data from GeoUtils. */
  def geoUtilCustomer(
      prefix: Boolean = false,
      geopath: String = PathDB.geoUtil
    ): DataFrame = {
    val out = SparkSession.builder.getOrCreate
        .read
        .option("delimiter", "|")
        .option("inferSchema", "true")
        .csv(geopath)
        .toDF("customerId", "customerName")
    if (prefix == false) { 
      out.withColumn("customerName",
            regexp_replace(col("customerName"), "c3.", ""))
    } else {
      out
    }
  }

  /** Get the ID of the customer name. 
   *  @param ids The ids of the customer.
   *  @param cdat A dataset derived from `geoUtilCustomer`.
   *  @example{{{
   *  customerIdToName(List("c3.MLB")) // or
   *  customerIdToName(List("MLB"))
   *  }}}
   */
  def customerIdToName(ids: List[Int], 
      cdat: DataFrame = geoUtilCustomer(false)): Array[String] = {
    cdat.where(col("customerId").isin(ids: _*))
      .select(col("customerName"))
      .collect().map(_.getString(0))
  }
  
  /** Get the ID of the customer name. 
   *  @param names The names of the customer.
   *  @param cdat A dataset derived from `geoUtilCustomer`.
   *  @example{{{
   *  customerNameToId(List("c3.MLB")) // or
   *  customerNameToId(List("MLB"))
   *  }}}
   */
  def customerNameToId(names: List[String], 
      cdat: DataFrame = geoUtilCustomer(false)): Array[String] = {
    val snames = names.map(_.replace("c3.", ""))
    cdat
      .select(col("customerId"))
      .where(col("customerName").isin(snames:_*))
      .collect().map(_(0).toString)
  }

  /** Get the customer IDs associated with a file path on Databricks. 
   *  @param path The path to the GCS files on Databricks.
   *  @example{{{
   *  val path = Monthly(year = 2023, month = 1).toPath 
   *  getCustomerIds(path)
   *  }}}
  */
  def getCustomerIds(path: String): Array[String] = {
    val ss = SparkSession.builder
      .getOrCreate.sparkContext.hadoopConfiguration
    val dbfs = FileSystem.get(ss)
    val paths = dbfs.listStatus(new Path(path))
      .map(_.getPath.toString)
      .filter(!_.contains("_SUCCESS"))
      .sorted.drop(1) // drop1 drops cust=0 after sort
    val pattern = "dbfs.*/cust=([0-9]+)$".r
    paths.map(f => { val pattern(h) = f; h })
  }


}

