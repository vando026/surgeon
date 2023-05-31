package conviva.surgeon

import conviva.surgeon.Paths._
import conviva.surgeon.Heart._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{when, col, regexp_replace}
import org.apache.hadoop.fs._

object Customer {

  /** Read customer data from GeoUtils. */
  def customerNames(
      path: String = PathDB.geoUtil
    ): DataFrame = {
    val out = SparkSession.builder.getOrCreate
        .read
        .option("delimiter", "|")
        .option("inferSchema", "true")
        .csv(path)
        .toDF("customerId", "customerName")
        .withColumn("customerName",
            regexp_replace(col("customerName"), "c3.", ""))
     out
  }

  /** Get the ID of the customer name. 
   *  @param ids The ids of the customer.
   *  @param cdat A dataset derived from `customerNames`.
   *  @example{{{
   *  customerIdToName(List(196900922, 196300090)) 
   *  customerIdToName(196300090)
   *  }}}
   */
  def customerIdToName[A](ids: A,
      cdat: DataFrame = customerNames()): Array[String] = {
    cdat.where(col("customerId").isin(mkIntList(ids): _*))
      .select(col("customerName"))
      .collect().map(_.getString(0))
  }
  
  /** Get the ID of the customer name. 
   *  @param names The names of the customer.
   *  @param cdat A dataset derived from `customerNames`.
   *  @example{{{
   *  customerNameToId(List("MLB", "CBNS"))
   *  customerNameToId(List("c3.MLB", "c3.CBNS"))
   *  customerNameToId("MLB")
   *  }}}
   */
  def customerNameToId[A](names: A, 
      cdat: DataFrame = customerNames()): Array[Int] = {
    val snames = mkStrList(names).map(_.replace("c3.", ""))
    cdat.select(col("customerId"))
      .where(col("customerName").isin(snames:_*))
      .collect().map(_(0).toString.toInt)
  }

  /** Get the customer IDs associated with a file path on Databricks. 
   *  @param path The path to the GCS files on Databricks.
   *  @example{{{
   *  val path = Monthly(year = 2023, month = 1).toPath 
   *  customerIds(path)
   *  }}}
  */
  def customerIds(path: String): Array[Int] = {
    val ss = SparkSession.builder
      .getOrCreate.sparkContext.hadoopConfiguration
    val dbfs = FileSystem.get(ss)
    val paths = dbfs.listStatus(new Path(path))
      .map(_.getPath.toString)
      .filter(!_.contains("_SUCCESS"))
      .sorted.drop(1) // drop1 drops cust=0 after sort
    val pattern = "dbfs.*/cust=([0-9]+)$".r
    val out = paths.map(f => { val pattern(h) = f; h })
    out.map(_.toInt)
  }

  /* Get customer Ids that are in both paths. 
   * @param path1 The first path
   * @param path2 The second path
   * @example{{{
   * customersInBothPaths(
   *   HourlyRaw(2023, 5, 20, 10).toString, 
   *   Hourly(2023, 5, 20, 10).toString
   * )
   * }}}
  */

  def customersInBothPaths(path1: String, path2: String): Array[Int] = {
    val cid1 = customerIds(path1)
    val cid2 = customerIds(path2)
      cid1.intersect(cid2)
  }

}

