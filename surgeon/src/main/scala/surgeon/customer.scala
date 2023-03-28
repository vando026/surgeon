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
   *  @param names The names of the customer.
   *  @param cdat A dataset derived from `geoUtilCustomer`.
   *  @example{{{
   *  customerNameToId(List("c3.MLB")) // or
   *  customerNameToId(List("MLB"))
   *  }}}
   */
  def customerNameToId(names: List[String], cdat: DataFrame):
      Array[String] = {
    val snames = names.map(_.replace("c3.", ""))
    val out = cdat
      .select(col("customerId"))
      .where(col("customerName").isin(snames:_*))
      .collect().map(_(0).toString)
    out
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


  /** Construct Product Archive on Databricks for paths based on selection of Customer Ids. 
   @param path Path to the files with customer heartbeats or sessions. 
  */
  case class Cust(obj: DataPath)

  object Cust {

    def stitch(obj: DataPath, cnames: String) = 
      s"${obj.toPath}/cust={${cnames}}"

    /** Method to get data path using customer names.
     *  @param obj A DataPath object. 
     *  @param names The customer names with `c3.` prefix removed. 
     *  @example{{{
     *  Cust(Monthly(2023, 2), names = List("MLB", "CBSCom"))
     *  }}}
    */
    def apply(obj: DataPath, names: List[String], geopath: String = PathDB.geoUtil): String = {
      val cnames = customerNameToId(names, geoUtilCustomer(geopath = geopath))
      stitch(obj, cnames.mkString(","))
    }

    /** Method to get paths to data by customer IDs.
     *  @param obj A DataPath object. 
     *  @param ids List of customer Ids. 
     *  @example{{{
     *  Cust(Monthly(2023, 2), ids = List(1960180360))
     *  }}}
    */
    def apply(obj: DataPath, ids: List[Int]) = {
      stitch(obj, ids.mkString(","))
    }

    /** Method to get paths to data for the first n customer IDs.
     *  @param obj A DataPath object. 
     *  @param take The number of customer Ids to take. 
     *  @example{{{
     * Cust(Monthly(2023, 2), take = 10)
     *  }}}
    */
    def apply(obj: DataPath, take: Int) = {
      val cids = getCustomerIds(obj.toPath).take(take)
      stitch(obj, cids.map(_.toString).mkString(","))
    }

    /** Method to get path to data for all customers.
    * @example{{{
    * Cust(Monthly(2023, 2))
    * }}}
    */
    def apply(obj: DataPath) = stitch(obj, "*")
  }
}

