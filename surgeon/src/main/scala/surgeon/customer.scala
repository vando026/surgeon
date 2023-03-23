package conviva.surgeon

import conviva.surgeon.Paths._
import conviva.surgeon.Heart._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{when, col, regexp_replace}
import org.apache.hadoop.fs._

object Customer {

  /** Read customer data from GeoUtils. */
  def geoUtilCustomer(prefix: Boolean = false): DataFrame = {
    val out = SparkSession.builder.getOrCreate
        .read
        .option("delimiter", "|")
        .option("inferSchema", "true")
        .csv(geoCustPath)
        .toDF("customerId", "customerName")
    if (prefix == false) { 
      out.withColumn("customerName",
            regexp_replace(col("customerName"), "c3.", ""))
    } else {
      out
    }
  }
  
    /** Get the ID of the customer name. 
     *  @param name Name of the customer
     *  @example{{{
     *  getCustId(List("c3.MLB")) // or
     *  getCustId(List("MLB"))
     *  }}}
     */
    def customerNameToId(name: List[String], cdat: DataFrame):
        Array[String] = {
      val names = name.map(_.replace("c3.", ""))
      val out = cdat
        .select(col("customerId"))
        .where(col("customerName").isin(names:_*))
        .collect().map(_(0).toString)
      out
    }

  /** Get the customer IDs associated with a file path on Databricks Prod Archive folder. 
   *  @param path The path to the GCS files on Databricks.
   *  @example{{{
   *  getCustomerId(pbssMonthly(year = 2023, month = 1, day = 2, cid = None))
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
  trait CustExtract {
    val customerData = geoUtilCustomer(prefix = false)
    def path: String
    def stitch(path: String, cnames: String) = 
      s"${path}/cust={${cnames}}"

    /** Method to get data by customer names.
     *  @example{{{
     * Monthly(2023, 2).custNames(List("MLB", "CBSCom"))
     *  }}}
    */
    def custNames(name: List[String]) = {
      stitch(path, customerNameToId(name, customerData).mkString(","))
    }
    /** Method to get data by customer name.
     * @example{{{
     * Monthly(2023, 2).custName("MLB")
     * }}}
    */
    def custName(name: String) = {
      stitch(path, customerNameToId(List(name), customerData).mkString(","))
    }
    /** Method to get all customers.
     * @example{{{
     * Monthly(2023, 2).custAll
     * }}}
     *  */
    def custAll() = path 

    /** Method to get data by customer ID.
     *  @example{{{
     * Monthly(2023, 2).custId(1960180360)
     *  }}}
    */
    def custId(id: Int) = {
      stitch(path, id.toString)
    }
    /** Method to get data by customer IDs.
     *  @example{{{
     * Monthly(2023, 2)).custIds(List(1960180360, 1960180492))
     *  }}}
    */
    def custIds(ids: List[Int]) = {
      stitch(path, ids.map(_.toString).mkString(","))
    }
    /** Method to get the first n customer IDs.
     *  @example{{{
     * Monthly(2023, 2).custTake(10)
     *  }}}
    */
    def custTake(n: Int) = {
      val cids = getCustomerIds(path).take(n)
      stitch(path, cids.map(_.toString).mkString(","))
    }
  }

}

