package conviva.surgeon

import conviva.surgeon.Paths._
import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions.{when, col, regexp_replace}
import org.apache.hadoop.fs._

object Customer {

  val sparkDonor = SparkSession
      .builder()
      .master("local")
      .appName("spark wrapper")
      .getOrCreate()

  /** Read customer data from GeoUtils. 
   *  @param prefix Remove the `c3.` prefix from customer name.
  */
  def geoUtilCustomer(prefix: Boolean = true, 
      geopath: String = s"${GeoUtils.root}/cust_dat.txt"):
    DataFrame = {
    val dat = sparkDonor.read
      .option("delimiter", "|")
      .option("inferSchema", "true")
      .csv(geopath)
      .toDF("customerId", "customerName")
      if (!prefix)
        dat.withColumn("customerName", 
          regexp_replace(col("customerName"), "c3.", ""))
      else dat
  }
  /** Get the customer IDs associated with a file path on Databricks Prod Archive folder. 
   *  @param path The path to the GCS files on Databricks.
   *  @example{{{
   *  getCustomerId(pbssMonthly(year = 2023, month = 1, day = 2, cid = None))
   *  }}}
  */
  def getCustomerId(path: String): Array[String] = {
    val dbfs = FileSystem.get(sparkDonor.sparkContext.hadoopConfiguration)
    val paths = dbfs.listStatus(new Path(path))
      .map(_.getPath.toString)
      .filter(!_.contains("_SUCCESS"))
      .sorted.drop(1) // drop1 drops cust=0 after sort
    val pattern = "dbfs.*/cust=([0-9]+)$".r
    paths.map(f => { val pattern(h) = f; h })
  }

  /** Get the ID of the customer name. 
   *  @param name Name of the customer
   *  @example{{{
   *  getCustId(List("c3.MLB")) // or
   *  getCustId(List("MLB"))
   *  }}}
   */
  def customerNameToId(name: List[String]): Array[String] = {
    val names = name.map(_.replace("c3.", ""))
    val out = geoUtilCustomer(prefix = false)
      .select(col("customerId"))
      .where(col("customerName").isin(names:_*))
      .collect().map(_(0).toString)
    out
  }

  /** Construct Product Archive on Databricks for paths based on selection of Customer Ids. 
   @param path Path to the files with customer heartbeats or sessions. 
  */
  trait CustomerPath {
    def path: String
    def stitch(path: String, cnames: String) = 
      s"${path}/cust={${cnames}}"

    /** Method to get data by customer names.
     *  @example{{{
     * PbSSMonthly(2023, 2).custNames(List("MLB", "CBSCom"))
     *  }}}
    */
    def custNames(name: List[String]) = {
      stitch(path, customerNameToId(name).mkString(","))
    }
    /** Method to get data by customer name.
     * @example{{{
     * PbSSMonthly(2023, 2).custName("MLB")
     * }}}
    */
    def custName(name: String) = {
      stitch(path, customerNameToId(List(name)).mkString(","))
    }
    /** Method to get all customers.
     * @example{{{
     * PbSSMonthly(2023, 2).custAll
     * }}}
     *  */
    def custAll() = path 

    /** Method to get data by customer ID.
     *  @example{{{
     * PbSSMonthly(2023, 2).custId(1960180360)
     *  }}}
    */
    def custId(id: Int) = {
      stitch(path, id.toString)
    }
    /** Method to get data by customer IDs.
     *  @example{{{
     * PbSSMonthly(2023, 2)).custIds(List(1960180360, 1960180492))
     *  }}}
    */
    def custIds(ids: List[Int]) = {
      stitch(path, ids.map(_.toString).mkString(","))
    }
    /** Method to get the first n customer IDs.
     *  @example{{{
     * PbSSMonthly(2023, 2).custTake(10)
     *  }}}
    */
    def custTake(n: Int) = {
      val cids = getCustomerId(path).take(n)
      stitch(path, cids.map(_.toString).mkString(","))
    }
  }
}


