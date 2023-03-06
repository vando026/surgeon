package conviva.surgeon

import conviva.surgeon.Paths._
import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions.{when, col, regexp_replace}
import org.apache.hadoop.fs._

object Donor {

  val sparkDonor = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()

  /** Read customer data from GeoUtils. 
   *  @param prefix Remove the `c3.` prefix from customer name.
  */
  case class GeoUtilCustomer(prefix: Boolean = true,
      path: String = s"${GeoUtils.root}/cust_dat.txt") {
    def data(): DataFrame = {
      val dat = sparkDonor.read
        .option("delimiter", "|")
        .option("inferSchema", "true")
        .csv(path)
        .toDF("customerId", "customerName")
        if (!prefix)
          dat.withColumn("customerName", 
            regexp_replace(col("customerName"), "c3.", ""))
        else dat
    }
  }
  
  // create instance of GeoUtilCustomer
  val geoUtilCustomer = GeoUtilCustomer().data

  /** Get the customer IDs associated with a file path on Databricks Prod Archive folder. 
   *  @param path The path to the GCS files on Databricks.
   *  @example{{{
   *  getCustomerId(pbssMonthly(year = 2023, month = 1, day = 2, cid = None))
   *  }}}
   */
  def getCustomerIds(path: String): Array[String] = {
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
    val out = geoUtilCustomer
      .select(col("customerId"))
      .where(col("customerName").isin(names:_*))
      .collect()
      .map(_(0).toString)
    out
  }

  /** Construct Product Archive on Databricks for paths based on selection of Customer Ids. 
   @param path Path to the files with customer heartbeats or sessions. 
  */
  trait Customer {
    def path: String
    def stitch(path: String, cnames: String) = 
      s"${path}/cust={${cnames}}"

    /** Method to get data by customer names.
     *  @example{{{
     * Customer(pbssMonthly(2)).names(List("MLB", "CBSCom"))
     *  }}}
    */
    def custNames(name: List[String]) = {
      stitch(path, customerNameToId(name).mkString(","))
    }
    /** Method to get data by customer name.
     * @example{{{
     * Customer(pbssMonthly(2)).name("MLB")
     * }}}
    */
    def custName(name: String) = {
      stitch(path, customerNameToId(List(name)).mkString(","))
    }
    /** Method to get all customers.
     * @example{{{
     * Customer(pbssMonthly(2)).all
     * }}}
     *  */
    def custAll() = path 
    /** Method to get data by customer ID.
     *  @example{{{
     * Customer(pbssMonthly(2)).id(1960180360)
     *  }}}
    */
    def custID(id: Int) = {
      stitch(path, id.toString)
    }
    /** Method to get data by customer IDs.
     *  @example{{{
     * Customer(pbssMonthly(2)).ids(List(1960180360, 1960180492))
     *  }}}
    */
    def custIDs(id: List[Int]) = {
      stitch(path, id.map(_.toString).mkString(","))
    }
    /** Method to get the first n customer IDs.
     *  @example{{{
     * Customer(pbssMonthly(2)).take(10)
     *  }}}
    */
    def custTake(n: Int) = {
      val cids = getCustomerIds(path).take(n)
      stitch(path, cids.map(_.toString).mkString(","))
    }
  }
}


