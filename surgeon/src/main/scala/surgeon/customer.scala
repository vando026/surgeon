package conviva.surgeon

import conviva.surgeon.Paths._
import conviva.surgeon.Heart._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{when, col, regexp_replace}
import org.apache.hadoop.fs._

object Customer {

  /** Read customer data from a file.
   * @path Path to the customer file.
   * @delim The delimiter. Default is "|"
  */
  def customerNames(
      path: String = PathDB.geoUtil,
      delim: String = "|"): 
    DataFrame = {
    val out = SparkSession.builder.getOrCreate
        .read
        .option("delimiter", delim)
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
   *  customerIdToName(List(196900922, 196300090), cdat = customerNames()) 
   *  customerIdToName(List(196900922, 196300090)) 
   *  customerIdToName(196300090)
   *  }}}
   */
  def customerIdToName[A](ids: A, cdat: DataFrame = customerNames()): 
      List[String] = {
    cdat.where(col("customerId").isin(mkIntList(ids): _*))
      .select(col("customerName"))
      .collect().map(_.getString(0)).toList
  }
  
  /** Get the ID of the customer name. 
   *  @param names The names of the customer.
   *  @param cdat A dataset derived from `customerNames`.
   *  @example{{{
   *  customerNameToId(List("MLB", "CBNS"), cdat = customerNames())
   *  customerNameToId(List("c3.MLB", "c3.CBNS"))
   *  customerNameToId("MLB")
   *  }}}
   */
  def customerNameToId[A](names: A, cdat: DataFrame = customerNames()): 
      List[Int] = {
    val snames = mkStrList(names).map(_.replace("c3.", ""))
    cdat.select(col("customerId"))
      .where(col("customerName").isin(snames:_*))
      .collect().map(_(0).toString.toInt).toList
  }

  /** Get the customer IDs associated with a file path on Databricks. 
   *  @param path The path to the GCS files on Databricks.
   *  @example{{{
   *  val path = Daily(year = 2023, month = 1, day = 20)
   *  customerIds(path)
   *  val paths = Hourly(month = 6, days = 2, hours = List(2, 3)).toList
   *  customerIds(paths)
   *  }}}
  */
  case class customerIds() {
    def get(path: String): List[Int] = {
      val ss = SparkSession.builder
        .getOrCreate.sparkContext.hadoopConfiguration
      val dbfs = FileSystem.get(ss)
      val paths = dbfs.listStatus(new Path(path))
        .map(_.getPath.toString)
        .filter(!_.contains("_SUCCESS"))
        .sorted.drop(1) // drop1 drops cust=0 after sort
      val pattern = "dbfs.*/cust=([0-9]+)$".r
      val out = paths.map(f => { val pattern(h) = f; h })
      out.map(_.toInt).toList
    }
  }
  object customerIds {
    def apply(path: String): List[Int] = {
      customerIds().get(path)
    }
    def apply(paths: List[String]): List[Int] = {
      paths.map(customerIds().get(_)).flatten.toSet.toList
    }
  }


  /* Get customer Ids that are in both paths. 
   * @param path1 The first path
   * @param path2 The second path
   * @example{{{
   * customersInBothPaths(
   *   HourlyRaw(2023, 5, 20, 10).toString, 
   *   Hourly(2023, 5, 20, 10).toString
   * )
   * // without toString method
   * customersInBothPaths(
   *   HourlyRaw(2023, 5, 20, 10), 
   *   Hourly(2023, 5, 20, 10)
   * )
   * }}}
  */
 case class customersInBothPaths(path1: String, path2: String) {
    customerIds(path1).intersect(customerIds(path2))
  }
  object customersInBothPaths {
    def apply(path1: DataPath, path2: DataPath): List[Int] = {
      customersInBothPaths(path1.toString, path2.toString)
    }
    def apply(path1: String, path2: String): List[Int] = {
      customersInBothPaths(path1, path2)
    }
  }

}

