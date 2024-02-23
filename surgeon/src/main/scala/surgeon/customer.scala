package conviva.surgeon

import conviva.surgeon.Paths._
import conviva.surgeon.GeoInfo._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{when, col, regexp_replace}
import org.apache.hadoop.fs._

object Customer {

  /** Read the `/FileStore/Geo_Utils/c3ServiceConfig*.xml` from Databricks, which
   *  contains customer names and ids. */
  def customerNames(): Map[Int, String] = getGeoData("customer")

  /** Get the ID of the customer name. 
   *  @param ids The ids of the customer.
   *  @param cmap A map derived from `customerNames`.
   *  @example{{{
   *  customerIdToName(List(196900922, 196300090), cmap = customerNames()) 
   *  customerIdToName(List(196900922, 196300090)) 
   *  customerIdToName(196300090)
   *  }}}
   */
  def customerIdToName[A](ids: A, cmap: Map[Int, String] = customerNames()): List[String] = {
    mkIntList(ids).map(cmap.getOrElse(_, "Key_missing"))
  }
  
  /** Get the ID of the customer name. 
   *  @param names The names of the customer.
   *  @param cmap A map derived from `customerNames`.
   *  @example{{{
   *  customerNameToId(List("c3.MLB", "c3.CBNS"))
   *  customerNameToId("c3.MLB")
   *  }}}
   */
  def customerNameToId[A](names: A, cmap: Map[Int, String] = customerNames()): List[Int] = {
    mkStrList(names).map(i => cmap.filter(_._2 == i)).map(_.keys).flatten
  }

  /** Get the customer IDs associated with a file path on Databricks. 
   *  @param path The path to the GCS files on Databricks.
   *  @example{{{
   *  val path = Daily(year = 2023, month = 1, day = 20)
   *  customerIds(path)
   *  customerIds(path.toString)
   *  val paths = Hourly(month = 6, day = 2, hours = List(2, 3))
   *  customerIds(paths.toList)
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
    def apply(path: DataPath): List[Int] = {
      customerIds().get(path.toString)
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
 case class customersInBothPaths() {
   def get(path1: String, path2: String): List[Int] = {
    customerIds(path1).intersect(customerIds(path2))
   }
  }
  object customersInBothPaths {
    def apply(path1: DataPath, path2: DataPath): List[Int] = {
      customersInBothPaths().get(path1.toString, path2.toString)
    }
    def apply(path1: String, path2: String): List[Int] = {
      customersInBothPaths().get(path1, path2)
    }
  }

}
