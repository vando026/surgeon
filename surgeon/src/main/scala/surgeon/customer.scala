package conviva.surgeon

import conviva.surgeon.Paths._
import conviva.surgeon.GeoInfo._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{when, col, regexp_replace}
import org.apache.hadoop.fs._
// needed?
import org.apache.hadoop.conf._ 

object Customer {

  // Set Int to List[Int] for generic methods
  def mkIntList[A](i: A): List[Int] = {
    val out =  i match {
      case (i: Int) => List(i)
      case (i: List[Int]) => i
      case (i: Array[Int]) => i.toList
      case _ => throw new Exception("Must be either Int, Array[Int], List[Int]")
    }
    out
  }
  def mkStrList[A](i: A): List[String] = {
    val out =  i match {
      case (i: String) => List(i)
      case (i: List[String]) => i
      case (i: Array[String]) => i.toList
      case _ => throw new Exception("Must be either String, Array[String], List[String]")
    }
    out
  }

  /** Read the `/FileStore/Geo_Utils/c3ServiceConfig*.cvs` from Databricks, which
   *  contains customer names and ids. */
  def c3IdMap(): Map[Int, String] = getGeoData("customer")

  /** Get the ID of the customer name. 
   *  @param ids The ids of the customer.
   *  @param customerMap A map derived from `c3IdMap`.
   *  @example{{{
   *  c3IdToName(List(196900922, 196300090), c3IdMap = c3IdMap()) 
   *  c3IdToName(List(196900922, 196300090)) 
   *  c3IdToName(196300090)
   *  }}}
   */
  def c3IdToName[A](ids: A): List[String] = {
    mkIntList(ids).map(getGeoData("customer").getOrElse(_, "Key_missing"))
  }
  
  /** Get the ID of the customer name. 
   *  @param names The names of the customer.
   *  @param customerMap A map derived from `c3IdMap`.
   *  @example{{{
   *  c3NameToId(List("c3.MLB", "c3.CBNS"))
   *  c3NameToId("c3.MLB")
   *  }}}
   */
  def c3NameToId[A](names: A): List[Int] = {
    mkStrList(names).map(i => getGeoData("customer").filter(_._2 == i)).map(_.keys).flatten
  }

  /** Get the customer IDs associated with a file path on Databricks. 
   *  @param path The path to the GCS files on Databricks.
   *  @example{{{
   *  val path = Daily(year = 2023, month = 1, day = 20)
   *  c3IdOnPath(path)
   *  c3IdOnPath(path.toString)
   *  val paths = Hourly(month = 6, day = 2, hour = List(2, 3))
   *  c3IdOnPath(paths.toList)
   *  }}}
  */
  case class c3IdOnPath() {
    def get(path: String): List[Int] = {
      val ss = SparkSession.builder.master("local[*]")
        .getOrCreate.sparkContext.hadoopConfiguration
      val dbfs = FileSystem.get(ss)
      val paths = dbfs.listStatus(new Path(s"${path}"))
        .map(_.getPath.toString)
        .filter(_.contains("cust"))
      val pattern = "^.*/cust=([0-9]+)$".r
      val out = paths.map(f => { val pattern(h) = f; h })
      out.map(_.toInt).toList.filter(_ != 0).sorted // drop cust=0
    }
  }
  object c3IdOnPath {
    def apply(path: String): List[Int] = {
      c3IdOnPath().get(path)
    }
    def apply(paths: List[String]): List[Int] = {
      paths.map(c3IdOnPath().get(_)).flatten.toSet.toList
    }
    def apply(path: SurgeonPath): List[Int] = {
      c3IdOnPath().get(path.toList(0))
    }
  }


  /* Get customer Ids that are in both paths. 
   * @param path1 The first path
   * @param path2 The second path
   * @example{{{
   * c3IdOnBothPaths(
   *   HourlyRaw(2023, 5, 20, 10).toString, 
   *   Hourly(2023, 5, 20, 10).toString
   * )
   * // without toString method
   * c3IdOnBothPaths(
   *   HourlyRaw(2023, 5, 20, 10), 
   *   Hourly(2023, 5, 20, 10)
   * )
   * }}}
  */
 case class c3IdOnBothPaths() {
   def get(path1: String, path2: String): List[Int] = {
    c3IdOnPath(path1).intersect(c3IdOnPath(path2))
   }
  }
  object c3IdOnBothPaths {
    def apply(path1: SurgeonPath, path2: SurgeonPath): List[Int] = {
      c3IdOnBothPaths().get(path1.toList(0), path2.toList(0))
    }
    def apply(path1: String, path2: String): List[Int] = {
      c3IdOnBothPaths().get(path1, path2)
    }
  }

}
