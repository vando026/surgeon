package conviva.surgeon

import conviva.surgeon.Paths._
import conviva.surgeon.GeoInfo._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{when, col, regexp_replace}
import org.apache.hadoop.fs._
// needed?
import org.apache.hadoop.conf._ 

object Customer {

  /** Read the `/FileStore/Geo_Utils/c3ServiceConfig*.cvs` from Databricks, which
   *  contains customer names and ids. */

  case class C3(paths: PathDB) {
    def idMap(): Map[Int, String] = GetGeoData(paths.geoUtilPath).data("customer")

    /** Get the ID of the customer name. 
     *  @param ids The ids of the customer.
     *  @param customerMap A map derived from `c3IdMap`.
     *  @example{{{
     *  c3IdToName(196300090) 
     *  c3IdToName(196900922, 196300090) 
     *  c3IdToName(196900922, 196300090, c3IdMap = c3IdMap()) 
     *  }}}
     */
    def idToName(ids: Int*): Seq[String] = { 
      ids.map(i => GetGeoData(paths.geoUtilPath).data("customer").getOrElse(i, "Id_missing"))
    }

    /** Get the ID of the customer name. 
     *  @param names The names of the customer.
     *  @param customerMap A map derived from `c3IdMap`.
     *  @example{{{
     *  c3NameToId("c3.MLB")
     *  c3NameToId("c3.MLB", "c3.CBNS")
     *  }}}
    */
    def nameToId(names: String*): Seq[Int] = {
      names.map(i => GetGeoData(paths.geoUtilPath).data("customer")
        .filter(_._2 == i)).map(_.keys).flatten
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
     def idOnPath(path: String): List[Int] = {
        val ss = SparkSession.builder.master("local[*]")
          .getOrCreate.sparkContext.hadoopConfiguration
        val dbfs = FileSystem.get(ss)
        val xpaths = dbfs.listStatus(new Path(s"${path}"))
          .map(_.getPath.toString)
          .filter(_.contains("cust"))
        val pattern = "^.*/cust=([0-9]+)$".r
        val out = xpaths.map(f => { val pattern(h) = f; h })
        out.map(_.toInt).toList.filter(_ != 0).sorted.toSet.toList
     }
  }

}
