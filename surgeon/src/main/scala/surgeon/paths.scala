// Project: define paths for pbss/prbl paths
// Description: 
// Date: 27-Jan-2023
package conviva.surgeon

import conviva.surgeon.Customer._
import org.apache.spark.sql.DataFrame

/** Object with methods to create file paths for parquet datasets on `/mnt/conviva-prod-archive`. 
   * @define month A value from 1 to 12 representing the month of the year. 
   * @define year A value for the year. The default in this version is 2023. 
   * Use this parameter to change to a previous year. 
   * @define day A value from 1 to 31 representing the day of the month.
   * @define hour A value from 0 to 23 representing an hour of the day. Can be a list of strings or integers, an integer, or a string.
   * @define root The root of the path, see `PathDB`. Defaults to the `/mnt/conviva-prod-archive/...` root.
*/

object Paths {
   
  /** Database of common paths to the parquet files on Databricks. 
   *
  */
  object PathDB {
    /** Root path to `databricks-user-share`. */
    val dbUserShare = "/mnt/databricks-user-share"
    /** Root path to `conviva-prod-archive`. */
    val prodArchive    = "/mnt/conviva-prod-archive-"
    /** Path to the daily session summary parquet files. */
    val daily   = prodArchive + "pbss-daily/pbss/daily"
    /** Path to the hourly session summary parquet files. */
    def hourly(st: Int = 0)  = prodArchive + s"pbss-hourly/pbss/hourly/st=$st"
    /** Path to the monthly session summary parquet files. */
    val monthly = prodArchive + "pbss-monthly/pbss/monthly"
    /** Path to the parquet heartbeat (raw log) files. */
    def rawlog(lt: Int = 1) = prodArchive + s"pbrl/3d/rawlogs/pbrl/lt_$lt"
    /** Path to TLB 1-hour analytics. */
    def tlb1hour(snap: String, st: Int = 0): String = {
      s""" 
       | $dbUserShare/tlb/analytics-1hr/
       | parquet/tlb_offlineJob-assesmbly_$snap/
       | pbss/hourly/st=${st}/
      """.stripMargin
    }
    /** Path to Geo_Utils folder on Databricks. */
    val geoUtil = "dbfs:/FileStore/Geo_Utils/cust_dat.txt"
  }

  def fmt(s: Int) = f"${s}%02d"
  def toString_(x: List[Int]) = {
    val out = x.map(fmt(_)).mkString(",")
    if (x.length == 1) out else s"{$out}"
  }

  /** Trait with methods to format strings paths on the Databricks `/mnt`
   *  directory. */
  trait DataPath

  /** Class with methods to construct paths to monthly PbSS parquet data on Databricks. 
   *  @param year $year
   *  @param month $month Only a single value for month is permitted. 
   *  @param root The root of the path, defaults to `PathDB.monthly()`.
   *  @return 
   *  @example {{{
   *  Monthly(year = 2023, month = 1)
   *  Monthly(2023, 1).toString 
   *  }}}
   */ 
  case class Monthly(year: Int = 2023, month: Int, root: String = PathDB.monthly) 
      extends DataPath {
    val (nyear, nmonth) = if (month == 12) (year + 1, 1) else (year, month + 1)
    override def toString = List(root, s"y=${year}", f"m=${fmt(month)}",
      f"dt=c${year}_${fmt(month)}_01_08_00_to_${nyear}_${fmt(nmonth)}_01_08_00")
        .mkString("/")
  }
  /** Class with methods to construct paths to daily PbSS parquet data on
   *  Databricks. If parameter names are omitted, then the order is month, day,
   *  year. The default year is the current year. Only single values for month
   *  and day are permitted. 
   *  @param month $month
   *  @param day $day
   *  @param year $year
   *  @param root The root of the path, defaults to `PathDB.daily`.
   *  @return 
   *  @example {{{
   *  Daily(year = 2023, month = 1, day = 12)
   *  Daily(month = 1, day = 12)
   *  Daily(1, 12).toString
   *  // Get customer data over 3 days
   *  List.range(1, 4).map(d => Daily(2023, month = 2, day = d).toString)
   *  }}}
   */ 
  case class Daily(month: Int, day: Int, year: Int = 2023, root: String = PathDB.daily)
      extends DataPath {
    val (nyear, nmonth, nday) = if (month == 12 & day == 31) 
      (year + 1, 1, 1) else (year, month, day + 1)
    override def toString = List(root, s"y=${year}", f"m=${fmt(month)}", 
      f"dt=d${year}_${fmt(month)}_${fmt(day)}_08_00_to_${nyear}_${fmt(nmonth)}_${fmt(nday)}_08_00")
        .mkString("/")
  }

  /** Class with methods to construct paths to hourly parquet data on
   *  Databricks. If parameter names are omitted, then the order is month, day,
   *  hour(s), year, path. The default year is the current year. 
   *  The default is the hourly Session Summary path. See `PathDB`.  Only the `hours`
   *  parameter can take a list of integers.
   *  @param month $month
   *  @param day $day
   *  @param hours $hour
   *  @param year $year
   *  @param root $root
   *  @return 
   *  @example {{{
   *  Hourly(year = 2023, month = 1, day = 12, hours = List(2, 3)).toString
   *  }}}
   */ 
  case class Hourly(month: Int, day: Int, hours: List[Int], year: Int = 2023, root: String = PathDB.hourly()) 
      extends DataPath {
    override def toString = List(root, s"y=${year}", f"m=${fmt(month)}", f"d=${fmt(day)}",
      f"dt=${year}_${fmt(month)}_${fmt(day)}_${toString_(hours)}")
        .mkString("/")
  }

  /** Construct Product Archive on Databricks for paths based on selection of Customer Ids. 
   @param path Path to the files with customer heartbeats or sessions. 
  */
  case class Cust(obj: DataPath)

  object Cust {

    private def stitch(obj: DataPath, cnames: String) = 
      s"${obj.toString}/cust={${cnames}}"

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
      val cids = getCustomerIds(obj.toString).take(take)
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

