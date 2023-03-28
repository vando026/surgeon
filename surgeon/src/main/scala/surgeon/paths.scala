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
  trait DataPath {
    def toPath: String
  }

  /** Class with methods to construct paths to monthly PbSS parquet data on Databricks. 
   *  @param year $year
   *  @param month $month Only a single value for month is permitted. 
   *  @param root The root of the path, see `PathDB`.
   *  @return 
   *  @example {{{
   *  Monthly(year = 2023, month = 1).toPath // all customers
   *  Monthly(2023, 1).custAll // defaults to current year, with all customers
   *  Monthly(2022,  12).custName("DSS") // Different year with only Disney customer
   *  Monthly(2022, 12).custNames(List("DSS", "CBSCom")) // Only Disney and CBS customers
   *  Monthly(2022, 12).custID(1960180360) // call by Id
   *  Monthly(2023, 3).custIDs(List(1960180360, 1960180492) // call by Ids
   *  Monthly(2023, 5).custTake(3) // take only the first n customers, in this case 3
   *  }}}
   */ 
  case class Monthly(year: Int = 2023, month: Int, root: String = PathDB.monthly) 
      extends DataPath {
    val (nyear, nmonth) = if (month == 12) (year + 1, 1) else (year, month + 1)
    override def toPath() = List(root, s"y=${year}", f"m=${fmt(month)}",
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
   *  @return 
   *  @example {{{
   *  Daily(year = 2023, month = 1, day = 12).toPath // all customers
   *  Daily(month = 1, day = 12).custAll // defaults to current year, with all customers
   *  Daily(1, 12).custName("DSS") // Different year with only Disney customer
   *  Daily(12, 28).custNames(List("DSS", "CBSCom")) // Only Disney and CBS customers
   *  Daily(2, 13).custID(1960180360) // call by Id
   *  Daily(year = 2022,  month = 3, day = 1).custIDs(List(1960180360, 1960180492) // call by Ids
   *  Daily(5, 27, 2023).custTake(3) // take only the first n customers, in
   *  this case 3
   *  // Get customer data over 3 days
   *  List.range(1, 4).map(d => Daily(year = 2023, month = 2, day = d).custName("DSS"))
   *  }}}
   */ 
  case class Daily(month: Int, day: Int, year: Int = 2023, root: String = PathDB.daily)
      extends DataPath {
    val (nyear, nmonth, nday) = if (month == 12 & day == 31) 
      (year + 1, 1, 1) else (year, month, day + 1)
    override def toPath() = List(root, s"y=${year}", f"m=${fmt(month)}", 
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
   *  Hourly(year = 2023, month = 1, day = 12, hours = List(2, 3)).path // all customers
   *  Hourly(year = 2023, month = 1, day = 12, hours = List(2, 3), root = PathDB.rawlog).path // change to rawlog data
   *  Hourly(month = 1, day = 12, hours = List(3, 5)).custAll // defaults to current year, with all customers
   *  Hourly(1, 12, List.range(1, 7)).custName("DSS") // Different year with only Disney customer
   *  Hourly(2, 13, List(2)).custTake(10)
   *  Hourly(year = 2022,  month = 3, day = 1, hours = List(4, 5)).custIDs(List(1960180360, 1960180492) // call by Ids
   *  }}}
   */ 
  case class Hourly(month: Int, day: Int, hours: List[Int], year: Int = 2023, root: String = PathDB.hourly()) 
      extends DataPath {
    override def toPath() = List(root, s"y=${year}", f"m=${fmt(month)}", f"d=${fmt(day)}",
      f"dt=${year}_${fmt(month)}_${fmt(day)}_${toString_(hours)}")
        .mkString("/")
  }

}

