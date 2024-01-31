// Project: define paths for pbss/prbl paths
// Description: 
// Date: 27-Jan-2023
package conviva.surgeon

import conviva.surgeon.Customer._
import org.apache.spark.sql.DataFrame

/** Object with methods to create file paths for parquet datasets on `/mnt/conviva-prod-archive`. 
   * @define month A value from 1 to 12 representing the month of the year. 
   * @define year A value for the year. The default in this version is 2024. 
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
    /** Path to Geo_Utils folder on Databricks. */
    val geoUtil = "dbfs:/FileStore/Geo_Utils"
    /** Path to personally updated files */
    val fileStore = "dbfs:/FileStore/avandormael"
  }

  // utility methods
  def fmt(s: Int) = f"${s}%02d"
  def paste(x: List[Int]) = {
    val out = x.map(fmt(_)).mkString(",")
    if (x.length == 1) out else s"{$out}"
  }
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

  /** Trait with methods to format strings paths on the Databricks `/mnt`
   *  directory. */
  trait DataPath {
    val year: Int 
    val month: Int
    val root: String
    def toList: List[String]

    def checkDays(month: Int, days: List[Int]) {
      if (
        (month > 12 || month < 1) ||
        (days.exists(d => d > 31) || days.exists(d => d < 1)) ||
        (days.exists(d => d > 29) & month == 2))
          throw new Exception("Invalid day or month.")
    }

    def checkHours(hours: List[Int]) {
      if (hours.exists(h => h > 23) || hours.exists(h => h < 0))
        throw new Exception("Invalid hour of day.")
    }

    def stitch(
        root: String, year: Int, month: Int, day: List[Int],
        nyear: Int, nmonth: Int, nday: List[Int], sign: String = "d"):
      String = {
        List(root, s"y=${year}", f"m=${fmt(month)}", 
        f"dt=${sign}${year}_${fmt(month)}_${paste(day)}_08_00_to_${nyear}_${fmt(nmonth)}_${paste(nday)}_08_00")
        .mkString("/")
    }
  }

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

  case class Monthly(year: Int = 2024, month: Int, root: String = PathDB.monthly) 
      extends DataPath {
    val (nyear, nmonth) = if (month == 12) (year + 1, 1) else (year, month + 1)
    checkDays(month, List(1))
    override def toString = stitch(root, year, month, List(1), nyear, nmonth, List(1), sign = "c")
    override def toList = List(toString())
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
   *  Daily(2023, month = 2, day = List.range(1, 4)))
   *  }}}
   */ 
  case class Daily[A](month: Int, days: A, year: Int = 2024, root: String = PathDB.daily)
      extends DataPath {

    val days_ = mkIntList(days).sorted
    checkDays(month, days_)

    /* Method to check if day is last day of month. */
    private def lastDay(month: Int, days: List[Int]): Boolean = {
      val m31 = List(1, 3, 5, 7, 8, 10, 12)
      val m30 = List(4, 6, 9, 11)
      (m31.contains(month) & days.contains(31)) ||
      (m30.contains(month) & days.contains(30)) ||
      month == 2 & days.exists(d => List(28, 29).contains(d))
    }

    override def toString = {
      val (nyear, nmonth, ndays) = month match {
        case m if (lastDay(m, days_) & days_.length > 1) => 
          throw new Exception("List[Int] cannot include last day of month, use .toList instead.")
        case m if (m != 12 & lastDay(m, days_) & days_.length == 1) => (year, month + 1, List(1))
        case m if (m == 12 & lastDay(m, days_) & days_.length == 1) => (year + 1, 1, List(1))
        case _ => (year, month, days_.map(_ + 1))
      }
      stitch(root, year, month, days_, nyear, nmonth, ndays)
    }

    override def toList = {
      def singlePath(day: Int): String = { 
        val (nyear, nmonth, nday) = day match {
          case d if (lastDay(month, List(d))) => (year, month + 1, 1)
          case d if (month == 12 & lastDay(month, List(d))) => (year + 1, 1, 1)
          case _ => (year, month, day + 1)
        }
        stitch(root, year, month, List(day), nyear, nmonth, List(nday))
      }
      for (d <- days_) yield singlePath(d)
    }

  }

  /** Trait for defining Hourly Paths to data. */ 
  trait HourlyPath[A] extends DataPath {
    val days: A
    val hours: A

     def stitch(days: List[Int], hours: List[Int]): String = {
      List(root, s"y=${year}", f"m=${fmt(month)}", f"d=${paste(days)}",
        f"dt=${year}_${fmt(month)}_${paste(days)}_${paste(hours)}")
        .mkString("/")
    }
    val days_ = mkIntList(days)
    val hours_ = mkIntList(hours)

    checkDays(month, days_)
    checkHours(hours_)

    override def toString(): String = stitch(days_, hours_)
    override def toList(): List[String] = for (h <- hours_; d <- days_) yield stitch(List(d), List(h))
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
   *  @param lt The log version
   *  @return 
   *  @example {{{
   *  Hourly(year = 2023, month = 1, day = 12, hours = List(2, 3)).toString
   *  Hourly(year = 2023, month = 1, day = 12, hours = 2).toString
   *  }}}
   */ 
  case class Hourly[A](val month: Int, val days: A, val hours: A, val year: Int = 2024, root: String = PathDB.hourly()) 
      extends HourlyPath[A] {
  }

  case class HourlyRaw[A](val month: Int, val days: A, val hours: A, val year: Int = 2024, lt: Int = 1)
      extends HourlyPath[A] {
    override val root: String = PathDB.rawlog(lt)
  }

  /** Construct Product Archive on Databricks for paths based on selection of Customer Ids. 
   @param path Path to the files with customer heartbeats or sessions. 
  */
  case class Cust()

  object Cust {

    private def stitch(obj: DataPath, cnames: String) = 
      s"${obj.toString}/cust={${cnames}}"

    /** Method to get data path using customer names.
     *  @param obj A DataPath object. 
     *  @param names Customer name as String or names as List[String], with `c3.` prefix removed. 
     *  @example{{{
     *  Cust(Monthly(2023, 2), names = List("MLB", "CBSCom"))
     *  Cust(Monthly(2023, 2), names = "MLB")
     *  }}}
    */
    def apply[A](obj: DataPath, names: A, cmap: Map[Int, String] = customerNames()):
        String = {
      val cnames = customerNameToId(mkStrList(names), cmap)
      stitch(obj, cnames.mkString(","))
    }

    /** Method to get paths to data by customer IDs.
     *  @param obj A DataPath object. 
     *  @param ids customer Id as Int or customer Ids as List[Int]
     *  @example{{{
     *  Cust(Monthly(2023, 2), ids = List(1960180360, 1960183601))
     *  Cust(Monthly(2023, 2), ids = 1960180360)
     *  }}}
    */
    def apply[A](obj: DataPath, ids: A) = {
      stitch(obj, mkIntList(ids).mkString(","))
    }

    /** Method to get paths to data for the first n customer IDs.
     *  @param obj A DataPath object. 
     *  @param take The number of customer Ids to take. 
     *  @example{{{
     * Cust(Monthly(2023, 2), take = 10)
     *  }}}
    */
    def apply(obj: DataPath, take: Int) = {
      val cids = customerIds(obj.toList).take(take)
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
