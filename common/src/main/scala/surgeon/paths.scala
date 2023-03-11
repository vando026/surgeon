// Project: define paths for pbss/prbl paths
// Description: 
// Date: 27-Jan-2023
package conviva.surgeon
import conviva.surgeon.Donor._

/** Object with methods to create file paths for parquet datasets on `/mnt/conviva-prod-archive`. 
   * @define month A value from 1 to 12 representing the month of the year. 
   * @define year A value for the year. The default in this version is 2023. 
   * Use this parameter to change to a previous year. 
   * @define day A value from 1 to 31 representing the day of the month.
   * @define hour A value from 0 to 23 representing an hour of the day. Can be a list of strings or integers, an integer, or a string.
*/

object Paths {
   
  /** Common root paths used to read in parquet files on the `conviva-prod-archive`
   *  GCS bucket on Databricks. 
   */
  object PrArchPaths {
    /** The root path on `conviva-prod-archive`. */
    val root    = "/mnt/conviva-prod-archive-"
    /** Path to the daily session summary parquet files. */
    val daily   = root + "pbss-daily/pbss/daily"
    /** Path to the hourly session summary parquet files. */
    val hourly  = root + "pbss-hourly/pbss/hourly/st=0"
    /** Path to the monthly session summary parquet files. */
    val monthly = root + "pbss-monthly/pbss/monthly"
    /** Path to the parquet heartbeat (raw log) files. */
    val rawlog  = root + "pbrl/3d/rawlogs/pbrl/lt_1"
  }

  /** Path to the `Geo_Utils` folder on Databricks. */
  object GeoUtils {
    /** The root path. */
    val root = "dbfs:/FileStore/Geo_Utils"
  }

  trait ProdPath extends Customer {
    def year: Int 
    def fmt(s: Int) = f"${s}%02d"
    def toString_(x: List[Int]) = {
      val out = x.map(fmt(_)).mkString(",")
      if (x.length == 1) out else s"{$out}"
    }
  }

  /** Class with methods to construct paths to monthly PbSS parquet data on Databricks. 
   *  @param year $year
   *  @param month $month Only a single value for month is permitted. 
   *  @return 
   *  @example {{{
   *  PbSSMonthly(year = 2023, month = 1).asis // all customers
   *  PbSSMonthly(2023, 1).custAll // defaults to current year, with all customers
   *  PbSSMonthly(2022,  12).custName("DSS") // Different year with only Disney customer
   *  PbSSMonthly(2022, 12).custNames(List("DSS", "CBSCom")) // Only Disney and CBS customers
   *  PbSSMonthly(2022, 12).custID(1960180360) // call by Id
   *  PbSSMonthly(2023, 3).custIDs(List(1960180360, 1960180492) // call by Ids
   *  PbSSMonthly(2023, 5).custTake(3) // take only the first n customers, in
   *  this case 3
   *  }}}
   */ 
  case class PbSSMonthly(year: Int = 2023, month: Int) extends ProdPath {
    val (nyear, nmonth) = if (month == 12) (year + 1, 1) else (year, month + 1)
    def asis() = List(PrArchPaths.monthly, s"y=${year}", f"m=${fmt(month)}",
      f"dt=c${year}_${fmt(month)}_01_08_00_to_${nyear}_${fmt(nmonth)}_01_08_00")
    .mkString("/")
    override def path = asis()
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
   *  PbSSDaily(year = 2023, month = 1, day = 12).asis // all customers
   *  PbSSDaily(month = 1, day = 12).custAll // defaults to current year, with all customers
   *  PbSSDaily(1, 12).custName("DSS") // Different year with only Disney customer
   *  PbSSDaily(12, 28).custNames(List("DSS", "CBSCom")) // Only Disney and CBS customers
   *  PbSSDaily(2, 13).custID(1960180360) // call by Id
   *  PbSSDaily(year = 2022,  month = 3, day = 1).custIDs(List(1960180360, 1960180492) // call by Ids
   *  PbSSDaily(5, 27, 2023).custTake(3) // take only the first n customers, in
   *  this case 3
   *  // Get customer data over 3 days
   *  List.range(1, 4).map(d => PbSSDaily(year = 2023, month = 2, day = d).custName("DSS"))
   *  }}}
   */ 
  case class PbSSDaily(month: Int, day: Int, year: Int = 2023) 
      extends ProdPath {
    val (nyear, nmonth, nday) = if (month == 12 & day == 31) 
      (year + 1, 1, 1) else (year, month, day + 1)
    def asis() = List(PrArchPaths.daily, s"y=${year}", f"m=${fmt(month)}", 
      f"dt=d${year}_${fmt(month)}_${fmt(day)}_08_00_to_${nyear}_${fmt(nmonth)}_${fmt(nday)}_08_00")
    .mkString("/")
    override def path = asis() 
  }

  /** Class with methods to construct paths to hourly PbSS parquet data on
   *  Databricks. If parameter names are omitted, then the order is month, day,
   *  hour(s), year. The default year is the current year.  Only the `hours`
   *  parameter can take a list of integers.
   *  @param month $month
   *  @param day $day
   *  @param hours $hour
   *  @param year $year
   *  @return 
   *  @example {{{
   *  PbSSHourly(year = 2023, month = 1, day = 12, hours = List(2, 3)).asis // all customers
   *  PbSSHourly(month = 1, day = 12, hours = List(3, 5)).custAll // defaults to current year, with all customers
   *  PbSSHourly(1, 12, List.range(1, 7)).custName("DSS") // Different year with only Disney customer
   *  PbSSHourly(2, 13, List(2)).custTake(10)
   *  PbSSHourly(year = 2022,  month = 3, day = 1, hours = List(4, 5)).custIDs(List(1960180360, 1960180492) // call by Ids
   *  }}}
   */ 
  case class PbSSHourly(month: Int, day: Int, hours: List[Int], year: Int = 2023) 
      extends ProdPath {
    def asis() = List(PrArchPaths.hourly, s"y=${year}", f"m=${fmt(month)}", f"d=${fmt(day)}",
      f"dt=${year}_${fmt(month)}_${fmt(day)}_${toString_(hours)}")
      .mkString("/")
    override def path = asis()
  }

  /** Class with methods to construct paths to hourly RawLog parquet data on
   *  Databricks. If parameter names are omitted, then the order is month, day,
   *  hour(s), year. The default year is the current year.  Only the `hours`
   *  parameter can take a list of integers.
   *  @param month $month
   *  @param day $day
   *  @param hours $hour
   *  @param year $year
   *  @return 
   *  @example {{{
   *  PbRawLog(year = 2023, month = 1, day = 12, hours = List(2, 3)).asis // all customers
   *  PbRawLog(month = 1, day = 12, hours = List(3)).custAll // defaults to current year, with all customers
   *  PbRawLog(1, 12, List.range(1, 7)).custName("DSS") // Different year with only Disney customer
   *  PbRawLog(year = 2022, 2, 13, List(2)).custTake(10)
   *  PbRawLog(3, 1, hours = List(4, 5)).custIds(List(1960180360, 1960180492) // call by Ids
   *  }}}
   */ 
  case class PbRawLog(month: Int, day: Int, hours: List[Int], year: Int = 2023) 
    extends ProdPath {
      def asis() = List(PrArchPaths.rawlog, s"y=${year}", f"m=${fmt(month)}", f"d=${fmt(day)}",
        f"dt=${year}_${fmt(month)}_${fmt(day)}_${toString_(hours)}")
        .mkString("/")
      override def path = asis()
  }

}

