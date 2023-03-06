// Project: define paths for pbss/prbl paths
// Description: 
// Date: 27-Jan-2023
package conviva.surgeon
import conviva.surgeon.Donor._

/** Object with methods to create file paths for parquet datasets on `/mnt/conviva-prod-archive`. 
   * @define month A value from 1 to 12 representing the month of the year. 
   * @define cid The Customer Id(s), eg 1960180360, which can be a list of strings or integers, an integer, or a string.
   * @define year The default in this version is 2023. Use this parameter to change to a previous year. 
   * @define day A value from 1 to 31 representing the day of the month.
   * @define hour A value from 0 to 23 representing an hour of the day. Can be a list of strings or integers, an integer, or a string.
*/


object Paths  {
   
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

  /** Construct Product Archive on Databricks for paths based on selection of Customer Ids. 
  */
  trait ProdPath {
    def year: Int
    def fmt(s: Int) = f"${s}%02d"
    def toString_(x: List[Int]) = x.map(fmt(_)).mkString(",")
  }

  /** Construct a path to monthly PbSS parquet data on Databricks.
   *  @param year $year
   *  @param month $month
   *  @return 
   *  @example {{{
   *  pbssMonthly(year = 2023, month = 1)
   *  }}}
   */ 
  case class pbssMonthly(year: Int = 2023, month: Int) extends ProdPath {
    val nyear = if (month == 12) year + 1 else year 
    val nmonth = if (month == 12) 1 else month + 1
    def asis() = List(PrArchPaths.monthly, s"y=${year}", f"m=${fmt(month)}",
      f"dt=c${year}_${fmt(month)}_01_08_00_to_${nyear}_${fmt(nmonth)}_01_08_00")
    .mkString("/")
  }

  /** Returns a string of the file path to the daily PbSS parquet data.
   *
   *  @param month $month
   *  @param day $day
   *  @param year $year
   *  @return 
   *  @example {{{
   *  pbssDaily(month = 10, day = 2)
   *  pbssDaily(month = 12, day = 13, year = 2022) 
   *  }}}
   */ 
  case class PbSSDaily(month: Int, day: Int, year: Int = 2023) extends Customer with ProdPath {
    val nyear = if (month == 12 & day == 31) year + 1 else year
    val nmonth = if (month == 12 & day == 31) 1 else month
    val nday = if (month == 12 & day == 31) 1 else day + 1
    def asis() = List(PrArchPaths.daily, s"y=${year}", f"m=${fmt(month)}", 
      f"dt=d${year}_${fmt(month)}_${fmt(day)}_08_00_to_${nyear}_${fmt(nmonth)}_${fmt(nday)}_08_00")
    .mkString("/")
    override def path = asis() 
  }

  /** Returns a string of the file path to the hourly PbSS parquet data.
   *
   *  @param month $month
   *  @param hour $hour
   *  @param year $year
   *  @return 
   *  @example {{{
   *  pbssHourly(month = 10, day = 2, hour = 12)
   *  pbssHourly(month = 10, day = 2, hour = List.range(12, 18), year = 2022)
   *  pbssHourly(month = 10, day = 2, hour = "03")
   *  }}}
   */ 

  case class PbSSHourly(month: Int, day: Int, hour: List[Int], year: Int = 2023) extends ProdPath {
    def asis() = List(PrArchPaths.hourly, s"y=${year}", f"m=${fmt(month)}", f"d=${fmt(day)}",
      f"dt=${year}_${fmt(month)}_${fmt(day)}_${toString_(hour)}")
      .mkString("/")
  }

  /** Returns a string of the file path to the hourly RawLog (Heartbeat) parquet data.
   *
   *  @param month $month
   *  @param day $day
   *  @param hour $hour
   *  @param year $year
   *  @return 
   *  @example {{{
   *  pbRawlog(month = 10, day = 2, hour = 12, year = 2022)
   *  pbRawlog(month = 10, day = 2, hour = List.range(12, 18))
   *  pbRawlog(month = 10, day = 2, hour = "02")
   *  }}}
   */ 
  case class PbRawLog(month: Int, day: Int, hour: List[Int], year: Int = 2023) extends ProdPath {
    def asis() = List(PrArchPaths.rawlog, s"y=${year}", f"m=${fmt(month)}", f"d=${fmt(day)}",
      f"dt=${year}_${fmt(month)}_${fmt(day)}_${toString_(hour)}")
      .mkString("/")
  }

}

