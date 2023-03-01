// Project: define paths for pbss/prbl paths
// Description: 
// Date: 27-Jan-2023
package conviva.surgeon

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

  private def fmt(x: Any, offset: Int = 0): String = {
    def ft(s: Int) = f"${s}%02d"
    x match {
      case h: List[Int] => "{" + h.map(i => 
          ft(i.toString.toInt + offset)).mkString(",") + "}"
      case h: Int => ft(h + offset)
      case _ => throw new Exception("Only Int or List[Int] allowed")
    }
  }

  /** Returns a string of the file path to the monthly PbSS parquet data. Only
   *  one month per path is permitted.
   *
   *  @param year $year
   *  @param month $month
   *  @return 
   *  @example {{{
   *  monthly(year = 2023, month = 1)
   *  }}}
   */ 

  def pbssMonthly(year: Int, month: Int): String = {
    List(PrArchPaths.monthly, s"y=${year}", f"m=${fmt(month)}",
      f"dt=c${year}_${fmt(month)}_01_08_00_to_${year}_${fmt(month, 1)}_01_08_00")
    .mkString("/")
  }

  /** Returns a string of the file path to the daily PbSS parquet data.
   *
   *  @param month $month
   *  @param day $day
   *  @param year $year
   *  @return 
   *  @example {{{
   *  daily(month = 10, day = 2)
   *  daily(month = 12, day = 13, year = 2022) 
   *  }}}
   */ 
  def pbssDaily(month: Int, day: Any, year: Int = 2023): String = {
    List(PrArchPaths.daily, s"y=${year}", f"m=${fmt(month)}", 
      f"dt=d${year}_${fmt(month)}_${fmt(day)}_08_00_to_${year}_${fmt(month)}_${fmt(day, 1)}_08_00")
    .mkString("/")
  }

  /** Returns a string of the file path to the hourly PbSS parquet data.
   *
   *  @param month $month
   *  @param hour $hour
   *  @param year $year
   *  @return 
   *  @example {{{
   *  hourly(month = 10, day = 2, hour = 12)
   *  hourly(month = 10, day = 2, hour = List.range(12, 18), year = 2022)
   *  hourly(month = 10, day = 2, hour = "03")
   *  }}}
   */ 
  def pbssHourly(month: Int, day: Int, hour: Any, year: Int = 2023): String = {
    List(PrArchPaths.hourly, s"y=${year}", f"m=${fmt(month)}", f"d=${fmt(day)}",
      f"dt=${year}_${fmt(month)}_${fmt(day)}_${fmt(hour)}")
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
  def pbRawLog(month: Int, day: Int, hour: Any, year: Int = 2023): String = {
    List(PrArchPaths.rawlog, s"y=${year}", f"m=${fmt(month)}", f"d=${fmt(day)}",
      f"dt=${year}_${fmt(month)}_${fmt(day)}_${fmt(hour)}")
    .mkString("/")
  }
}

