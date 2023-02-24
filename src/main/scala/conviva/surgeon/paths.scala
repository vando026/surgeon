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
   
  private val dyear = 2023

  private def fmt(x: Any, offset: Int = 0): String = {
    def ft(s: Int) = f"${s}%02d"
    x match {
      case h: List[Any] => h.map(i => ft(i.toString.toInt + offset)).mkString(",")
      case h: Int => ft(h + offset)
      case h if (h == "*" || h == "{*}") => "*"
      case h: String => ft(h.toInt + offset)
    }
  }

  private def parseCid(cid: Any): String = {
    cid match {
      case s: List[Any] => s.mkString(",")
      case s: Int => s.toString
      case s: String => s
    } 
  }

  object Root {
    def root    = "/mnt/conviva-prod-archive-"
    def daily   = root + "pbss-daily/pbss/daily"
    def hourly  = root + "pbss-hourly/pbss/hourly/st=0"
    def monthly = root + "pbss-monthly/pbss/monthly"
    def rawlog  = root + "pbrl/3d/rawlogs/pbrl/lt_1"
    def geoUtilCustomer = "dbfs:/FileStore/Geo_Utils/cust_dat.txt"
  }

  case class Stitch(_month: Any, _day: Any, _hour: Any, _cid: Any = "*", _year: Int = dyear)  {
    def fyear = s"y=${_year}"
    def month = f"m=${fmt(_month)}"
    def day = f"d=${fmt(_day)}"
    def cid = s"cust={${parseCid(_cid)}}"
    def dtm = f"dt=c${_year}_{${fmt(_month)}}_01_08_00_to_${_year}_{${fmt(_month, 1)}}_01_08_00"
    def dtd = f"dt=d${_year}_${fmt(_month)}_{${fmt(_day)}}_08_00_to_${_year}_${fmt(_month)}_{${fmt(_day, 1)}}_08_00"
    def dth = f"dt=${_year}_${fmt(_month)}_${fmt(_day)}_{${fmt(_hour)}}"
  }
  
  /** Returns a string of the file path to the monthly PbSS parquet data.
   *
   *  @param month $month
   *  @param cid $cid 
   *  @param year $year
   *  @return 
   *  @example {{{
   *  monthly(month = 1, cid = 198020000)
   *  monthly(month = 1, year = 2022) 
   *  monthly(month = 5, cid = List(1960180360, 1960180361))
   *  monthly(month = 5, cid = "1960180360, 1960180361")
   *  }}}
   */ 
  def pbssMonthly(month: Any, cid: Any = "*", year: Int = dyear): String = {
    val m = Stitch(month, 0, 0, cid, year)
    List(Root.monthly, m.fyear, m.month, m.dtm, m.cid).mkString("/")
  }

  /** Returns a string of the file path to the daily PbSS parquet data.
   *
   *  @param month $month
   *  @param day $day
   *  @param cid $cid  
   *  @param year $year
   *  @return 
   *  @example {{{
   *  daily(month = 10, day = 2, cid = "1960180360")}
   *  daily(month = 12, day = 13, cid = 1960180360, year = 2022) 
   *  }}}
   */ 
  def pbssDaily(month: Any, day: Any, cid: Any = "*", year: Int = dyear): String = {
    val m = Stitch(month, day, 0, cid, year)
    List(Root.daily, m.fyear, m.month, m.dtd, m.cid).mkString("/")
  }

  /** Returns a string of the file path to the hourly PbSS parquet data.
   *
   *  @param month $month
   *  @param cid A $cid  
   *  @param hour $hour
   *  @param year $year
   *  @return 
   *  @example {{{
   *  hourly(month = 10, day = 2, hour = 12, cid = "1960180360")
   *  hourly(month = 10, day = 2, hour = List.range(12, 18))
   *  }}}
   */ 
  def pbssHourly(month: Any, day: Any, hour: Any, cid: Any = "*", year: Int = dyear): String = {
    val m = Stitch(month, day, hour, cid, year)
    List(Root.hourly, m.fyear, m.month, m.day, m.dth, m.cid).mkString("/")
  }

  /** Returns a string of the file path to the hourly RawLog (Heartbeat) parquet data.
   *
   *  @param month $month
   *  @param day $day
   *  @param cid $cid.  
   *  @param hour $hour
   *  @param year $year
   *  @return 
   *  @example {{{
   *  rawlog(month = 10, day = 2, hour = 12, cid = "1960180360")
   *  rawlog(month = 10, day = 2, hour = List.range(12, 18))
   *  }}}
   */ 
  def pbRawLog(month: Any, day: Any, hour: Any, cid: Any = "*", year: Int = dyear): String = {
    val m = Stitch(month, day, hour, cid, year)
    List(Root.rawlog, m.fyear, m.month, m.day, m.dth, m.cid).mkString("/")
  }
}
