package conviva.surgeon

object Paths {

  import java.time.{LocalDateTime, LocalDate}
  import java.time.format.DateTimeFormatter
  import java.time.temporal.ChronoUnit
  import org.apache.spark.sql.DataFrame
  import conviva.surgeon.Customer._
  import conviva.surgeon.GeoInfo._

  /** Class for setting components of path names to files. **/
  class SetPaths {
    var root =  "/mnt"
    var st = 0;  var lt = 1
    /** Root path to `databricks-user-share`. */
    var dbUserShare = "/mnt/databricks-user-share"
    /** Path to the daily session summary parquet files. */
    var pbssDaily = "conviva-prod-archive-pbss-daily/pbss/daily"
    /** Path to the hourly session summary parquet files. */
    var pbssHourly = s"conviva-prod-archive-pbss-hourly/pbss/hourly/st=$st"
    /** Path to the monthly session summary parquet files. */
    var pbssMonthly = "conviva-prod-archive-pbss-monthly/pbss/monthly"
    /** Path to the parquet heartbeat (raw log) files. */
    var pbrlHourly = s"conviva-prod-archive-pbrl/3d/rawlogs/pbrl/lt_$lt"
    /** Path to the minute session summary parquet files. */
    var pbssMinute = "databricks-user-share/avandormael/tlb1min"
    /** Path to Geo_Utils folder on Databricks. */
    var geoUtilPath = "dbfs:/FileStore/Geo_Utils"
    /** Path to personally updated files */
    val fileStore = "dbfs:/FileStore/avandormael"
    /** Path to test data for this library. */
    val testPath = "./surgeon/src/test/data" 
  }

  var PathDB = new SetPaths()

  /** Formats for dates **/
  trait DateFormats {
    def fmt02(s: Int) = f"${s}%02d"
    def pt(s: String) = DateTimeFormatter.ofPattern(s)
    // def toString: String
    def ymdh() = pt("yyyy-MM-dd HH:mm")
    def ymd() = pt("yyyy-MM-dd")
    def y4() = pt("yyyy")
    def ym_() = pt("yyyy_MM_")
    def ym() = pt("'y='yyyy/'m='MM") 
    def ymn() = pt("'y='yyyy/'m='") 
    def ymdd() = pt("'y='yyyy/'m='MM/'d='dd")
    def ymd_h() = pt("yyyy_MM_dd_HH")
    def ym_d() = pt("yyyy_MM_dd")
  }

  /** Defines path for monthly data **/
  class Monthly(dt: String, units: List[Int]) extends DateFormats {
    units.map(i => LocalDate.parse(s"${dt}-${fmt02(i)}-01", ymd)) // check if sensible
    val baseDate = LocalDate.parse(s"${dt}-01-01", ymd)
    if (units.contains(12) && units.length > 1) 
      throw new Exception("surgeon-paths: If contains month=12, then only month=12 allowed.")
    val s = "_01_08_00"
    var l = "{"; var r = "}"
    if (units.length == 1) {l = ""; r = ""}
    val dates = units.distinct.map(i => baseDate.plusMonths(i - 1))
    val nextDates = dates.map(i => i.plusMonths(1))
    val mnths = dates.map(i => fmt02(i.getMonthValue)).mkString(",")
    val mnthsNext = nextDates.map(i => fmt02(i.getMonthValue)).mkString(",")
    val year = dates.map(_.getYear).distinct(0)
    val yearNext = nextDates.map(_.getYear).distinct(0)
    private val p1 = s"${PathDB.root}/${PathDB.pbssMonthly}/${baseDate.format(ymn)}${l}${mnths}${r}/"
    private val p2 = s"dt=c${year}_${l}${mnths}${r}${s}_to_${yearNext}_${l}${mnthsNext}${r}${s}"
    override def toString() = p1 + p2
  }

  /** Defines path for daily data **/
  class Daily(dt: String, units: List[Int]) extends DateFormats {
    units.map(i => LocalDate.parse(s"${dt}-${fmt02(i)}", ymd)) // check if sensible
    val baseDate = LocalDate.parse(s"${dt}-01", ymd)
    if (units.contains(31) && baseDate.getMonthValue == 12 && units.length > 1) 
      throw new Exception("surgeon-paths: If contains month=12/day=31, then only day=31 allowed.")
    val s = "_08_00"
    var l = "{"; var r = "}"
    if (units.length == 1) {l = ""; r = ""}
    val dates = units.distinct.map(i => baseDate.plusDays(i - 1))
    val nextDates = dates.map(i => i.plusDays(1))
    val days = dates.map(i => fmt02(i.getDayOfMonth)).mkString(",")
    val daysNext = nextDates.map(i => fmt02(i.getDayOfMonth)).mkString(",")
    val year = dates.map(_.getYear).distinct(0)
    val yearNext = nextDates.map(_.getYear).distinct(0)
    val mnth = dates.map(i => fmt02(i.getMonthValue)).distinct(0)
    val mnthNext = nextDates.map(i => fmt02(i.getMonthValue)).distinct(0)
    private val p1 = s"${PathDB.root}/${PathDB.pbssDaily}/${baseDate.format(ym)}/"
    private val p2 = s"dt=d${year}_${mnth}_${l}${days}${r}${s}_to_${yearNext}_${mnthNext}_${l}${daysNext}${r}${s}"
    override def toString() = p1 + p2
  }

  /** Defines path for hourly data **/
  class Hourly(dt: String, units: List[Int], path: String = PathDB.pbssHourly) extends DateFormats {
    units.map(i => LocalDate.parse(s"${dt} ${fmt02(i)}:00", ymdh)) // check if sensible
    var l = "{"; var r = "}"
    if (units.length == 1) {l = ""; r = ""}
    val baseDate = LocalDateTime.parse(s"${dt} 00:00", ymdh)
    val dates = units.map(baseDate.plusHours(_))
    val hrs = dates.distinct.map(i => fmt02(i.getHour)).mkString(",")
    val p1 = s"${PathDB.root}/${path}/${baseDate.format(ymdd)}"
    val p2 = s"/dt=${baseDate.format(ym_d)}_${l}${hrs}${r}"
    override def toString() = p1 + p2
  }

  class DatesBuilder(dt: String, path: String) {
    val pMonth = "^(202[0-9])-(\\{[0-9,-]+\\}|[0-9]{1,2})$".r
    val pDayMonth = "^(202[0-9]-[0-9]{1,2})-(\\{[0-9,-]+\\}|[0-9]{1,2})$".r
    val pHourDayMonth = "^(202[0-9]-[0-9]{1,2}-[0-9]{1,2})T(\\{[0-9,-]+\\}|[0-9]{1,2})$".r
    def parseRegex(x: String): List[Int] = {
      x.toString.replaceAll("[{}]", "")
        .split(",").map(_.split("-"))
        .flatMap(i => if (i.length == 2) List.range(i(0).toInt, i(1).toInt + 1) else i)
        .map(_.toString.toInt).toList
    }
    val dtTrim = dt.replaceAll("[\\s]+", "")
    override def toString() = dtTrim match {
        case pMonth(dtTrim, month) => new Monthly(dtTrim, parseRegex(month)).toString
        case pDayMonth(dtTrim, day) =>  new Daily(dtTrim, parseRegex(day)).toString  
        case pHourDayMonth(dtTrim, hour) => new Hourly(dtTrim, parseRegex(hour), path).toString
        case _ => throw new Exception("Incorrect date-time format, see surgeon.wiki for examples.")
      }
  }

  def idToString(s: Any): String = {
    val out = s match {
      case s: Int => s.toString
      case s: String => s
      case _ => throw new Exception("Argument must be either Int or String")
    } 
    out
  }
  def idsToString(s: List[Any]): String = {
    val out = s match {
      case s: List[Int] => s.mkString(",")
      case s: List[String] => s.mkString(",")
      case _ => throw new Exception("Argument must be either List[Int] or List[String]")
    } 
    out
  }

  class SurgeonPath(val date: String) {
    private def returnPath(clist: String): String = date + s"/cust={${clist}}" 
    def c3name(name: String) = returnPath(c3NameToId(name)(0).toString)
    def c3names(names: List[String]) = returnPath(c3NameToId(names).mkString(","))
    def c3id(id: Any) = returnPath(idToString(id))
    def c3ids(ids: List[Any]) = returnPath(idsToString(ids))
    def c3take(n: Int) = returnPath(c3IdOnPath(date.toString).sorted.take(n).mkString(","))
    def c3all() = returnPath("*")
    override def toString = date
  }

  object Path {
    def pbss(dt: String): SurgeonPath = {
      val path = new DatesBuilder(dt, PathDB.pbssHourly).toString
      new SurgeonPath(path)
    }
    def pbrl(dt: String): SurgeonPath = {
      val path = new DatesBuilder(dt, PathDB.pbrlHourly).toString
      new SurgeonPath(path)
    }
  }

  // tt.cust(12345).paths
  // tt.paths

}
