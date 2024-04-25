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
    def ym_() = pt("yyyy_MM_")
    def ym() = pt("'y='yyyy/'m='MM") 
    def ymdd() = pt("'y='yyyy/'m='MM/'d='dd")
    def ymd_h() = pt("yyyy_MM_dd_HH")
    def ym_d() = pt("yyyy_MM_dd_")
  }

  /** Defines path for monthly data **/
  class Monthly(dt: String, units: List[Int]) extends DateFormats {
    val baseDate = LocalDate.parse(s"${dt}-01", ymd)
    val s = "_01_08_00"
    val mnths = units.distinct.map(fmt02(_)).mkString(",")
    val mnthsNext = units.map(_ + 1).map(fmt02).mkString(",")
    val rootPath = s"${PathDB.root}/${PathDB.pbssMonthly}/${baseDate.format(ym)}/"
    override def toString() = rootPath + s"dt=c${baseDate.format(ym)}{${mnths}}${s}_to_${baseDate.format(ym)}{${mnthsNext}}${s}"
  }

  /** Defines path for daily data **/
  class Daily(dt: String, units: List[Int]) extends DateFormats   {
    val baseDate = LocalDate.parse(s"${dt}-01", ymd)
    val s = "_08_00"
    val days = units.distinct.map(fmt02(_)).mkString(",") 
    val daysNext = units.map(_ + 1).map(fmt02).mkString(",") 
    val pathRoot = s"${PathDB.root}/${PathDB.pbssDaily}/${baseDate.format(ym)}/dt=d${baseDate.format(ym_)}"
    override def toString = pathRoot  + s"{${days}}${s}_to_${baseDate.format(ym_)}{${daysNext}}${s}"
  }

  /** Defines path for hourly data **/
  class Hourly(dt: String, units: List[Int], path: String = PathDB.pbssHourly) extends DateFormats {
    val baseDate = LocalDateTime.parse(s"${dt} 00:00", ymdh)
    val dates = units.map(baseDate.plusHours(_))
    val hrs = dates.distinct.map(i => fmt02(i.getHour)).mkString(",")
    override def toString() = s"${PathDB.root}/${path}/${dt.format(ymdd)}/dt=${dt.format(ymd)}_{${hrs}}"
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
