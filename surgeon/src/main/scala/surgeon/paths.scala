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
    def toList: List[String]
    // def toString: String
    def ymdh() = pt("yyyy-MM-dd HH:mm")
    def ymd() = pt("yyyy-MM-dd")
    def ym() = pt("'y='yyyy/'m='MM") 
    def ymdd() = pt("'y='yyyy/'m='MM/'d='dd")
    def ymd_h() = pt("yyyy_MM_dd_HH")
    def ym_d() = pt("yyyy_MM_dd_")
  }

  /** Defines path for monthly data **/
  class Monthly(dt: String, units: List[Int]) extends DateFormats {
    val pdates = units.sorted.map(i => LocalDate.parse(s"$dt-${fmt02(i)}-01", ymd))
    val p1 = pt("yyyy_MM_01_08_00")
    def p2(s: String) = s"{$s}_01_08_00"
    def toList() = {
      for (d <- pdates) yield 
        s"${PathDB.root}/${PathDB.pbssMonthly}/${d.format(ym)}/dt=c${d.format(p1)}_to_${d.plusMonths(1).format(p1)}"
    }
  }

  /** Defines path for daily data **/
  class Daily(dt: String, units: List[Int]) extends DateFormats  {
    val pdates = units.sorted.map(i => LocalDate.parse(s"$dt-${fmt02(i)}", ymd))
    val p1 = pt("yyyy_MM_dd_08_00")
    def toList() = {
      for (d <- pdates) yield 
        s"${PathDB.root}/${PathDB.pbssDaily}/${d.format(ym)}/dt=d${d.format(p1)}_to_${d.plusDays(1).format(p1)}"
    }
  }

  /** Defines path for hourly data **/
  class Hourly(date: String, hours: Array[Array[String]], path: String = PathDB.pbssHourly) extends DateFormats {
    val baseDate = LocalDateTime.parse(s"${date} 00:00", ymdh)
    def toDateTime(d: String, trange: Array[String]): List[java.time.LocalDateTime] = {
      val start = LocalDateTime.parse(s"${date} ${fmt02(trange(0).toInt)}:00", ymdh)
      if (trange.length == 2) {
        val end = LocalDateTime.parse(s"${d} ${fmt02(trange(1).toInt)}:00", ymdh)
        val hrs = ChronoUnit.HOURS.between(start, end) 
        val res = if (hrs < 0) 24L + hrs else hrs
        List.range(trange(0).toInt, trange(0).toInt + res).map(i => baseDate.plusHours(i)) 
      } else {
        List(baseDate.plusHours(trange(0).toInt))
      }
    }
    def mkPath(dt: java.time.LocalDateTime) = {
      s"${PathDB.root}/${path}/${dt.format(ymdd)}/dt=${dt.format(ymd_h)}"
    }
    def mkBaseDatePaths(dt: List[java.time.LocalDateTime]): String = {
      // only get hours in baseDate; ignore if hours cross into next 24 hour day
      val hrs = dt.filter(_.getDayOfMonth == baseDate.getDayOfMonth)
        .map(i => fmt02(i.getHour)).distinct.mkString(",")
      mkPath(baseDate) + s"_{${hrs}}"
    }
    val dates = hours.flatMap(toDateTime(date, _)).toList
    def toList(): List[String] = dates.map(mkPath)
    override def toString() = mkBaseDatePaths(dates)
  }

  class DatesBuilder(dt: String, path: String) {
    val pMonth = "^(202[0-9])-(\\{[0-9,-]+\\}|[0-9]{1,2})$".r
    val pDayMonth = "^(202[0-9]-[0-9]{1,2})-(\\{[0-9,-]+\\}|[0-9]{1,2})$".r
    val pHourDayMonth = "^(202[0-9]-[0-9]{1,2}-[0-9]{1,2})T(\\{[0-9,-]+\\}|[0-9]{1,2})$".r
    def parseRegex(x: String): Array[Array[String]] = {
      x.toString.replaceAll("[{}]", "")
        .split(",").map(_.split("-"))
    }
    val dtTrim = dt.replaceAll("[\\s]+", "")
    override def toString() = dtTrim match {
        // case pMonth(dtTrim, month) => new Monthly(dtTrim, parseRegex(month)).toLis
        // case pDayMonth(dtTrim, day) =>  new Daily(dtTrim, parseRegex(day)).toList  
        case pHourDayMonth(dtTrim, hour) => new Hourly(dtTrim, parseRegex(hour), path).toString
        case _ => throw new Exception("Incorrect date-time format, see surgeon.wiki for examples.")
      }
  }

  class CustBuilder  {
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
  }

  class SurgeonPath(val date: String) extends CustBuilder {
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
      val date = new DatesBuilder(dt, PathDB.pbssHourly).toString
      new SurgeonPath(date)
    }
    // def pbrl(dt: String): String = {
      // val datesList = new DatesBuilder(dt, PathDB.pbrlHourly).toStrings
      // new SurgeonPath(datesList)
    // }
  }

  val t2 = Path.pbss("2023-01-01T02").c3id(2000)
  // tt.cust(12345).paths
  // tt.paths

}
