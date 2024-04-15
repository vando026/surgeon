package conviva.surgeon

object Paths {

  import java.time.{LocalDateTime, LocalDate}
  import java.time.format.DateTimeFormatter
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
    def ymd() = pt("yyyy-MM-dd")
    def ym() = pt("'y='yyyy/'m='MM") 
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
  class Hourly(dt: String, units: List[Int], path: String = PathDB.pbssHourly) extends DateFormats {
    val p1 = pt("'y='yyyy/'m='MM/'d='dd")
    val p2 = pt("yyyy_MM_dd_HH")
    val p3 = pt("yyyy-MM-dd HH:mm")
    val pdates = units.map(i => LocalDateTime.parse(s"$dt ${fmt02(i)}:00", p3))
    def toList()  = {
      for (d <- pdates) yield 
        s"${PathDB.root}/${path}/${d.format(p1)}/dt=${d.format(p2)}"
    }
  }

  class CustBuilder  {
    def idToString(s: Any): String = {
      val out = s match {
        case s: Int => s.toString
        case s: String => s
        case _ => throw new Exception("Argument be either Int or String")
      } 
      out
    }
    def idsToString(s: List[Any]): String = {
      val out = s match {
        case s: List[Int] => s.mkString(",")
        case s: List[String] => s.mkString(",")
        case _ => throw new Exception("Argument be either List[Int] or List[String]")
      } 
      out
    }
  }

  class SurgeonPath(val toList: List[String]) extends CustBuilder {
    var i = 0
    i += 1
    private def returnPath(clist: String): SurgeonPath = {
      if (i == 1)  new SurgeonPath(toList.map(i => s"${i}/cust={${clist}}"))
      else new SurgeonPath(toList)
    }
    def c3name(name: String) = returnPath(c3NameToId(name)(0).toString)
    def c3names(names: List[String]) = returnPath(c3NameToId(names).mkString(","))
    def c3id(id: Any) = returnPath(idToString(id))
    def c3ids(ids: List[Any]) = returnPath(idsToString(ids))
    def c3take(n: Int) = returnPath(c3IdOnPath(toList).sorted.take(n).mkString(","))
  }

  class DatesBuilder(dt: String, path: String) {
    val pMonth = "^(202[0-9])-(\\{[0-9,-]+\\}|[0-9]{2})$".r
    val pDayMonth = "^(202[0-9]-[0-9]{2})-(\\{[0-9,-]+\\}|[0-9]{2})$".r
    val pHourDayMonth = "^(202[0-9]-[0-9]{2}-[0-9]{2})T(\\{[0-9,-]+\\}|[0-9]{2})$".r
    def parseRegex(x: String): List[Int] = {
      x.toString.replaceAll("[{}]", "")
        .split(",").map(_.split("-"))
        // end points are inclusive
        .flatMap(i => if (i.length == 2) List.range(i(0).toInt, i(1).toInt + 1) else i)
        .map(_.toString.toInt).toList
    }
    val dtTrim = dt.replaceAll("[\\s]+", "")
    val toList = dtTrim match {
        case pMonth(dtTrim, month) => new Monthly(dtTrim, parseRegex(month)).toList  
        case pDayMonth(dtTrim, day) =>  new Daily(dtTrim, parseRegex(day)).toList  
        case pHourDayMonth(dtTrim, hour) => new Hourly(dtTrim, parseRegex(hour), path).toList  
        case _ => throw new Exception("Incorrect date-time format, see surgeon.wiki for examples.")
      }
  }

  object Path {
    def pbss(dt: String): SurgeonPath = {
      val datesList = new DatesBuilder(dt, PathDB.pbssHourly).toList
      new SurgeonPath(datesList)
    }
    def pbrl(dt: String): SurgeonPath = {
      val datesList = new DatesBuilder(dt, PathDB.pbrlHourly).toList
      new SurgeonPath(datesList)
    }
  }

  // val tt = Path.pbss("2023-01")
  // tt.cust(12345).paths
  // tt.paths

}
