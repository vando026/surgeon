package conviva.surgeon

object Paths2 {

  import java.time.{LocalDateTime, LocalDate}
  import java.time.format.DateTimeFormatter
  import org.apache.spark.sql.DataFrame
  import conviva.surgeon.Customer._
  import conviva.surgeon.GeoInfo._

  /** Database of common paths to the parquet files on Databricks. **/
  object PathDB {
    val root =  "/mnt"
    /** Root path to `databricks-user-share`. */
    val dbUserShare = "/mnt/databricks-user-share"
    /** Path to the daily session summary parquet files. */
    val pbssDaily = "conviva-prod-archive-pbss-daily/pbss/daily"
    /** Path to the hourly session summary parquet files. */
    val pbssHourly = "conviva-prod-archive-pbss-hourly/pbss/hourly"
    /** Path to the monthly session summary parquet files. */
    val pbssMonthly = "conviva-prod-archive-pbss-monthly/pbss/monthly"
    /** Path to the parquet heartbeat (raw log) files. */
    val pbrlHourly = "pbrl/3d/rawlogs/pbrl/"
    /** Path to Geo_Utils folder on Databricks. */
    val geoUtil = "dbfs:/FileStore/Geo_Utils"
    /** Path to personally updated files */
    val fileStore = "dbfs:/FileStore/avandormael"
    /** Path to test data for this library. */
    val testPath = "./surgeon/src/test/data" 
  }


  trait DataPath {
    def fmt02(s: Int) = f"${s}%02d"
    def pt(s: String) = DateTimeFormatter.ofPattern(s)
    def toList: List[String]
    def toString: String
    def ymd() = pt("yyyy-MM-dd")
    def ym() = pt("'y='yyyy/'m='MM") 
  }

  class Monthly(dt: String, units: List[Int]) extends DataPath {
    val pdates = units.sorted.map(i => LocalDate.parse(s"$dt-${fmt02(i)}-01", ymd))
    val p1 = pt("yyyy_MM_01_08_00")
    def p2(s: String) = s"{$s}_01_08_00"
    def toList() = {
      for (d <- pdates) yield s"${d.format(ym)}/dt=c${d.format(p1)}_to_${d.plusMonths(1).format(p1)}"
    }
  }


  class Daily(dt: String, units: List[Int]) extends DataPath  {
    val pdates = units.sorted.map(i => LocalDate.parse(s"$dt-${fmt02(i)}", ymd))
    val p1 = pt("yyyy_MM_dd_08_00")
    def toList() = {
      for (d <- pdates) yield s"${d.format(ym)}/dt=d${d.format(p1)}_to_${d.plusDays(1).format(p1)}"
    }
  }

  class Hourly(dt: String, units: List[Int], st: Int = 0) extends DataPath {
    val p1 = pt("'y='yyyy/'m='MM/'d='dd")
    val p2 = pt("yyyy_MM_dd_HH")
    val p3 = pt("yyyy-MM-dd HH:mm")
    val pdates = units.map(i => LocalDateTime.parse(s"$dt ${fmt02(i)}:00", p3))
    def toList()  = {
      for (d <- pdates) yield s"st=${st}/${d.format(p1)}/dt=${d.format(p2)}"
    }
  }


    // private def stitch(obj: DataPath, cnames: String) = 
    //   s"${obj.toString}/cust={${cnames}}"
  
    // val name = List("c3.TopServe", "c3.DuoFC")
  // val c3Map = getGeoData("customer", PathDB.testPath)
  // c3NameToId(mkStrList(name), c3Map).mkString(",")
    //   val cids = c3IdOnPath(obj.toList).sorted.take(take)
    //   stitch(obj, cids.map(_.toString).mkString(","))

  val datesList  = dates.map(i => s"${rootName}/${bodyName}/${i}")
    c3IdOnPath(dates).sorted.take(take)

  def parseCust(x: Any, dates: List[String]): String = {
    x match {
      case s: List[Int] => s.mkString(",")
      case s: List[String] => c3NameToId(s, c3Map).mkString(",")
      case s: Int if (s < 1000) => c3IdOnPath(dates).sorted.take(s).mkString(",")
      case s: Int => s.toString
      case s: String => c3NameToId(s, c3Map).mkString(",")
      case _ => throw new Exception("Input not accepted")
    }
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

  case class Path(root: String = PathDB.root) {
    var dates = List[String]()
    var rootName = root
    var bodyName = ""
    var custList = ""
    val c3Map = getGeoData("customer", root)
    def body(path: String): this.type = {
      bodyName = path
      this
    }
    def pbss(dt: String): this.type = {
      dates = dt match {
        case pMonth(dt, month) => {
          if (bodyName.isEmpty) bodyName = PathDB.pbssMonthly
          new Monthly(dt, parseRegex(month)).toList
        }
        case pDayMonth(dt, day) => {
          if (bodyName.isEmpty) bodyName = PathDB.pbssDaily
          new Daily(dt, parseRegex(day)).toList
        }
        case pHourDayMonth(dt, hour) => {
          if (bodyName.isEmpty) bodyName = PathDB.pbssHourly
          new Hourly(dt, parseRegex(hour)).toList
        }
        case _ => throw new Exception("Incorrect datetime entry, see surgeon.wiki for examples.")
      }
      this
    }
    // def pbrl(dt: String, units: List[Int]): this.type = {
    //   dates = dt.length match {
    //     case 9 => {
    //       if (bodyName.isEmpty) bodyName = PathDB.pbrlHourly
    //       new Hourly(dt, units).toList
    //     }
    //     case _  => throw new Exception(msg)
    //   }
    //   this
    // }
    def cust(x: Any): this.type = {
      custList = s"/cust={${parseCust(x)}}"
      this
    } 
    def toList = dates.map(i => s"${rootName}/${bodyName}/${i}${custList}")
    override def toString() = toList.toString
  }

  val Prod = new Path()

    // val pMonth = "^(202[0-9])-(\\{[0-9,-]+\\}|[0-9]{2})$".r
    // val pDayMonth = "^(202[0-9]-[0-9]{2})-(\\{[0-9,-]+\\}|[0-9]{2})$".r
    // val pHourDayMonth = "^(202[0-9]-[0-9]{2}-[0-9]{2})T(\\{[0-9,-]+\\}|[0-9]{2})$".r

  // "2023_02" match {
  //   case pMonth(dt, day) => println("this is month")
  //   case pDayMonth(dt, day) => println("this is day")
  //   case pHourDayMonth(dt, day) => println("this is hour")
  // }

    // def parseRegex(x: String): List[Int] = {
    //   x.toString.replaceAll("[{}]", "")
    //     .split(",").map(_.split("-"))
    //     .flatMap(i => if (i.length == 2) List.range(i(0).toInt, i(1).toInt) else i)
    //     .map(_.toString.toInt).toList
    // }

}
