package conviva.surgeon

object Paths2 {

  import java.time.{LocalDateTime, LocalDate}
  import java.time.temporal.ChronoUnit
  import java.time.format.DateTimeFormatter
  import org.apache.spark.sql.DataFrame

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

  // val expect1 = s"${PathDB.pbssProd1M}/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00"
  // val expect1 = s"${PathDB.pbssProd1M}/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00"
  class Monthly(dt: String, units: List[Int]) extends DataPath {
    val pdates = units.sorted.map(i => LocalDate.parse(s"$dt-${fmt02(i)}-01", ymd))
    val p1 = pt("yyyy_MM_01_08_00")
    def p2(s: String) = s"{$s}_01_08_00"
    def toList() = {
      for (d <- pdates) yield s"${d.format(ym)}/dt=c${d.format(p1)}_to_${d.plusMonths(1).format(p1)}"
    }
    //   if (units.contains(12) && units.length > 1)
    //     throw new Exception("Cannot have multiple months if Month=12")
    //   val yearNext = if (units.contains(12) && units.length == 1) 
    //     pdates(0).plusYears(1).format(pt("yyyy")) else pdates(0).format(pt("yyyy"))
    //   val mnths = pdates.map(_.format(pt("MM"))).mkString(",")
    //   val mnthsNext = pdates.map(_.plusMonths(1).format(pt("MM"))).mkString(",")
    //   s"y=${dt}/m={${mnths}}/dt=c${dt}_${p2(mnths)}_to_${yearNext}_${p2(mnthsNext)}"
    // }
  }

  val dt = "2023"
  val units = List(11, 12)

  val tt = new Monthly("2023", List(11, 10))


  // val expect1 = s"${PathDB.pbssProd1d}/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00"
  class Daily(dt: String, units: List[Int]) extends DataPath  {
    val pdates = units.sorted.map(i => LocalDate.parse(s"$dt-${fmt02(i)}", ymd))
    val p1 = pt("yyyy_MM_dd_08_00")
    def toList() = {
      for (d <- pdates) yield s"${d.format(ym)}/dt=d${d.format(p1)}_to_${d.plusDays(1).format(p1)}"
    }
    // val p2 = pt("yyyy_MM")
    // override def toString = {
      // val days = pdates.map(_.format(pt("dd"))).mkString(",")
      // val daysNext = pdates.map(_.plusDays(1).format(pt("dd"))).mkString(",")
      // s"${dt.format(ym)}/dt=d${dt.format(p2)}_{${days}}_to_${dt.format(p2)}_{${daysNext}}"
    // }
  }

  // val dt = "2023-02"
  // val units = List(28, 29)
  // val t1 = new Daily("2023-02", List(28, 29))

  // val expect1 = s"${PathDB.pbssProd1h()}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360}"
  class Hourly(dt: String, units: List[Int], st: Int = 0) extends DataPath {
    val p1 = pt("'y='yyyy/'m='MM/'d='dd")
    val p2 = pt("yyyy_MM_dd_HH")
    val p3 = pt("yyyy-MM-dd HH:mm")
    val pdates = units.map(i => LocalDateTime.parse(s"$dt ${fmt02(i)}:00", p3))
    def toList()  = {
      for (d <- pdates) yield s"st=${st}/${d.format(p1)}/dt=d${d.format(p2)}"
    }
  }

  // val t2 = new Hourly("2023-02-01", List(1, 2))

  def parseCust(x: Any): String = {
    x match {
      case s: List[Int] => s.mkString(",")
      case s: List[String] => s.mkString(",")
      case s: Int if (s < 1000) => "yes"
      case s: Int => s.toString
      case s: String => s
    }
  }

 // class Path {
 //    protected var rname =  PathDB.root
 //    protected var bodyName = ""
 //    def root(root: String = PathDB.root): this.type = {
 //      rname = root
 //      this
 //    }
 //    def body(path: String): this.type = {
 //      bodyName = path
 //      this
 //    }
 //    override def toString = s"${rname}/${bodyName}"
 //  }

  // new Path().root("yy").body("uu")

  class SetPath(root: String = PathDB.root) {
    var dates = List[String]()
    var bodyName = ""
    var custList = ""
    val pMonth = "(202[0-9])-\\{?([0-9,-]+)\\}?".r
    val pDayMonth = "(202[0-9]-[0-9]{2})-\\{?([0-9,-]+)\\}?".r
    val pHourDayMonth = "(202[0-9]-[0-9]{2}-[0-9]{2})T\\{?([0-9,-]+)\\}?".r
    def parseRegex(x: String): List[Int] = {
      x.toString.split(",")
        .map(_.split("-"))
        .flatMap(i => if (i.length == 2) List.range(i(0).toInt, i(1).toInt) else i)
        .map(_.toString.toInt).toList
    }
    def body(path: String): this.type = {
      bodyName = path
      this
    }
    def pbss(dt: String): this.type = {
      dates = dt match {
        case pMonth(dt, month) => new Monthly(dt, parseRegex(month)).toList
        case pDayMonth(dt, day) => new Daily(dt, parseRegex(day)).toList
        // case pHourDayMonth(dt, hour) => new Hourly(dt, parseRegex(hour)).toList
        case _ => throw new Exception("oops")
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
    // def cust(x: Any): this.type = {
    //   custList = s"/cust={${parseCust(x)}}"
    //   this
    // } 
    // override def toString = s"${dates.map(i => s"${root}/${bodyName}/${i}").mkString(",")}${custList}"
    def toList = dates.map(i => s"${root}/${bodyName}/${i}${custList}")
  }

  // val Prod = new DataPath()
  // val Test = new DataPath(PathDB.testPath)
  // Prod.pbss("2024", List(1))
  // Test.body("pbss").pbss("2023", List(2))

  // val pMonth = "(202[0-9])-\\{?([0-9,-]+)\\}?".r
  // val pDayMonth = "(202[0-9]-[0-9]{2})-\\{?([0-9,-]+)\\}?".r
  // val pHourDayMonth = "(202[0-9]-[0-9]{2}-[0-9]{2})T\\{?([0-9,-]+)\\}?".r
  // // val t1 = "2023-{1-3,5}"
  // val t1 = "2023-02-{2,4}"
  // // val t1 = "2023-01-02T{01}"

  // val out: (String, List[Int]) = t1 match {
  //   case pMonth(dt, month) => (dt, parseRegex(month))
  //   case pDayMonth(dt, day) => (dt, parseRegex(day))
  //   case pHourDayMonth(dt, hour) => (dt, parseRegex(hour))
  //   case _ => throw new Exception("oops")
  // }
  // out

  // val out: DataPath = t1 match {
  //   case pMonth(dt, month) => new Monthly(dt, parseRegex(month))
  //   case pDayMonth(dt, day) => new Daily(dt, parseRegex(day))
  //   case pHourDayMonth(dt, hour) => (dt, parseRegex(hour))
  //   case _ => throw new Exception("oops")
  // }
  // out


}
