package conviva.surgeon

class PbRl_Suite extends munit.FunSuite {

  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._
  import conviva.surgeon.PbRl._
  import conviva.surgeon.Sanitize._
  import conviva.surgeon.GeoInfo._
  import conviva.surgeon.Paths2._

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  PathDB.geoUtilPath = PathDB.testPath
  PathDB.root = PathDB.testPath
  PathDB.pbrlHourly = "pbrl"

  val path = Path.pbrl("2023-05-01T09").cust("c3.DuoFC").toList
  val dat = spark.read.parquet(path:_*).cache
  val d701 = dat.where(sessionId === 701891892)

  test("Data nrow should be expected") {
    val nrow = dat.count.toInt
    assertEquals(nrow, 100)
  }

  test("c3isAd should work as expected") {
    val t1 = d701.select(c3isAd, c3isAd.recode)
    assertEquals(t1.select("c3_isAd").first.apply(0), "F")
    assertEquals(t1.select("c3_isAd_rc").first.apply(0).toString, "false")
  }

  val path2 = Path.pbrl("2023-12-28T12").cust("c3.FappleTV").toList
  val dat2 = spark.read.parquet(path2:_*)

  test("ipv6 should work as expected") {
    val e1 = "38:1:5:137:3:1:84:96:13:29:190:184:171:173:248:37"
    val e2 = "2601:0589:0301:5460:0d1d:beb8:abad:f825"
    val t1 = dat2
      .select(ipv6.concat, ipv6.concatToHex)
      .filter(sessionId === -776801731)
    assertEquals(e1, t1.select("ipv6").first.getString(0))
    assertEquals(e2, t1.select("ipv6Hex").first.getString(0))

  }

  test("ipv4 should work as expected") {
    val t1 = dat2.select(ipv4.concat).first.getString(0)
    val e1 = "107:130:101:70"
    assertEquals(t1, e1)
  }

  // test("timeStampUs should compute us/ms/sec") {
    // val t1 = d8905.select(sessionTimeMs).first.getInt(0)
    // assertEquals(t1, 1906885)
  // }

  // test("geoInfo should be expected") {
    // d701.select(geoInfo("city")).first.getInt(0)
  // d701.select(geoInfo("city").label).first.getString(0)
  // }
}
