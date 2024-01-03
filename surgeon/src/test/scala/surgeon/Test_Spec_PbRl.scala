package conviva.surgeon

class PbRl_Suite extends munit.FunSuite {

  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._
  import conviva.surgeon.PbRl._
  import conviva.surgeon.Sanitize._
  import conviva.surgeon.GeoInfo._

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  val dataPath = "./surgeon/src/test/data" 
  val dat = spark.read.parquet(s"${dataPath}/pbrlHourly1.parquet").cache
  val d701 = dat.where(sessionId === 701891892)

  val cust_dat = Map(
    207488736 -> "c3.MSNBC",
    744085924 -> "c3.PMNN",
    1960180360 -> "c3.TV2",
    978960980 -> "c3.BASC"
  )

  test("Data nrow should be expected") {
    val nrow = dat.count.toInt
    assertEquals(nrow, 100)
  }

  test("c3isAd should work as expected") {
    val t1 = d701.select(c3isAd, c3isAd.recode)
    assertEquals(t1.select("c3_isAd").first.apply(0), "F")
    assertEquals(t1.select("c3_isAd_rc").first.apply(0).toString, "false")
  }

  val dat2 = spark.read.parquet(s"${dataPath}/pbrlHourly2.parquet")

  test("ipv6 should work as expected") {
    val e1 = "38:1:5:137:3:1:84:96:13:29:190:184:171:173:248:37"
    val e2 = "2601:0589:0301:5460:0d1d:beb8:abad:f825"
    val t1 = dat2
      .select(ipv6.concat, ipv6.concatToHex)
      .filter(sessionId === -776801731)
    assertEquals(e1, t1.select("ipv6").first.getString(0))
    assertEquals(e2, t1.select("ipv6Hex").first.getString(0))

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
