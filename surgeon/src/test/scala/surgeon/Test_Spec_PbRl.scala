package conviva.surgeon

class PbRl_Suite extends munit.FunSuite {

  import conviva.surgeon.Heart._

  val dataPath = "./src/test/data" 
  val PathDB = SetPaths(geoUtil = dataPath)

  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._
  import conviva.surgeon.PbRl._
  import conviva.surgeon.Sanitize._
  import conviva.surgeon.GeoInfo._

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

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

  // test("timeStampUs should compute us/ms/sec") {
    // val t1 = d8905.select(sessionTimeMs).first.getInt(0)
    // assertEquals(t1, 1906885)
  // }

  test("geoInfo should be expected") {
    // d701.select(geoInfo("city")).first.getInt(0)
  // d701.select(geoInfo("city").label).first.getString(0)
  }
}
