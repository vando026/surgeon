package conviva.surgeon


class PbSSCoreLib_Suite extends munit.FunSuite {

  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._
  import conviva.surgeon.PbSS._
  import conviva.surgeon.PbSSCoreLib._
  import conviva.surgeon.GeoInfo._
  import conviva.surgeon.Paths._
  

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  val custMap = getGeoData("customer", PathDB.testPath)
  val path = PbSS.prodHourly(year=2023, month=2, day=7, hour=2, root = PathDB.testPath + "pbss")
  val pbssPath = Cust(path, names = "c3.TopServe", custMap)
  val dat = spark.read.parquet(pbssPath).cache
  val d8905 = dat.where(sessionId === 89057425)
    .withColumn("sessionAdId", lit(200500))

  test("Core metrics should be expected") {
    val t1 = d8905.select(isEBVS).first.getBoolean(0)
    assertEquals(t1, false)
    val t2 = d8905.select(isVPF).first.getBoolean(0)
    assertEquals(t2, false)
    val t3 = d8905.select(isVSF).first.getBoolean(0)
    assertEquals(t3, false)
    val t4 = d8905.select(hasJoined).first.getBoolean(0)
    assertEquals(t4, true)
    val t5 = d8905.select(lifeAvgBitrateKbps).first.getDouble(0)
    assertEquals(t5, 5807.0)
    val t6 = d8905.select(firstHbTimeMs).first.getDouble(0)
    assertEquals(t6, 1.675765693115E12)
    val t7 = d8905.select(intvAvgBitrateKbps).first.getDouble(0)
    assertEquals(t7, 5806.0)
    val t8 = d8905.select(intvBufferingTimeMs).first.getDouble(0)
    assertEquals(t8, 2375.0)
    val t9 = d8905.select(intvPlayingTimeMs).first.getDouble(0)
    assertEquals(t9, 1742812.0)
  }
  

}
