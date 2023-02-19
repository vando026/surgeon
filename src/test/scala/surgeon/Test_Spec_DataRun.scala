package surgeon

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions.{col, when, from_unixtime, lit}
import conviva.surgeon.PbSS._
import conviva.surgeon.Sanitize._

class DataSuite extends munit.FunSuite {

  /** Helper function to test time fields. */ 
  def testTimeIsMs(dat: DataFrame, field: ExtractColMs, 
    expectMs: Long): Unit = {
    val expectSec: Long = (expectMs * (1.0/1000)).toLong
    val t1 = dat.select(field.asis).collect()
    val t2 = dat.select(field.ms).collect()
    val t3 = dat.select(field.sec).collect()
    assertEquals(t1(0)(0), expectMs)
    assertEquals(t2(0)(0), expectMs)
    assertEquals(t3(0)(0), expectSec)
  }

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("Conviva-Surgeon").getOrCreate()

  val dat = spark.read.parquet("./data/pbssHourly1.parquet")
    .cache
  val d8905 = dat.where(col("key.sessId.clientSessionId") === 89057425)

  test("Check test data nrow") {
    val nrow = dat.count.toInt
    assertEquals(nrow, 10)
  }

  test("sessionId should eq chosen session") {
    val expect: Any = 89057425
    val t1 = d8905.select(sessionId.asis)
      .collect()
    assertEquals(t1(0)(0), expect)
  }

  test("clientId should eq signed ID str") {
    val expect = "476230728:1293028608:-1508640558:-1180571212"
    val t1 = d8905.select(clientId.signed)
      .collect()
    assertEquals(t1(0)(0).toString, expect)
  }

  test("clientId should eq unsigned ID str") {
    val expect = "476230728:1293028608:2786326738:3114396084"
    val t1 = d8905.select(clientId.unsigned)
      .collect()
    assertEquals(t1(0)(0).toString, expect)
  }

  test("sid5 should eq hex ID str") {
    val expect = "1c62b448:4d120d00:a613f8d2:b9a1e9b4:54ee891"
    val t1 = d8905.select(sid5.hex)
      .collect()
    assertEquals(t1(0)(0).toString, expect)
  }

  test("sid5 should eq unsigned ID str") {
    val expect = "476230728:1293028608:2786326738:3114396084:89057425"
    val t1 = d8905.select(sid5.unsigned)
      .collect()
    assertEquals(t1(0)(0).toString, expect)
  }

  test("sid5 should eq signed ID str") {
    val expect = "476230728:1293028608:-1508640558:-1180571212:89057425"
    val t1 = d8905.select(sid5.signed)
      .collect()
    assertEquals(t1(0)(0).toString, expect)
  }

  test("intvStartTimeSec should compute ms/sec") {
    val expect: Any = 1675764000L
    val expect2: Any = 1675764000L * 1000
    val t1 = d8905.select(intvStartTime.asis)
      .collect()
    val t2 = d8905.select(intvStartTime.ms)
      .collect()
    val t3 = d8905.select(intvStartTime.sec)
      .collect()
    assertEquals(t1(0)(0), expect)
    assertEquals(t2(0)(0), expect2)
    assertEquals(t3(0)(0), expect)
  }

  test("Select should include all field names") {
    val expect = "customerId:clientSessionId:sid5Signed:intvStartTimeSec"
    val tnames = d8905
      .select(
        customerId.asis, sessionId.asis,
        sid5.signed, intvStartTime.sec)
      .columns.mkString(":")
    assertEquals(tnames, expect)
  }

  test("Should equal contentSession") {
    val t1 = d8905.select(isAd).distinct
      .collect()(0)(0).toString
    assertEquals(t1, "contentSession")
  }

  test("lifeFirstRecvTime should compute sec/ms") {
    testTimeIsMs(d8905, lifeFirstRecvTime, 1675765693115L)
  }

  test("lifePlayingTime should compute ms/sec") {
    testTimeIsMs(d8905, lifePlayingTime, 1742812L)
  }

  test("joinTime should compute ms/sec") {
    testTimeIsMs(d8905, joinTime, 4978L)
  }

  test("lastRecvTime should compute ms/sec") {
    testTimeIsMs(d8905, lastRecvTime, 1675767600000L)
  }

  test("sessionTime should compute ms/sec") {
    testTimeIsMs(d8905, sessionTime, 1906885L)
  }

  test("lifeBufferingTime should compute ms/sec") {
    testTimeIsMs(d8905, lifeBufferingTime, 2375L)
  }

}
