package conviva.surgeon

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import conviva.surgeon.PbSS._
import conviva.surgeon.Sanitize._
import conviva.surgeon.Donor._

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()
  }
}

class DataSuite extends munit.FunSuite
    with SparkSessionTestWrapper  {

  /** Helper function to test time fields. */ 
  def testTimeIsMs(dat: DataFrame, field: TimeMsCol, 
    expectMs: Long): Unit = {
    val expectSec: Long = (expectMs * (1.0/1000)).toLong
    val t1 = dat.select(field.asis).collect()
    val t2 = dat.select(field.ms).collect()
    val t3 = dat.select(field.sec).collect()
    assertEquals(t1(0)(0), expectMs)
    assertEquals(t2(0)(0), expectMs)
    assertEquals(t3(0)(0), expectSec)
  }

  // test("Customer nrow should be expected") {
  //   val nrow = geoUtilCustomer.count.toInt
  //   assertEquals(nrow, 4)
  // }

  val dat = spark.read.parquet("./common/src/test/data/pbssHourly1.parquet")
    .cache
  val d8905 = dat.where(col("key.sessId.clientSessionId") === 89057425)

  test("Data nrow should be expected") {
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

  test("sid6 should eq signed ID str") {
    val expect = "476230728:1293028608:-1508640558:-1180571212:89057425:1675765692087"
    val t1 = d8905.select(sid6.signed)
      .collect()
    assertEquals(t1(0)(0).toString, expect)
  }

  test("sessionCreationId should eq expected") {
    val expect = "2b6b36b7"
    val t1 = d8905.select(sessionCreationId.hex).collect()
    assertEquals(t1(0)(0), expect)
  }

  test("sid6 should eq hex ID str") {
    val expect = "1c62b448:4d120d00:a613f8d2:b9a1e9b4:54ee891:2b6b36b7"
    val t1 = d8905.select(sid6.hex)
      .collect()
    assertEquals(t1(0)(0).toString, expect)
  }

  test("ad session id should eq chosen session") {
    val expect: Any = 89057425
    val t1 = d8905.select(sessionId.asis)
      .collect()
    assertEquals(t1(0)(0), expect)
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

  test("Select should include all ID field names") {
    val expect = "customerId:clientId:sessionId:sid5Signed:sid5Unsigned"
    val tnames = d8905
      .select(
        customerId, clientId.asis, sessionId.asis,
        sid5.signed, sid5.unsigned)
      .columns.mkString(":")
    assertEquals(tnames, expect)
  }

  test("Select should include intvStartTime fields") {
    val expect = "intvStartTime:intvStartTimeMs:intvStartTimeSec:intvStartTimeStamp"
    val tnames = d8905
      .select(intvStartTime.asis, intvStartTime.ms,
        intvStartTime.sec, intvStartTime.stamp)
      .columns.mkString(":")
    assertEquals(tnames, expect)
  }

  test("Select should include joinTime fields") {
    val expect = "joinTime:joinTimeMs:joinTimeSec:joinTimeStamp"
    val tnames = d8905
      .select(joinTime.asis, joinTime.ms,
        joinTime.sec, joinTime.stamp)
      .columns.mkString(":")
    assertEquals(tnames, expect)
  }

  // test("Should equal contentSession") {
  //   val t1 = d8905.select(isAd).distinct
  //     .collect()(0)(0).toString
  //   assertEquals(t1, "contentSession")
  // }

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

  test("sessionTimeMs should compute ms/sec") {
    val t1 = d8905.select(sessionTimeMs).collect()
    assertEquals(t1(0)(0), 1906885L)
  }

  test("lifeBufferingTime should compute ms/sec") {
    testTimeIsMs(d8905, lifeBufferingTime, 2375L)
  }

  test("sessionCreationTime should compute ms/sec") {
    testTimeIsMs(d8905, sessionCreationTime, 1675765692087L)
  }

  test("isConsistent should eq expected values") {
    val t1 = d8905.select(isConsistent()).collect()
    assertEquals(t1(0)(0), 1)
  }

  test("isConsistent should eq expected table") {
    val d1 = dat.select(isJoinTime, isLifePlayingTime, shouldProcess, joinState)
      .groupBy(col("shouldProcess"), col("isJoinTime"), col("joinState"), col("isLifePlayingTime"))
      .agg(count("*").as("sessCnt"))
      .withColumn("isConsistent", isConsistent(col("isJoinTime"), col("joinState"), col("isLifePlayingTime")))
      .sort(col("shouldProcess"), col("isJoinTime"), col("joinState"), col("isLifePlayingTime"))
    val t1 = d1.select(col("sessCnt"))
      .where(col("shouldProcess") === false).collect()
    assertEquals(t1(0)(0), 4)
  }

  test("isLive should eq expected") {
    val t1 = d8905.select(c3VideoIsLive).collect()
    assertEquals(t1(0)(0), "F")
  }

}

