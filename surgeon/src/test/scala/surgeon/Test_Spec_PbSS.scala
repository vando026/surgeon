package conviva.surgeon

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import conviva.surgeon.PbSS._
import conviva.surgeon.Sanitize._
import conviva.surgeon.Customer._
import conviva.surgeon.Heart._


class PbSS_Suite extends munit.FunSuite {

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

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

  val pbssTestPath = "./src/test/data" 
  val dat = spark.read.parquet(s"${pbssTestPath}/pbssHourly1.parquet")
    .cache
  val d8905 = dat.where(col("key.sessId.clientSessionId") === 89057425)

  test("Data nrow should be expected") {
    val nrow = dat.count.toInt
    assertEquals(nrow, 10)
  }

  test("sessionId should eq chosen session") {
    val expect: String = "89057425"
    val t1 = d8905.select(sessionId.asis)
      .collect()(0)(0).toString
    assertEquals(t1, expect)
  }

  test("clientId should eq signed and asis ID str") {
    val expect = "476230728:1293028608:-1508640558:-1180571212"
    val t1 = d8905.select(clientId.signed).collect()
    val t2 = d8905.select(clientId.asis).collect()
    assertEquals(t1(0)(0).toString, expect)
    assertEquals(t2(0)(0).toString, expect)
  }

  test("clientId should eq unsigned ID str") {
    val expect = "476230728:1293028608:2786326738:3114396084"
    val t1 = d8905.select(clientId.unsigned)
      .collect()(0)(0).toString
    assertEquals(t1.toString, expect)
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

  test("sid5 should eq signed and asis ID str") {
    val expect = "476230728:1293028608:-1508640558:-1180571212:89057425"
    val t1 = d8905.select(sid5.signed).collect()
    val t2 = d8905.select(sid5.asis).collect()
    assertEquals(t1(0)(0).toString, expect)
    assertEquals(t2(0)(0).toString, expect)
  }

  test("sid6 should eq signed and asis ID str") {
    val expect = "476230728:1293028608:-1508640558:-1180571212:89057425:1675765692087"
    val t1 = d8905.select(sid6.signed).collect()
    val t2 = d8905.select(sid6.asis).collect()
    assertEquals(t1(0)(0).toString, expect)
    assertEquals(t2(0)(0).toString, expect)
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
    val expect = "89057425"
    val t1 = d8905.select(sessionId.asis)
      .collect()(0)(0).toString
    assertEquals(t1, expect)
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

  // This has changed select another
  // test("Select should include lifePlayingTime fields") {
  //   val expect = "lifePlayingTimeMs:lifePlayingTimeSec:lifePlayingTimeStamp"
  //   val tnames = d8905
  //     .select(lifePlayingTime.ms, lifePlayingTime.sec, lifePlayingTime.stamp)
  //     .columns.mkString(":")
  //   assertEquals(tnames, expect)
  // }

  // test("Should equal contentSession") {
  //   val t1 = d8905.select(isAd).distinct
  //     .collect()(0)(0).toString
  //   assertEquals(t1, "contentSession")
  // }

  test("lifeFirstRecvTime should compute sec/ms") {
    testTimeIsMs(d8905, lifeFirstRecvTime, 1675765693115L)
  }

  test("lifePlayingTime should be expected") {
    val t1 = d8905.select(lifePlayingTimeMs).first.getInt(0)
    assertEquals(t1, 1742812)
  }

  test("lastRecvTime should compute ms/sec") {
    testTimeIsMs(d8905, lastRecvTime, 1675767600000L)
  }

  test("sessionTimeMs should compute ms/sec") {
    val t1 = d8905.select(sessionTimeMs).collect()
    assertEquals(t1(0)(0), 1906885L)
  }

  test("lifeBufferingTime should compute") {
    val t1 = d8905.select(lifeBufferingTimeMs).first.getInt(0)
    assertEquals(t1, 2375)
  }

  test("sessionCreationTime should compute ms/sec") {
    testTimeIsMs(d8905, sessionCreationTime, 1675765692087L)
  }

  test("isConsistent should eq expected values") {
    val t1 = d8905.select(isConsistent()).collect()
    assertEquals(t1(0)(0), 1)
  }

  // test("isConsistent should eq expected table") {
  //   val d1 = dat.select(isJoinTime, isLifePlayingTime, shouldProcess, joinState)
  //     .groupBy(col("shouldProcess"), col("isJoinTime"), col("joinState"), col("isLifePlayingTime"))
  //     .agg(count("*").as("sessCnt"))
  //     .withColumn("isConsistent", isConsistent(col("isJoinTime"), col("joinState"), col("isLifePlayingTime")))
  //     .sort(col("shouldProcess"), col("isJoinTime"), col("joinState"), col("isLifePlayingTime"))
  //   val t1 = d1.select(col("sessCnt"))
  //     .where(col("shouldProcess") === false).collect()
  //   assertEquals(t1(0)(0), 4)
  // }

  test("sumTag video.isLive should eq expected") {
    val t1 = d8905.select(sumTag("c3.video.isLive"))
      .collect().map(_.getString(0))
    assertEquals(t1(0), "F")
  }

 test("sumTag c3.video.isAd should be expected") {
   val t1 = d8905.select(sumTag("c3.video.isAd")) 
    .collect.map(_.getString(0))
   assertEquals(t1(0), "F")
 }

 test("invTag publicIP should be expected") {
   val t1 = d8905.select(invTag("publicIp")) 
    .collect.map(_.getInt(0))
   assertEquals(t1(0), 0)
 }


trait TestMe {
  def field: Column
  def rename(s: String): Column = field.alias(s)
}

val h = "payload.heartbeat.pbSdmEvents.cwsPlayerMeasurementEvent"
class CWSPlayer(name: String) extends Column(s"$h.$name") {
  def rename(s: String): Column = this.alias(s)
}
def CWS(s: String): CWSPlayer = {
  new CWSPlayer(s) 
}

class SS(name: String) extends Column(name) {
  def rename(s: String): Column = this.alias(s)
}
def SSMake(s: String): SS = {
  new SS(s) 
}
val tt = SSMake("val.sessSummary.shouldProcess")


  class ArrayCol2(name: String) extends Column(name) {
    // def rename(s: String): Column = this.alias(s)
    val nm = name.split("\\.").last
    def first(): Column = {
        filter(this, x => x.isNotNull)(0).alias(s"${nm}First")
    }
  }
  def joinSwitch(name: String): ArrayCol2 = {
    new ArrayCol2(s"val.sessSummary.joinSwitchInfos.$name")
  }

  dat.select(
    joinSwitch("framesPlayingTimeMs"),
    joinSwitch("framesPlayingTimeMs").first
  ).show

}
