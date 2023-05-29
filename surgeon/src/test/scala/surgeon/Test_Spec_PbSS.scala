package conviva.surgeon

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import conviva.surgeon.PbSS._
import conviva.surgeon.Sanitize._


class PbSS_Suite extends munit.FunSuite {

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
  val pbssTestPath = "./src/test/data" 
  val dat = spark.read.parquet(s"${pbssTestPath}/pbssHourly1.parquet").cache
  val d8905 = dat.where(col("key.sessId.clientSessionId") === 89057425)
    .withColumn("clientAdId", lit(200500))

  /** Helper function to test time fields. */ 
  def testTimeIsMs(dat: DataFrame, field: TimeMsCol, 
    expectMs: Long): Unit = {
    val expectSec: Long = (expectMs * (1.0/1000)).toLong
    val t1 = dat.select(field).first.getLong(0)
    val t2 = dat.select(field.sec).first.getLong(0)
    assertEquals(t1, expectMs)
    assertEquals(t2, expectSec)
  }

  test("Data nrow should be expected") {
    val nrow = dat.count.toInt
    assertEquals(nrow, 10)
  }

  test("sessionId should eq chosen session") {
    val expect: String = "89057425"
    val t1 = d8905.select(sessionId).first.getInt(0).toString
    assertEquals(t1, expect)
  }

  test("clientId should eq signed and asis ID str") {
    val expect = "476230728:1293028608:-1508640558:-1180571212"
    val t1 = d8905.select(clientId.asis).first.getString(0)
    assertEquals(t1, expect)
  }

  test("clientId should eq unsigned ID str") {
    val expect = "476230728:1293028608:2786326738:3114396084"
    val t1 = d8905.select(clientId.nosign).first.getString(0)
    assertEquals(t1, expect)
  }

  test("sid5 should eq hex ID str") {
    val expect = "1c62b448:4d120d00:a613f8d2:b9a1e9b4:54ee891"
    val t1 = d8905.select(sid5.hex).first.getString(0)
    assertEquals(t1, expect)
  }

  test("sid5 should eq unsigned ID str") {
    val expect = "476230728:1293028608:2786326738:3114396084:89057425"
    val t1 = d8905.select(sid5.nosign).first.getString(0)
    assertEquals(t1, expect)
  }

  test("sid5 should eq asis ID str") {
    val expect = "476230728:1293028608:-1508640558:-1180571212:89057425"
    val t1 = d8905.select(sid5.asis).first.getString(0)
    assertEquals(t1, expect)
  }

  test("sid6 should eq asis ID str") {
    val expect = "476230728:1293028608:-1508640558:-1180571212:89057425:1675765692087"
    val t1 = d8905.select(sid6.asis).first.getString(0)
    assertEquals(t1, expect)
  }

  test("sessionCreationId should eq expected") {
    val expect = "2b6b36b7"
    val t1 = d8905.select(sessionCreationId.hex).first.getString(0)
    assertEquals(t1, expect)
  }

  test("sid6 should eq hex ID str") {
    val expect = "1c62b448:4d120d00:a613f8d2:b9a1e9b4:54ee891:2b6b36b7"
    val t1 = d8905.select(sid6.hex).first.getString(0)
    assertEquals(t1, expect)
  }

  test("Select should include all ID field names") {
    val expect = "customerId:clientId:clientSessionId:sid5:sid5NoSign:sid5Hex"
    val tnames = d8905
      .select(customerId, clientId.asis, sessionId, sid5.asis, sid5.nosign, sid5.hex)
        .columns.mkString(":")
    assertEquals(tnames, expect)
  }

  test("intvStartTimeSec should compute ms/sec") {
    val t1 = d8905.select(intvStartTime).first.getInt(0)
    val t2 = d8905.select(intvStartTime.ms).first.getLong(0)
    assertEquals(t1, 1675764000)
    assertEquals(t2, 1675764000L * 1000)
  }

  test("Select should include intvStartTime fields") {
    val expect = "intvStartTimeSec:intvStartTimeMs:intvStartTimeStamp"
    val tnames = d8905
      .select(intvStartTime, intvStartTime.ms, intvStartTime.stamp)
      .columns.mkString(":")
    assertEquals(tnames, expect)
  }

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
    val t1 = d8905.select(sessionTimeMs).first.getInt(0)
    assertEquals(t1, 1906885)
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
    val t1 = d8905.select(sumTags("c3.video.isLive"))
      .collect().map(_.getString(0))
    assertEquals(t1(0), "F")
  }

 test("sumTag c3.video.isAd should be expected") {
   val t1 = d8905.select(sumTags("c3.video.isAd")) 
    .collect.map(_.getString(0))
   assertEquals(t1(0), "F")
 }

 test("invTag publicIP should be expected") {
   val t1 = d8905.select(invTags("publicIp")) 
    .collect.map(_.getInt(0))
   assertEquals(t1(0), 0)
 }

 test("lifeSwitchInfo.sessionTimeMsSum should eq expected ") {
  val t1 = d8905.select(lifeSwitch("sessionTimeMs").sumInt)
  assertEquals(t1.first.getInt(0), 1906885)
 }

 test("lifeSwitchInfo.playingTimeMsSum should eq expected ") {
  val t1 = d8905.select(lifeSwitch("playingTimeMs").sumInt)
  assertEquals(t1.first.getInt(0), 1742812)
 }

 test("lifeSwitchInfo.playingTimeMsFirst should eq expected ") {
  val t1 = d8905.select(lifeSwitch("playingTimeMs").first)
  assertEquals(t1.first.getInt(0), 1742812)
 }

 // for documentation

 test("README example should work") {
 val dat2 = dat.select(
    customerId, 
    sessionId, 
    sid5.hex, 
    intvStartTime.stamp,
    lifeFirstRecvTime.stamp, 
    sumTags("c3.viewer.id"),
    sumTags("c3.video.isAd"),
    lifeFirstRecvTime, 
    hasEnded, 
    justJoined, 
    sessSum("lifePlayingTimeMs"), 
    lifeFirstRecvTime, 
    endedStatus, 
    shouldProcess, 
    intvStartTime
  )
 }

 test("select containers cols should run without issues") {
  val dat2 = dat.select(
    sessSum("playerState"), 
    d3SessSum("lifePausedTimeMs"),
    joinSwitch("playingTimeMs"),
    lifeSwitch("sessionTimeMs"),
    intvSwitch("networkBufferingTimeMs"), 
    invTags("sessionCreationTimeMs"), 
    sumTags("c3.video.isAd"), 
  )
 }


 test("select Ids should run without issues") {
  val dat2 = dat.select(
    customerId,
    clientId,             // returns as is, signed array
    clientId.asis,        // returns as signed String
    clientId.nosign,      // returns unsigned String
    clientId.hex,         // returns hexadecimal String
    sessionId,            // signed
    sessionId.nosign,     // unsigned 
    sessionId.hex,        // hexadecimal String
    sessionAdId,          // (c3.csid) signed array
    sessionAdId.nosign,   // (c3.csid) unsigned
    sessionAdId.hex,      // (c3.csid) unsigned
    sid5.asis,            // clientId:sessionId signed String
    sid5.nosign,          // clientId:sessionId unsigned String
    sid5.hex,             // clientId:sessionId hexadecimal String
    sid5Ad.asis,          // clientAdId:sessionId signed String
    sid5Ad.nosign,        // clientAdId:sessionId unsigned String
    sid5Ad.hex,           // clientAdId:sessionId hexadecimal String
    sid6.asis,            // clientAdId:sessionId:sessionCreationTime signed String
    sid6.nosign,          // clientAdId:sessionId:sessionCreationTime unsigned String
    sid6.hex              // clientAdId:sessionId:sessionCreationTime hexadecimal String
  )
 }



 test("select time cols and methods should run without issues") {
  val dat2 = dat.select(
  lifeFirstRecvTime, // as is, ms since unix epoch
  lifeFirstRecvTime.sec, // converts ms to seconds since unix epoch
  lifeFirstRecvTime.stamp, // converts ms since unix epoc to timestamp
  lifeFirstRecvTime, 
  lifeFirstRecvTime.sec,  
  lifeFirstRecvTime.stamp,
  lastRecvTime, 
  lastRecvTime.sec,  
  lastRecvTime.stamp,
  sessionCreationTime,
  sessionCreationTime.sec,
  sessionCreationTime.stamp,
  intvStartTime, // as is, seconds since unix epoch
  intvStartTime.ms, // converts seconds to ms since unix epoch
  intvStartTime.stamp
  )
 }





}
