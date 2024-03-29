package conviva.surgeon

class PbSS_Suite extends munit.FunSuite {

  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._
  import conviva.surgeon.PbSS._
  import conviva.surgeon.Sanitize._
  import conviva.surgeon.GeoInfo._
  import conviva.surgeon.PbSSCoreLib._
  import conviva.surgeon.Paths._

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  PathDB.geoUtilPath = PathDB.testPath 
  PathDB.root = PathDB.testPath
  PathDB.pbssHourly = "pbss"

  val path = Path.pbss("2023-02-07T02").cust("c3.TopServe").toList
  val dat = spark.read.parquet(path:_*).cache
  val d8905 = dat.where(sessionId === 89057425)
    .withColumn("sessionAdId", lit(200500))

  /** Helper function to test time fields. */ 
  def testTimeIsMs(dat: DataFrame, field: TimeMsCol, 
    expectMs: Long): Unit = {
    val expectSec: Long = (expectMs * (1.0/1000)).toLong
    val t1 = dat.select(field).first.getLong(0)
    val t2 = dat.select(field.toSec).first.getLong(0)
    assertEquals(t1, expectMs)
    assertEquals(t2, expectSec)
  }


  test("Data nrow should be expected") {
    val nrow = dat.count.toInt
    assertEquals(nrow, 10)
  }

  test("CustomerName should be expected") {
    val cdat = d8905.select(customerName).first.getString(0)
    assertEquals(cdat, "c3.TopServe")
  }

  test("sessionId should eq chosen session") {
    val expect: String = "89057425"
    val t1 = d8905.select(sessionId).first.getInt(0).toString
    assertEquals(t1, expect)
  }

  test("clientId should eq signed and asis ID str") {
    val expect = "476230728:1293028608:-1508640558:-1180571212"
    val t1 = d8905.select(clientId.concat).first.getString(0)
    assertEquals(t1, expect)
  }

  test("clientId should eq unsigned ID str") {
    val expect = "476230728:1293028608:2786326738:3114396084"
    val t1 = d8905.select(clientId.concatToUnsigned).first.getString(0)
    assertEquals(t1, expect)
  }

  test("sid5 should eq concatToHex ID str") {
    val expect = "1c62b448:4d120d00:a613f8d2:b9a1e9b4:54ee891"
    val t1 = d8905.select(sid5.concatToHex).first.getString(0)
    assertEquals(t1, expect)
  }

  test("sid5 should eq unsigned ID str") {
    val expect = "476230728:1293028608:2786326738:3114396084:89057425"
    val t1 = d8905.select(sid5.concatToUnsigned).first.getString(0)
    assertEquals(t1, expect)
  }

  test("sid5 should eq asis ID str") {
    val expect = "476230728:1293028608:-1508640558:-1180571212:89057425"
    val t1 = d8905.select(sid5.concat).first.getString(0)
    assertEquals(t1, expect)
  }


  test("sid6 should eq asis ID str") {
    val expect = "476230728:1293028608:-1508640558:-1180571212:89057425:1675765692087"
    val t1 = d8905.select(sid6.concat).first.getString(0)
    assertEquals(t1, expect)
  }

  test("sessionCreationId to Hex should eq expected") {
    val expect = "2b6b36b7"
    val t1 = d8905.select(sessionCreationId.toHex).first.getString(0)
    assertEquals(t1, expect)
  }

  test("sid6 should eq concatToHex ID str") {
    val expect = "1c62b448:4d120d00:a613f8d2:b9a1e9b4:54ee891:2b6b36b7"
    val t1 = d8905.select(sid6.concatToHex).first.getString(0)
    assertEquals(t1, expect)
  }

  test("Select should include all ID field names") {
    val expect = "customerId:clientId:sessionId:sid5:sid5Unsigned:sid5Hex"
    val tnames = d8905
      .select(customerId, clientId.concat, sessionId, sid5.concat, sid5.concatToUnsigned, sid5.concatToHex)
        .columns.mkString(":")
    assertEquals(tnames, expect)
  }

  test("intvStartTimeSec should compute ms/sec") {
    val t1 = d8905.select(intvStartTime).first.getInt(0)
    val t2 = d8905.select(intvStartTime.toMs).first.getLong(0)
    assertEquals(t1, 1675764000)
    assertEquals(t2, 1675764000L * 1000)
  }

  test("Select should include intvStartTime fields") {
    val expect = "intvStartTimeSec:intvStartTimeMs:intvStartTimeStamp"
    val tnames = d8905
      .select(intvStartTime, intvStartTime.toMs, intvStartTime.stamp)
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

  test("geoInfo select and label should work as expected") {
    val gcol = geoInfo("city", Some(Map(289024 -> "Epernay")))
    val gcol2 = geoInfo("country", Some(Map(165 -> "Norway")))
    val t1 = d8905.select(gcol, gcol.label())
    val t2 = d8905.select(gcol2, gcol2.label())
    assertEquals(t1.select("city").first.getInt(0), 289024)
    assertEquals(t1.select("cityLabel").first.getString(0), "Epernay")
    assertEquals(t2.select("country").first.getShort(0).toInt, 165)
    assertEquals(t2.select("countryLabel").first.getString(0), "Norway")
  }

  test("customerName should work") {
    def customerName(): Column = {
      val gMap = getGeoData("customer")
      val gLit: Column = typedLit(gMap) 
      gLit(customerId).alias(s"customerName")
    }
    val t1 = d8905.select(customerName).first.getString(0)
    assertEquals(t1, "c3.TopServe")
  }

 // for documentation

 test("README example should work") {
 val dat2 = dat.select(
    customerId, 
    sessionId, 
    sid5.concatToHex, 
    intvStartTime.stamp,
    lifeFirstRecvTime.stamp, 
    sumTags("c3.viewer.id"),
    sumTags("c3.video.isAd"),
    lifeFirstRecvTime, 
    hasEnded, 
    justJoined, 
    lifePlayingTimeMs, 
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
    geoInfo("city", Some(Map(1 -> "Test"))),
  )
 }


 test("select Ids should run without issues") {
  val dat2 = dat.select(
    customerId,
    clientId,             // returns as is, signed array
    clientId.concat,        // returns as asis String
    clientId.concatToUnsigned,      // returns unsigned String
    clientId.concatToHex,         // returns hexadecimal String
    sessionId,            // signed
    sessionId.toUnsigned,     // unsigned 
    sessionId.toHex,        // hexadecimal String
    sessionAdId,          // (c3.csid) array
    sessionAdId.toUnsigned,   // (c3.csid) unsigned
    sessionAdId.toHex,      // (c3.csid) unsigned
    sid5.concat,            // clientId:sessionId asis String
    sid5.concatToUnsigned,          // clientId:sessionId unsigned String
    sid5.concatToHex,             // clientId:sessionId hexadecimal String
    sid5Ad.concat,          // clientAdId:sessionId asis String
    sid5Ad.concatToUnsigned,        // clientAdId:sessionId unsigned String
    sid5Ad.concatToHex,           // clientAdId:sessionId hexadecimal String
    sid6.concat,                  // clientAdId:sessionId:sessionCreationTime asis String
    sid6.concatToUnsigned,        // clientAdId:sessionId:sessionCreationTime unsigned String
    sid6.concatToHex              // clientAdId:sessionId:sessionCreationTime hexadecimal String
  )
 }



 test("select time cols and methods should run without issues") {
  val dat2 = dat.select(
  lifeFirstRecvTime, // as is, ms since unix epoch
  lifeFirstRecvTime.toSec, // converts ms to seconds since unix epoch
  lifeFirstRecvTime.stamp, // converts ms since unix epoc to timestamp
  lifeFirstRecvTime, 
  lifeFirstRecvTime.toSec,  
  lifeFirstRecvTime.stamp,
  lastRecvTime, 
  lastRecvTime.toSec,  
  lastRecvTime.stamp,
  sessionCreationTime,
  sessionCreationTime.toSec,
  sessionCreationTime.stamp,
  intvStartTime, // as is, seconds since unix epoch
  intvStartTime.toMs, // converts seconds to ms since unix epoch
  intvStartTime.stamp
  )
 }

  d8905.select(
    lifeFirstRecvTime,                 // its original form, milliseconds since unix epoch
    lifeFirstRecvTime.toSec,           // converted to seconds since unix epoch
    lifeFirstRecvTime.stamp,           // as a timestamp (HH:mm:ss)
    dayofweek(lifeFirstRecvTime.stamp).alias("dow"),// get the day of the week (Spark method)
    hour(lifeFirstRecvTime.stamp).alias("hour")      // get hour of the time (Spark method)
    ).show(false)


}

