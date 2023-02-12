package surgeon

import org.apache.spark.sql.{SparkSession, DataFrame}
import conviva.surgeon.PbSS._
import org.apache.spark.sql.functions.{col, when, from_unixtime, lit}

class DataSuite extends munit.FunSuite {

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("Conviva-Surgeon").getOrCreate()

  val dat = spark.read.parquet("./data/pbssHourly1.parquet")
    .cache
  val d8905 = dat.where(col("key.sessId.clientSessionId") === 89057425)

  test("Data nrow") {
    val nrow = dat.count.toInt
    assertEquals(nrow, 10)
  }

  test("Extract: sessionId") {
    val expect: Any = 89057425
    val t1 = d8905.select(sessionId.asis)
      .collect()
    assertEquals(t1(0)(0), expect)
  }

  test("Extract: clientId") {
    val expect: Any = "476230728:1293028608:-1508640558:-1180571212"
    val t1 = d8905.select(clientIdUnsigned)
      .collect()
    assertEquals(t1(0)(0), expect)
  }


  test("ParseTimeMs: lifeFirstRecv") {
    val expect: Any = 1675765693115L
    val expect2: Any = 1675765693115L / 1000
    val t1 = d8905.select(lifeFirstRecvTime.asis)
      .collect()
    val t2 = d8905.select(lifeFirstRecvTime.ms)
      .collect()
    val t3 = d8905.select(lifeFirstRecvTime.sec)
      .collect()
    assertEquals(t1(0)(0), expect)
    assertEquals(t2(0)(0), expect)
    assertEquals(t3(0)(0), expect2)
  }

  test("ParseTimeSec: intvStartTimeSec") {
    val expect: Any = 1675764000
    val expect2: Any = 1675764000 * 1000
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


}
