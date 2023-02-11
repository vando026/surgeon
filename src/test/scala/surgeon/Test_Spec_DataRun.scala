package surgeon

import org.apache.spark.sql.{SparkSession, DataFrame}
import conviva.surgeon.PbSS.pbssMethods
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

  // test("toSid5Hex")
  // val sid1 = dat.sid5Hex().life

  test("ParseTimeMs: lifeFirstRecv") {
    val expect: Any = 1675765693115L
    val expect2: Any = 1675765693115L / 1000
    val t1 = d8905.lifeFirstRecvTime.asis
      .select(col("lifeFirstRecvTime")).collect()
    val t2 = d8905.lifeFirstRecvTime.ms
      .select(col("lifeFirstRecvTimeMs")).collect()
    val t3 = d8905.lifeFirstRecvTime.sec
      .select(col("lifeFirstRecvTimeSec")).collect()
    assertEquals(t1(0)(0), expect)
    assertEquals(t2(0)(0), expect)
    assertEquals(t3(0)(0), expect2)
  }

  test("ParseTimeSec: intvStartTimeSec") {
    val expect: Any = 1675764000
    val expect2: Any = 1675764000 * 1000
    val t1 = d8905.intvStartTime.asis
      .select(col("intvStartTime")).collect()
    val t2 = d8905.intvStartTime.ms
      .select(col("intvStartTimeMs")).collect()
    val t3 = d8905.intvStartTime.sec
      .select(col("intvStartTimeSec")).collect()
    assertEquals(t1(0)(0), expect)
    assertEquals(t2(0)(0), expect2)
    assertEquals(t3(0)(0), expect)
  }


}
