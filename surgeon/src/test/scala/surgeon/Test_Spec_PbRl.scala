package conviva.surgeon

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import conviva.surgeon.PbRl._
import conviva.surgeon.Sanitize._
import conviva.surgeon.Customer._
import conviva.surgeon.Heart._


class PbRl_Suite extends munit.FunSuite {

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  val dataPath = "./src/test/data" 
  val dat = spark.read.parquet(s"${dataPath}/pbrlHourly1.parquet")
    .cache
  // val d8905 = dat.where(col("key.sessId.clientSessionId") === 89057425)

  test("Data nrow should be expected") {
    val nrow = dat.count.toInt
    assertEquals(nrow, 100)
  }

  test("timeStampUs should compute us/ms/sec") {
    val t1 = d8905.select(sessionTimeMs).first.getInt(0)
    assertEquals(t1, 1906885)
  }
}
