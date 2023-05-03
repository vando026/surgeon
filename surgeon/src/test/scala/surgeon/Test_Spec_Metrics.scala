package conviva.surgeon

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import conviva.surgeon.PbSS._
import conviva.surgeon.Sanitize._
import conviva.surgeon.Heart._
import conviva.surgeon.PbSSCoreLib._
import conviva.surgeon.Metrics._

class MetricSuite extends munit.FunSuite {

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  val pbssTestPath = "./src/test/data" 

  // val dat = spark.read.parquet(s"${pbssTestPath}/pbssHourly1.parquet")
    // .cache
  // val d8905 = dat.where(col("key.sessId.clientSessionId") === 89057425)

  // test("hasJoinedUDF should be expected") {
  //   val t1 = d8905.select(hasJoined, justJoined)
  //     .collect
  //   assertEquals(t1(0)(0), true)
  //   assertEquals(t1(0)(1), true)
  // }

  // test("isEBFS and isVSF should be expected") {
  //   val t1 = d8905.select(isEBVS, isVSF)
  //     .collect
  //   assertEquals(t1(0)(0), false)
  //   assertEquals(t1(0)(1), false)
  // }

}
