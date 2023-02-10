package surgeon

import org.apache.spark.sql.{SparkSession, DataFrame}
import conviva.surgeon.PbSS.pbssMethods

class DataSuite extends munit.FunSuite {

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("Conviva-Surgeon").getOrCreate()

  val dat = spark.read.parquet("./data/pbssHourly1.parquet")
    .cache

  test("Data nrow") {
    val nrow = dat.count.toInt
    assertEquals(nrow, 10)
  }

  // test("toSid5Hex")
  // val sid1 = dat.sid5Hex().life

  test("Parse time: lifeFirstRecv") {
    val tdat = dat.lifeFirstRecvTime.timestamp.show()
  }

}
