package surgeon

import org.apache.spark.sql.{SparkSession, DataFrame}

class DataSuite extends munit.FunSuite {
  val spark = SparkSession.builder
    .master("local[1]")
    .appName("Conviva-Surgeon").getOrCreate()

  println("Hello, world!")
  val dat = spark.read.parquet("./data/pbssHourly1.parquet")

  test("Check data nrow") {
    val nrow = dat.count.toInt
    assertEquals(nrow, 10)
  }

}
