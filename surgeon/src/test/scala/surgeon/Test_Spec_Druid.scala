package conviva.surgeon

class DruidSuite extends munit.FunSuite {

  import conviva.surgeon.Druid._
  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  val dataPath = "./src/test/data" 

  val dat = readDruid(dataPath + "/druid.json")

  test("Data nrow should be expected") {
    val nrow = dat.count.toInt
    assertEquals(nrow, 3)
  }

  test("Data nrow should be expected") {
    val ncol = dat.columns.length
    assertEquals(ncol, 8)
  }

  test("Col ave should be expected") {
    val t1 = dat.select(round(avg(col("life_encodedFrames")))).first.getDouble(0)
    assertEquals(t1, 10547.0)
  }


}
