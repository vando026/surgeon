package conviva.surgeon

import conviva.surgeon.Sanitize._
// import conviva.surgeon.Paths._
// import conviva.surgeon.Customer._
// import conviva.surgeon.Heart._
// import org.apache.spark.sql.functions.{col}
// import org.apache.spark.sql.{SparkSession}

class SanitizeSuite extends munit.FunSuite { 

  // val spark = SparkSession
  //     .builder()
  //     .master("local[*]")
  //     .getOrCreate()

  test("Unsigned to BigInt") {
    val t1 = toUnsigned(-1)
    val t2 = BigInt(4294967295L)
    assertEquals(t1, t2) 
  }

}
