package conviva.surgeon

class CustomerSuite extends munit.FunSuite { 

  import conviva.surgeon.Sanitize._
  import conviva.surgeon.Paths._
  import conviva.surgeon.Customer._
  import org.apache.spark.sql.functions.{col}
  import org.apache.spark.sql.{SparkSession, Row}
  import conviva.surgeon.GeoInfo._

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  val pbssTestPath = "./surgeon/src/test/data" 
  val custData = getGeoData("customer", pbssTestPath)

  test("ServiceConfig data is expected") {
    assertEquals(custData(1960180360), "c3.TopServe")
  }

  test("Customer list is expected") {
    val t1 = custData.filter(x => x._2 == "c3.TopServe").map(_._1).toList
    assertEquals(t1, List(1960180360))
  }

  test("customerNameToId is expected") {
    val t1 = customerNameToId("c3.TopServe", custData)
    val t2 = customerNameToId(List("c3.PlayFoot", "c3.TopServe"), custData)
    assertEquals(t1, List(1960180360))
    assertEquals(t2, List(1960002004, 1960180360))
  }

  // test("Customer take n is expected") {
  //   val t1 = customerIds(Monthly(2023, 2), ).take(3).length
  //   assertEquals(t1, 3)
  // }

}

