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

  PathDB.geoUtilPath = PathDB.testPath 

  test("ServiceConfig data is expected") {
    assertEquals(getGeoData("customer")(1960180360), "c3.TopServe")
  }

  test("Customer list is expected") {
    val t1 = getGeoData("customer").filter(x => x._2 == "c3.TopServe").map(_._1).toList
    assertEquals(t1, List(1960180360))
  }

  test("c3NameToId is expected") {
    val t1 = c3NameToId("c3.TopServe")
    val t2 = c3NameToId("c3.PlayFoot", "c3.TopServe")
    assertEquals(t1, List(1960180360))
    assertEquals(t2, List(1960002004, 1960180360))
  }

  test("c3IdToName is expected") {
    val t1 = c3IdToName(1960180360)
    val t2 = c3IdToName(1960002004, 1960180360)
    assertEquals(t1, Seq("c3.TopServe"))
    assertEquals(t2, Seq("c3.PlayFoot", "c3.TopServe"))
  }

  // test("Customer take n is expected") {
  //   val t1 = customerIds(Monthly(2023, 2), ).take(3).length
  //   assertEquals(t1, 3)
  // }

}

