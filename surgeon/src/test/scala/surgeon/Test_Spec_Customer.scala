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


  val c3 = C3(TestPbSS())
  def pbss(date: String) = SurgeonPath(TestPbSS()).make(date)
  val p1 = pbss("2023-02-07T02")

  test("ServiceConfig data is expected") {
    assertEquals(GetGeoData(TestPbSS().geoUtilPath).data("customer")(1960180360), "c3.TopServe")
  }

  test("Customer list is expected") {
    val t1 = GetGeoData(TestPbSS().geoUtilPath).data("customer")
      .filter(x => x._2 == "c3.TopServe").map(_._1).toList
    assertEquals(t1, List(1960180360))
  }

  test("c3NameToId is expected") {
    val t1 = c3.nameToId("c3.TopServe")
    val t2 = c3.nameToId("c3.PlayFoot", "c3.TopServe")
    assertEquals(t1, List(1960180360))
    assertEquals(t2, List(1960002004, 1960180360))
  }

  test("c3IdToName is expected") {
    val t1 = c3.idToName(1960180360)
    val t2 = c3.idToName(1960002004, 1960180360)
    assertEquals(t1, Seq("c3.TopServe"))
    assertEquals(t2, Seq("c3.PlayFoot", "c3.TopServe"))
  }

  test("Customer take n is expected") {
    val t1 = p1.c3take(3)
    val e1 = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960002004,1960180360,1960181845}"
    assertEquals(t1, e1)
  }

  test("c3 idOnPath is expected") {
    val t1 = c3.idOnPath(p1.toString)
    val e1 = List(1960002004, 1960180360, 1960181845)
    assertEquals(t1, e1)
  }

}

