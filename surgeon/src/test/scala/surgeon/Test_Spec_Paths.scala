package conviva.surgeon

class PathSuite extends munit.FunSuite { 

  import conviva.surgeon.Sanitize._
  import conviva.surgeon.Paths._
  import conviva.surgeon.Customer._
  import conviva.surgeon.GeoInfo._
  import org.apache.spark.sql.functions.{col}
  import org.apache.spark.sql.{SparkSession, Row}
  import java.io._
  import org.apache.hadoop.fs._
  import org.apache.hadoop.conf._

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  val pbssTestPath = "./surgeon/src/test/data" 
  val root = "/mnt/conviva-prod-archive-pbss"
  val custData = getGeoData("customer", pbssTestPath)

  test("PbSS.Monthly is expected") {
    val expect1 = s"${PathDB.pbssProd1M}/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00"
    val expect2 = s"${PathDB.pbssProd1M}/y=2022/m=12/dt=c2022_12_01_08_00_to_2023_01_01_08_00/cust={1960180360}"
    val t1 = PbSS.prodMonthly(2023, 2).toString
    val t2 = Cust(PbSS.prodMonthly(2022, 12), names = List("c3.TopServe"), custData)
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
  }

  test("PbSS.Daily is expected") {
    val expect1 = s"${PathDB.pbssProd1d}/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00"
    val expect2 = s"${PathDB.pbssProd1d}/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00/cust={1960180360}"
    val expect3 = s"${PathDB.pbssProd1d}/y=2023/m=02/dt=d2023_02_{22,23}_08_00_to_2023_02_{23,24}_08_00"
    val expect4 = s"${PathDB.pbssProd1d}/y=2023/m=12/dt=d2023_12_31_08_00_to_2024_01_01_08_00"
    val expect5 = s"${PathDB.pbssProd1d}/y=2023/m=10/dt=d2023_10_31_08_00_to_2023_11_01_08_00"
    val t1 = PbSS.prodDaily(2, 22, 2023).toString
    val t2 = Cust(PbSS.prodDaily(2, 22, 2023), names = List("c3.TopServe"), custData)
    val t3 = PbSS.prodDaily(2, List(22,23), 2023).toString
    val t4 = PbSS.prodDaily(12, 31, 2023).toString
    val t5 = PbSS.prodDaily(10, 31, 2023).toString
    val t6 = PbSS.prodDaily(2, List(22, 23), 2023).toList
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
    assertEquals(t3, expect3)
    assertEquals(t4, expect4)
    assertEquals(t5, expect5)
    assertEquals(t6.length, 2)
    assertEquals(t6(0), expect1)
    intercept[java.lang.Exception]{PbSS.prodDaily(10, List(30, 31), 2023).toString}
    intercept[java.lang.Exception]{PbSS.prodDaily(2, List(30), 2023).toString}
    intercept[java.lang.Exception]{PbSS.prodDaily(3, List(32), 2023).toString}
  }

  test("PbSS.Hourly is expected") {
    val expect1 = s"${PathDB.pbssProd1h()}/y=2023/m=02/d=04/dt=2023_02_04_23"
    val expect3 = s"${PathDB.pbssProd1h()}/y=2023/m=02/d=22/dt=2023_02_22_{10,11,12}"
    val expect4 = s"${PathDB.pbssProd1h()}/y=2023/m=02/d={22,23}/dt=2023_02_{22,23}_{10,11,12}"
    val expect5 = s"${PathDB.pbssProd1h()}/y=2023/m=05/d=30/dt=2023_05_30_{00,01,02,03}"
    val expect6 = s"${PathDB.pbssProd1h()}/y=2023/m=05/d=30/dt=2023_05_30_00"
    val t1 = PbSS.prodHourly(2, 4, List(23), 2023).toString
    val t3 = PbSS.prodHourly(2, 22, List(10, 11, 12), 2023).toString
    val t4 = PbSS.prodHourly(2, List(22, 23), List(10, 11, 12), 2023).toString
    val t5 = PbSS.prodHourly(5, 30, List(0, 1, 2, 3), 2023)
    assertEquals(t1, expect1)
    assertEquals(t3, expect3)
    assertEquals(t4, expect4)
    assertEquals(t5.toString, expect5)
    assertEquals(t5.toList.length, 4)
    assertEquals(t5.toList.apply(0), expect6)
    intercept[java.lang.Exception]{PbSS.prodHourly(2, 30, List(22), 2023).toString}
    intercept[java.lang.Exception]{PbSS.prodHourly(3, 2, List(24), 2023).toString}
    intercept[java.lang.Exception]{PbSS.prodHourly(3, 2, 24, 2023).toString}
    // assertEquals(t4, expect4)
  }
  
  test("PbSS.Hourly with customer is expected") {
    val expect1 = s"${PathDB.pbssProd1h()}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360}"
    val expect2 = s"${PathDB.pbssProd1h()}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={*}"
    val expect3 = s"${PathDB.pbssProd1h()}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360,19000200}"
    val expect4 = s"${PathDB.pbssProd1h()}/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00/cust={1960180361,1960180418}"
    val t1 = Cust(PbSS.prodHourly(2, 22, List(23), 2023), ids = List(1960180360))
    val t2 = Cust(PbSS.prodHourly(2, 22, List(23), 2023))
    val t3 = Cust(PbSS.prodHourly(2, 22, List(23), 2023), ids = List(1960180360, 19000200))
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
    assertEquals(t3, expect3)
  }

  test("PbRl.parquet is expected") {
    val expect1 = s"${PathDB.pbrlProd()}/y=2024/m=02/d=01/dt=2024_02_01_00/cust={1960180442}"
    val t1 = Cust(PbRl.prod(2, 1, List(0), 2024), ids = 1960180442)
    assertEquals(t1, expect1)
  }

  // // see getData.scala in ./src/test/data/ for generating these paths
  // val ois = new ObjectInputStream(new java.io.FileInputStream(s"$pbssTestPath/pathsHourly2_24"))
  // val pathx = ois.readObject.asInstanceOf[Array[String]]
  // ois.close


}

