package org.surgeon

class PathSuite extends munit.FunSuite { 

  import org.surgeon.Sanitize._
  import org.surgeon.Paths._
  import org.surgeon.Customer._
  import org.surgeon.GeoInfo._
  import org.apache.spark.sql.functions.{col}
  import org.apache.spark.sql.{SparkSession, Row}

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  def pbss(date: String) = SurgeonPath(ProdPbSS()).make(date)

  test("pbssMonth is expected") {
    val mroot  = s"${ProdPbSS().root}/${ProdPbSS().monthly}"
    val expect1 = s"${mroot}/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00"
    val expect2 = s"${mroot}/y=2022/m=12/dt=c2022_12_01_08_00_to_2023_01_01_08_00/cust={1960180360}"
    val expect3 = s"${mroot}/y=2023/m={02,03,04}/dt=c2023_{02,03,04}_01_08_00_to_2023_{03,04,05}_01_08_00"
    val t1 = pbss("2023-02")
    val t2 = pbss("2022-12").c3id(1960180360)
    val t3 = pbss("2023-2")
    val t4 = pbss("2023-{2-4}")
    assertEquals(t1.toString, expect1)
    assertEquals(t2, expect2)
    assertEquals(t3.toString, expect1)
    assertEquals(t4.toString, expect3)
    intercept[java.lang.Exception]{pbss("2023-{11, 12}").toString}
    intercept[java.lang.Exception]{pbss("2023-{14}").toString}
  }

  test("pbssDay is expected") {
    val droot = s"${ProdPbSS().root}/${ProdPbSS().daily}"
    val expect1 = s"${droot}/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00"
    val expect2 = s"${droot}/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00/cust={1960180360}"
    val expect3 = s"${droot}/y=2023/m=02/dt=d2023_02_{22,23}_08_00_to_2023_02_{23,24}_08_00"
    val expect4 = s"${droot}/y=2023/m=12/dt=d2023_12_31_08_00_to_2024_01_01_08_00"
    val expect5 = s"${droot}/y=2023/m=10/dt=d2023_10_31_08_00_to_2023_11_01_08_00"
    val t1 = pbss("2023-02-22")
    val t2 = pbss("2023-02-22").c3id(1960180360)
    val t3 = pbss("2023-02-{22,23}")
    val t4 = pbss("2023-12-31")
    val t5 = pbss("2023-10-31")
    val t6 = pbss("2023-02-{22,23}")
    val t7 = pbss("2023-02-{22, 23}")
    val t8 = pbss("2023-02-{22,   23}")
    assertEquals(t1.toString, expect1)
    assertEquals(t2, expect2)
    assertEquals(t3.toString, expect3)
    assertEquals(t4.toString, expect4)
    assertEquals(t5.toString, expect5)
    assertEquals(t6.toString, expect3)
    assertEquals(t7.toString, expect3)
    assertEquals(t8.toString, expect3)
    intercept[java.lang.Exception]{pbss("2023-03-{34}")}
    intercept[java.lang.Exception]{pbss("2023-12-{30,31}")}
  }


  val hroot = s"${ProdPbSS().root}/${ProdPbSS().hourly}"
  test("PbSS.Hourly is expected") {
    val expect1 = s"${hroot}/y=2023/m=02/d=04/dt=2023_02_04_23"
    val expect2 = s"${hroot}/y=2023/m=02/d=22/dt=2023_02_22_{00,01,02,03}"
    val expect3 = s"${hroot}/y=2023/m=02/d=22/dt=2023_02_22_{10,11,12}"
    val expect4 = s"${hroot}/y=2023/m=02/d=22/dt=2023_02_22_{10,12}"
    val expect5 = s"${hroot}/y=2023/m=05/d=30/dt=2023_05_30_00"
    val t1 = pbss("2023-02-04T23")
    val t2 = pbss("2023-02-22T{0-3}")
    val t3 = pbss("2023-02-22T{10-12}")
    val t4 = pbss("2023-02-22T{10,12}")
    val t5 = pbss("2023-05-30T0")
    assertEquals(t1.toString, expect1)
    assertEquals(t2.toString, expect2)
    assertEquals(t3.toString, expect3)
    assertEquals(t4.toString, expect4)
    assertEquals(t5.toString, expect5)
    intercept[java.lang.Exception]{pbss("2023-02-01T25")}
  }
  
  test("pbssHour with customer is expected") {
    val expect1 = s"${hroot}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360}"
    val expect2 = s"${hroot}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={*}"
    val expect3 = s"${hroot}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360,19000200}"
    val t1 = pbss("2023-02-22T23").c3id(1960180360)
    val t2 = pbss("2023-02-22T23").c3all
    val t3 = pbss("2023-02-22T23").c3id(1960180360, 19000200)
    assertEquals(t1.toString, expect1)
    assertEquals(t2.toString, expect2)
    assertEquals(t3.toString, expect3)
  }


  test("Test data path should work as expected") {
    def pbss(date: String) = SurgeonPath(TestPbSS()).make(date)
    val path = pbss("2023-02-07T02").c3name("c3.TopServe")
    assertEquals(path, "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}")
  }

  test("Take should work as expected") {
    val c3 = C3(TestPbSS())
    def pbss1(date: String) = SurgeonPath(TestPbSS()).make(date)
    val expect1 = TestPbSS().root + "/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960002004,1960180360,1960181845}"
    val t1 = pbss1("2023-02-07T02").c3take(3)
    assertEquals(t1, expect1)
    val t2 = pbss1("2023-02-07T02").c3take(1)
    assertEquals(t2, TestPbSS().root + "/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960002004}")
  }

  test("c3 methods should work as expected") {
    def pbss(date: String) = SurgeonPath(TestPbSS()).make(date)
    val expect1 = TestPbSS().root + "/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}"
    val expect2 = TestPbSS().root + "/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360,1960184661}"
    val t1 = pbss("2023-02-07T02").c3name("c3.TopServe")
    assertEquals(t1, expect1)
    val t2 = pbss("2023-02-07T02").c3name("c3.TopServe", "c3.FappleTV")
    assertEquals(t2, expect2)
    val t3 = pbss("2023-02-07T02").c3id(1960180360)
    assertEquals(t3, expect1)
    val t4 = pbss("2023-02-07T02").c3id(1960180360, 1960184661)
    assertEquals(t4, expect2)
    // val t5 = pbss("2023-02-07T02").c3id("1960180360", "1960184661") 
    // assertEquals(t5, expect2)
    // val t6 = pbss("2023-02-07T02").c3i("1960180360")
    // assertEquals(t6, expect1)
  }


}

