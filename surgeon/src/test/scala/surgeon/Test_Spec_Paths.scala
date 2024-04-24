package conviva.surgeon

class PathSuite extends munit.FunSuite { 

  import conviva.surgeon.Sanitize._
  import conviva.surgeon.Paths._
  import conviva.surgeon.Customer._
  import conviva.surgeon.GeoInfo._
  import org.apache.spark.sql.functions.{col}
  import org.apache.spark.sql.{SparkSession, Row}

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  PathDB.geoUtilPath = PathDB.testPath

  test("pbssMonth is expected") {
    val mroot  = s"${PathDB.root}/${PathDB.pbssMonthly}"
    val expect1 = s"${mroot}/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00"
    val expect2 = s"${mroot}/y=2022/m=12/dt=c2022_12_01_08_00_to_2023_01_01_08_00/cust={1960180360}"
    val t1 = Path.pbss("2023-02")
    val t2 = Path.pbss("2022-12").c3id(1960180360)
    val t3 = Path.pbss("2023-2")
    assertEquals(t1.toList(0), expect1)
    assertEquals(t2.toList(0), expect2)
    assertEquals(t3.toList(0), expect1)
  }

  test("pbssDay is expected") {
    val droot = s"${PathDB.root}/${PathDB.pbssDaily}"
    val expect1 =  s"${droot}/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00"
    val expect2 =  s"${droot}/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00/cust={1960180360}"
    val expect3 =  s"${droot}/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00"
    val expect31 = s"${droot}/y=2023/m=02/dt=d2023_02_23_08_00_to_2023_02_24_08_00"
    val expect4 =  s"${droot}/y=2023/m=12/dt=d2023_12_31_08_00_to_2024_01_01_08_00"
    val expect5 =  s"${droot}/y=2023/m=10/dt=d2023_10_31_08_00_to_2023_11_01_08_00"
    val expect6 =  s"${droot}/y=2023/m=02/dt=d2023_02_02_08_00_to_2023_02_03_08_00"
    val t1 = Path.pbss("2023-02-22")
    val t2 = Path.pbss("2023-02-22").c3id(1960180360)
    val t3 = Path.pbss("2023-02-{22,23}")
    val t4 = Path.pbss("2023-12-31")
    val t5 = Path.pbss("2023-10-31")
    val t6 = Path.pbss("2023-02-{22,23}")
    val t7 = Path.pbss("2023-02-{22, 23}")
    val t8 = Path.pbss("2023-02-{22,   23}")
    val t9 = Path.pbss("2023-02-2")
    assertEquals(t1.toList(0), expect1)
    assertEquals(t2.toList(0), expect2)
    assertEquals(t3.toList(0), expect3)
    assertEquals(t3.toList(1), expect31)
    assertEquals(t4.toList(0), expect4)
    assertEquals(t5.toList(0), expect5)
    assertEquals(t9.toList(0), expect6)
    assertEquals(t6.toList.length, 2)
    // assertEquals(t7.toList(0), expect1)
    // assertEquals(t8.toList(0), expect1)
    intercept[java.lang.Exception]{Path.pbss("2023-03-34")}
    // intercept[java.lang.Exception]{pbssDay(3, List(32), 2023).toString}
  }

  val hroot = s"${PathDB.root}/${PathDB.pbssHourly}"
  test("PbSS.Hourly is expected") {
    val expect1 = s"${hroot}/y=2023/m=02/d=04/dt=2023_02_04_23"
    val expect2 = s"${hroot}/y=2023/m=02/d=07/dt=2023_02_07_02"
    val expect30 = s"${hroot}/y=2023/m=02/d=22/dt=2023_02_22_10"
    val expect31 = s"${hroot}/y=2023/m=02/d=22/dt=2023_02_22_11"
    val expect32 = s"${hroot}/y=2023/m=02/d=22/dt=2023_02_22_12"
    val t1 = Path.pbss("2023-02-04T23")
    val t2 = Path.pbss("2023-02-07T02")
    val t3 = Path.pbss("2023-02-22T{10-12}")
    val t4 = Path.pbss("2023-02-22T{10,12}")
    val t5 = Path.pbss("2023-02-07T2")
    assertEquals(t1.toList(0), expect1)
    // assertEquals(t2.toList(0), expect2)
    assertEquals(t3.toList(1), expect31)
    assertEquals(t4.toList(1), expect32)
    assertEquals(t3.toList.length, 3)
    assertEquals(t4.toList.length, 2)
    assertEquals(t2.toList(0), expect2)
    // intercept[java.lang.Exception]{pbssHour(2, 30, List(22), 2023).toString}
    // intercept[java.lang.Exception]{pbssHour(3, 2, List(24), 2023).toString}
    // intercept[java.lang.Exception]{pbssHour(3, 2, 24, 2023).toString}
  }
  
  test("pbssHour with customer is expected") {
    val expect1 = s"${hroot}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360}"
    val expect2 = s"${hroot}/y=2023/m=02/d=22/dt=2023_02_22_23"
    val expect3 = s"${hroot}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360,19000200}"
    val expect4 = s"${hroot}/y=2023/m=02/d=22/dt=2023_02_22_03"
    val t1 = Path.pbss("2023-02-22T23").c3id(1960180360)
    val t2 = Path.pbss("2023-02-22T23")
    val t3 = Path.pbss("2023-02-22T23").c3ids(List(1960180360, 19000200))
    assertEquals(t1.toList(0), expect1)
    assertEquals(t2.toList(0), expect2)
    assertEquals(t3.toList(0), expect3)
  }

  test("pbrlHour is expected") {
    val expect1 = s"${PathDB.root}/${PathDB.pbrlHourly}/y=2024/m=02/d=01/dt=2024_02_01_00/cust={1960180442}"
    val t1 = Path.pbrl("2024-02-01T00").c3id(1960180442)
    assertEquals(t1.toList(0), expect1)
  }

  test("Test data path should work as expected") {
    PathDB.root = PathDB.testPath
    PathDB.pbssHourly = "pbss"
    val path = Path.pbss("2023-02-07T02").c3name("c3.TopServe")
    assertEquals(path.toList(0), "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}")
  }

  test("Take should work as expected") {
    val expect1 = PathDB.testPath + "/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960002004,1960180360,1960181845}"
    val t1 = Path.pbss("2023-02-07T02").c3take(3).toList(0)
    assertEquals(t1, expect1)
    val t2 = Path.pbss("2023-02-07T02").c3take(1).toList(0)
    assertEquals(t2, PathDB.testPath + "/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960002004}")
  }

  test("c3 methods should work as expected") {
    val expect1 = PathDB.testPath + "/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}"
    val expect2 = PathDB.testPath + "/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360,1960184661}"
    val t1 = Path.pbss("2023-02-07T02").c3name("c3.TopServe").toList(0)
    assertEquals(t1, expect1)
    val t2 = Path.pbss("2023-02-07T02").c3names(List("c3.TopServe", "c3.FappleTV")).toList(0)
    assertEquals(t2, expect2)
    val t3 = Path.pbss("2023-02-07T02").c3id(1960180360).toList(0)
    assertEquals(t3, expect1)
    val t4 = Path.pbss("2023-02-07T02").c3ids(List(1960180360, 1960184661)).toList(0)
    assertEquals(t4, expect2)
    val t5 = Path.pbss("2023-02-07T02").c3ids(List("1960180360", "1960184661")).toList(0)
    assertEquals(t5, expect2)
    val t6 = Path.pbss("2023-02-07T02").c3id("1960180360").toList(0)
    assertEquals(t6, expect1)
  }

}

