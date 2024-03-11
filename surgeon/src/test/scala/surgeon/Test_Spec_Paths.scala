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

  val custData = getGeoData("customer", PathDB.testPath)

  test("pbssMonth is expected") {
    val expect1 = s"${PathDB.pbssProd1M}/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00"
    val expect2 = s"${PathDB.pbssProd1M}/y=2022/m=12/dt=c2022_12_01_08_00_to_2023_01_01_08_00/cust={1960180360}"
    val t1 = pbssMonth(2023, 2).toString
    val t2 = Cust(pbssMonth(2022, 12), name = List("c3.TopServe"), custData)
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
  }

  test("pbssDay is expected") {
    val expect1 = s"${PathDB.pbssProd1d}/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00"
    val expect2 = s"${PathDB.pbssProd1d}/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00/cust={1960180360}"
    val expect3 = s"${PathDB.pbssProd1d}/y=2023/m=02/dt=d2023_02_{22,23}_08_00_to_2023_02_{23,24}_08_00"
    val expect4 = s"${PathDB.pbssProd1d}/y=2023/m=12/dt=d2023_12_31_08_00_to_2024_01_01_08_00"
    val expect5 = s"${PathDB.pbssProd1d}/y=2023/m=10/dt=d2023_10_31_08_00_to_2023_11_01_08_00"
    val t1 = pbssDay(2, 22, 2023).toString
    val t2 = Cust(pbssDay(2, 22, 2023), name = List("c3.TopServe"), custData)
    val t3 = pbssDay(2, List(22,23), 2023).toString
    val t4 = pbssDay(12, 31, 2023).toString
    val t5 = pbssDay(10, 31, 2023).toString
    val t6 = pbssDay(2, List(22, 23), 2023).toList
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
    assertEquals(t3, expect3)
    assertEquals(t4, expect4)
    assertEquals(t5, expect5)
    assertEquals(t6.length, 2)
    assertEquals(t6(0), expect1)
    intercept[java.lang.Exception]{pbssDay(10, List(30, 31), 2023).toString}
    intercept[java.lang.Exception]{pbssDay(2, List(30), 2023).toString}
    intercept[java.lang.Exception]{pbssDay(3, List(32), 2023).toString}
  }

  test("PbSS.Hourly is expected") {
    val expect1 = s"${PathDB.pbssProd1h()}/y=2023/m=02/d=04/dt=2023_02_04_23"
    val expect3 = s"${PathDB.pbssProd1h()}/y=2023/m=02/d=22/dt=2023_02_22_{10,11,12}"
    val expect4 = s"${PathDB.pbssProd1h()}/y=2023/m=02/d={22,23}/dt=2023_02_{22,23}_{10,11,12}"
    val expect5 = s"${PathDB.pbssProd1h()}/y=2023/m=05/d=30/dt=2023_05_30_{00,01,02,03}"
    val expect6 = s"${PathDB.pbssProd1h()}/y=2023/m=05/d=30/dt=2023_05_30_00"
    val t1 = pbssHour(2, 4, List(23), 2023).toString
    val t3 = pbssHour(2, 22, List(10, 11, 12), 2023).toString
    val t4 = pbssHour(2, List(22, 23), List(10, 11, 12), 2023).toString
    val t5 = pbssHour(5, 30, List(0, 1, 2, 3), 2023)
    assertEquals(t1, expect1)
    assertEquals(t3, expect3)
    assertEquals(t4, expect4)
    assertEquals(t5.toString, expect5)
    assertEquals(t5.toList.length, 4)
    assertEquals(t5.toList.apply(0), expect6)
    intercept[java.lang.Exception]{pbssHour(2, 30, List(22), 2023).toString}
    intercept[java.lang.Exception]{pbssHour(3, 2, List(24), 2023).toString}
    intercept[java.lang.Exception]{pbssHour(3, 2, 24, 2023).toString}
    // assertEquals(t4, expect4)
  }
  
  test("pbssHour with customer is expected") {
    val expect1 = s"${PathDB.pbssProd1h()}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360}"
    val expect2 = s"${PathDB.pbssProd1h()}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={*}"
    val expect3 = s"${PathDB.pbssProd1h()}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360,19000200}"
    val expect4 = s"${PathDB.pbssProd1h()}/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00/cust={1960180361,1960180418}"
    val t1 = Cust(pbssHour(2, 22, List(23), 2023), id = List(1960180360))
    val t2 = Cust(pbssHour(2, 22, List(23), 2023))
    val t3 = Cust(pbssHour(2, 22, List(23), 2023), id = List(1960180360, 19000200))
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
    assertEquals(t3, expect3)
  }

  test("pbrLHour is expected") {
    val expect1 = s"${PathDB.pbrlProd()}/y=2024/m=02/d=01/dt=2024_02_01_00/cust={1960180442}"
    val t1 = Cust(pbrlHour(2, 1, List(0), 2024), id = 1960180442)
    assertEquals(t1, expect1)
  }

  test("Test data path should work as expected") {
    val path = Cust(pbssHour(year=2023, month=2, day=7, hour=2, root = PathDB.testPath + "pbss"), 
      name = "c3.TopServe", cmap = custData)
    assertEquals(path, "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}")
  }

  test("Take should work as expected") {
    val t1 = Cust(pbssHour(year=2023, month=2, day=7, hour=2, root = PathDB.testPath + "pbss"), take = 3)


    assertEquals(t1, PathDB.testPath + "pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960181845,1960002004,1960180360}")
  }

  // import org.apache.hadoop.fs._
  // import org.apache.hadoop.conf._

    // val path = pbssHour(year=2023, month=2, day=7, hour=2, root = PathDB.testPath + "pbss")
    // val ss = SparkSession.builder.master("local[*]")
    //   .getOrCreate.sparkContext.hadoopConfiguration
    // val dbfs = FileSystem.get(ss)
    // val paths = dbfs.listStatus(new Path(s"$path"))
    //   .map(_.getPath.toString)
    //   .filter(_.contains("cust"))
    // val pattern = "^.*/cust=([0-9][0-9]+)$".r
    // val out = paths.map(f => { val pattern(h) = f; h })
    // out.map(_.toInt).toList


}

