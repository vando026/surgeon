package conviva.surgeon

import conviva.surgeon.Sanitize._
import conviva.surgeon.Paths._
import conviva.surgeon.Customer._
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.{SparkSession}
import java.io._

class PathSuite extends munit.FunSuite { 

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  val geopath = "./src/test/data/cust_dat.txt"
  val pbssTestPath = "./src/test/data" 

  test("Customer data is expected") {
    val custData = customerNames(path = geopath)
    val t1 = custData
      .select(col("customerId"))
      .where(col("customerName") === "MSNBC")
      .collect().map(_(0)) 
    assertEquals(t1(0).toString, "207488736")
  }

  test("customerNamToId is expected") {
    val custData = customerNames(path = geopath)
    val t1 = customerNameToId(List("MSNBC"), custData)(0).toInt
    val t2 = customerNameToId(List("MSNBC", "TV2"), custData)
      .map(_.toInt)
    assertEquals(t1, 207488736)
    assertEquals(t2.toSeq, Seq(207488736, 1960180360))
  }

  // test("Customer take n is expected") {
  //   val t1 = customerIds(Monthly(2023, 2).path).take(3).length
  //   assertEquals(t1, 3)
  // }

  val root = "/mnt/conviva-prod-archive-pbss"
  test("pbssMonthly is expected") {
    val expect1 = s"${PathDB.monthly}/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00"
    val expect2 = s"${PathDB.monthly}/y=2022/m=12/dt=c2022_12_01_08_00_to_2023_01_01_08_00/cust={207488736}"
    val t1 = Monthly(2023, 2).toString
    val t2 = Cust(Monthly(2022, 12), names = List("MSNBC"), geopath)
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
  }

  test("Daily is expected") {
    val expect1 = s"${PathDB.daily}/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00"
    val expect2 = s"${PathDB.daily}/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00/cust={207488736}"
    val expect3 = s"${PathDB.daily}/y=2023/m=02/dt=d2023_02_{22,23}_08_00_to_2023_02_{23,24}_08_00"
    val expect4 = s"${PathDB.daily}/y=2023/m=12/dt=d2023_12_31_08_00_to_2024_01_01_08_00"
    val expect5 = s"${PathDB.daily}/y=2023/m=10/dt=d2023_10_31_08_00_to_2023_11_01_08_00"
    val t1 = Daily(2, 22, 2023).toString
    val t2 = Cust(Daily(2, 22, 2023), names = List("MSNBC"), geopath)
    val t3 = Daily(2, List(22,23), 2023).toString
    val t4 = Daily(12, 31, 2023).toString
    val t5 = Daily(10, 31, 2023).toString
    val t6 = Daily(2, List(22, 23), 2023).toList
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
    assertEquals(t3, expect3)
    assertEquals(t4, expect4)
    assertEquals(t5, expect5)
    assertEquals(t6.length, 2)
    assertEquals(t6(0), expect1)
    intercept[java.lang.Exception]{Daily(10, List(30, 31), 2023).toString}
    intercept[java.lang.Exception]{Daily(2, List(30), 2023).toString}
    intercept[java.lang.Exception]{Daily(3, List(32), 2023).toString}
  }

  test("Hourly is expected") {
    val expect1 = s"${PathDB.hourly()}/y=2023/m=02/d=04/dt=2023_02_04_23"
    val expect3 = s"${PathDB.hourly()}/y=2023/m=02/d=22/dt=2023_02_22_{10,11,12}"
    val expect4 = s"${PathDB.hourly()}/y=2023/m=02/d={22,23}/dt=2023_02_{22,23}_{10,11,12}"
    val expect5 = s"${PathDB.hourly()}/y=2023/m=05/d=30/dt=2023_05_30_{00,01,02,03}"
    val expect6 = s"${PathDB.hourly()}/y=2023/m=05/d=30/dt=2023_05_30_00"
    val t1 = Hourly(2, 4, List(23), 2023).toString
    val t3 = Hourly(2, 22, List(10, 11, 12), 2023).toString
    val t4 = Hourly(2, List(22, 23), List(10, 11, 12), 2023).toString
    val t5 = Hourly(5, 30, List(0, 1, 2, 3))
    assertEquals(t1, expect1)
    assertEquals(t3, expect3)
    assertEquals(t4, expect4)
    assertEquals(t5.toString, expect5)
    assertEquals(t5.toList.length, 4)
    assertEquals(t5.toList(0), expect6)
    intercept[java.lang.Exception]{Hourly(2, 30, List(22), 2023).toString}
    intercept[java.lang.Exception]{Hourly(3, 2, List(24), 2023).toString}
    intercept[java.lang.Exception]{Hourly(3, 2, 24, 2023).toString}
    // assertEquals(t4, expect4)
  }
  
  test("Hourly with customer is expected") {
    val expect1 = s"${PathDB.hourly()}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360}"
    val expect2 = s"${PathDB.hourly()}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={*}"
    val expect3 = s"${PathDB.hourly()}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360,19000200}"
    val expect4 = s"${PathDB.hourly()}/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00/cust={1960180361,1960180418}"
    val t1 = Cust(Hourly(2, 22, List(23), 2023), ids = List(1960180360))
    val t2 = Cust(Hourly(2, 22, List(23), 2023))
    val t3 = Cust(Hourly(2, 22, List(23), 2023), ids = List(1960180360, 19000200))
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
    assertEquals(t3, expect3)
  }

  // see getData.scala in ./src/test/data/ for generating these paths
  val ois = new ObjectInputStream(new java.io.FileInputStream(s"$pbssTestPath/pathsHourly2_24"))
  val pathx = ois.readObject.asInstanceOf[Array[String]]
  ois.close


}

