package conviva.surgeon

import conviva.surgeon.Sanitize._
import conviva.surgeon.Paths._
import conviva.surgeon.Customer._
import conviva.surgeon.Heart._
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.{SparkSession}

class Test_Spec_Sanitize extends munit.FunSuite { 

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  test("Unsigned to BigInt") {
    val t1 = toUnsigned(-1)
    val t2 = BigInt(4294967295L)
    assertEquals(t1, t2) 
  }

  // get the customer data
  val custData = geoUtilCustomer()

  test("Customer data is expected") {
    val t1 = custData
      .select(col("customerId"))
      .where(col("customerName") === "MSNBC")
      .collect().map(_(0)) 
    assertEquals(t1(0).toString, "207488736")
  }

  test("customerNameToId is expected") {
    val t1 = customerNameToId(List("MSNBC"), custData)(0).toInt
    val t2 = customerNameToId(List("MSNBC", "TV2"), custData)
      .map(_.toInt)
    assertEquals(t1, 207488736)
    assertEquals(t2.toSeq, Seq(207488736, 1960180360))
  }

  // test("Customer take n is expected") {
  //   val t1 = getCustomerIds(Monthly(2023, 2).path).take(3).length
  //   assertEquals(t1, 3)
  // }

  val root = "/mnt/conviva-prod-archive-pbss"
  test("pbssMonthly is expected") {
    val expect1 = s"${PathDB.monthly}/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00"
    val expect2 = s"${PathDB.monthly}/y=2022/m=12/dt=c2022_12_01_08_00_to_2023_01_01_08_00/cust={207488736}"
    val t1 = Monthly(2023, 2).path
    val t2 = Monthly(2022, 12).custName("MSNBC")
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
  }

  test("Daily is expected") {
    val expect1 = s"${PathDB.daily}/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00"
    val expect2 = s"${PathDB.daily}/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00/cust={207488736}"
    val t1 = Daily(2, 22, 2023).custAll
    val t2 = Daily(2, 22, 2023).custName("MSNBC")
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
  }

  val rhourly = root + "-hourly/pbss/hourly/st=0/"
  test("Hourly is expected") {
    val expect1 = s"${PathDB.hourly}/y=2023/m=02/d=04/dt=2023_02_04_23"
    val expect3 = s"${PathDB.hourly}/y=2023/m=02/d=22/dt=2023_02_22_{23,24,25}"
    val t1 = Hourly(2, 4, List(23), 2023).path
    val t3 = Hourly(2, 22, List(23, 24, 25), 2023).path
    assertEquals(t1, expect1)
    assertEquals(t3, expect3)
  }
  
  test("Hourly with customer is expected") {
    val expect1 = s"${PathDB.hourly}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360}"
    val expect2 = s"${PathDB.hourly}/y=2023/m=02/d=22/dt=2023_02_22_23"
    val expect3 = s"${PathDB.hourly}/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360,19000200}"
    val expect4 = s"${PathDB.monthly}/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00/cust={1960180361,1960180418}"
    val t1 = Hourly(2, 22, List(23), 2023).custId(1960180360)
    val t2 = Hourly(2, 22, List(23), 2023).custAll
    val t3 = Hourly(2, 22, List(23), 2023).custIds(List(1960180360, 19000200))
    // val t4 = Customer(pbssMonthly(2023, 2)).names(List("MLB", "CBSCom"))
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
    assertEquals(t3, expect3)
    // assertEquals(t4, expect4)
  }

}
