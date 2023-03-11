package conviva.surgeon

import munit.FunSuite
import conviva.surgeon.Sanitize._
import conviva.surgeon.Paths._
import conviva.surgeon.Donor._
import org.apache.spark.sql.{SparkSession}

class Test_Spec_Sanitize extends FunSuite 
  with SparkSessionTestWrapper { 

  test("A basic test") {
    assert(1 + 2 == 3)
  }

  test("Unsigned to BigInt") {
    val t1 = toUnsigned(-1)
    val t2 = BigInt(4294967295L)
    assertEquals(t1, t2) 
    // val t1 = BigInt(9223372036854775817)
    // val t2 = BigInt(9223372036854775807 + 10)
    // assertEquals(t1, t2.toUnsigned)
  }

  object TestRoot {
    /** The root path. */
    val custDat = "./common/src/test/data/cust_dat.txt"
  }

  val custDat = geoUtilCustomer(geopath =  TestRoot.custDat)

  test("Geoutil customer data is expected") {
    val t1 = custDat.select("customerName").collect().map(_(0)) 
    val t2 = custDat.count
    assertEquals(t1(0).toString, "c3.Demo1")
    assertEquals(t2.toInt, 4)
  }

  // test("customerName ") {
  //   val t1 = customerNameToId(List("Demo1"))(0).toInt
  //   assertEquals(t1, 207488736)
  // }

  val root = "/mnt/conviva-prod-archive-pbss"
  test("pbssMonthly is expected") {
    val expect1 = s"$root-monthly/pbss/monthly/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00"
    // val expect2 = s"$root-monthly/pbss/monthly/y=2022/m=12/dt=c2022_12_01_08_00_to_2023_01_01_08_00"
    val t1 = PbSSMonthly(2023, 2).asis
    // val t2 = PbSSMonthly(2022, 12).asis
    assertEquals(t1, expect1)
    // assertEquals(t2, expect2)
  }

  test("PbSSDaily is expected") {
    val expect1 = s"$root-daily/pbss/daily/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00"
    val expect2 = s"$root-daily/pbss/daily/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00/cust={207488736}"
    val t1 = PbSSDaily(2, 22, 2023).custAll
    // val t2 = PbSSDaily(2, 22, 2023).custName("c3.Demo1")
    assertEquals(t1, expect1)
    // assertEquals(t2, expect2)
  }

  test("PbSSHourly is expected") {
    val expect1 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=04/dt=2023_02_04_23"
    val expect3 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=22/dt=2023_02_22_{23,24,25}"
    val t1 = PbSSHourly(2, 4, List(23), 2023).asis
    val t3 = PbSSHourly(2, 22, List(23, 24, 25), 2023).asis
    assertEquals(t1, expect1)
    assertEquals(t3, expect3)
  }
  
  test("With customer is expected") {
    val expect1 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360}"
    val expect2 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=22/dt=2023_02_22_23"
    val expect3 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360,19000200}"
    val expect4 = s"$root-monthly/pbss/monthly/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00/cust={1960180361,1960180418}"
    val t1 = PbSSHourly(2, 22, List(23), 2023).custId(1960180360)
    val t2 = PbSSHourly(2, 22, List(23), 2023).custAll
    val t3 = PbSSHourly(2, 22, List(23), 2023).custIds(List(1960180360, 19000200))
    // val t4 = Customer(pbssMonthly(2023, 2)).names(List("MLB", "CBSCom"))
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
    assertEquals(t3, expect3)
    // assertEquals(t4, expect4)
  }

}
