package conviva.surgeon

import munit.FunSuite
import conviva.surgeon.Sanitize._
import conviva.surgeon.Paths._
import conviva.surgeon.Donor._

class Test_Spec_Sanitize extends FunSuite {
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
  val root = "/mnt/conviva-prod-archive-pbss"

  test("pbssMonthly is expected") {
    val expect1 = s"$root-monthly/pbss/monthly/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00"
    val expect2 = s"$root-monthly/pbss/monthly/y=2022/m=12/dt=c2022_12_01_08_00_to_2023_01_01_08_00"
    val t1 = pbssMonthly(2023, 2)
    val t2 = pbssMonthly(2022, 12)
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
  }

  test("pbssDaily is expected") {
    val expect1 = s"$root-daily/pbss/daily/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00"
    val expect2 = s"$root-daily/pbss/daily/y=2023/m=02/dt=d2023_02_{22}_08_00_to_2023_02_{23}_08_00"
    val expect3 = s"$root-daily/pbss/daily/y=2023/m=02/dt=d2023_02_{22,23}_08_00_to_2023_02_{23,24}_08_00"
    val t1 = pbssDaily(2, 22, 2023)
    val t2 = pbssDaily(2, List(22), 2023)
    val t3 = pbssDaily(2, List(22, 23), 2023)
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
    assertEquals(t3, expect3)
  }

  test("pbssHourly is expected") {
    val expect1 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=04/dt=2023_02_04_23"
    val expect3 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=22/dt=2023_02_22_{23,24,25}"
    val t1 = pbssHourly(2, 4, 23, 2023)
    val t3 = pbssHourly(2, 22, List(23, 24, 25), 2023)
    assertEquals(t1, expect1)
    assertEquals(t3, expect3)
  }
  
  test("With customer is expected") {
    val expect1 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360}"
    val expect2 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=22/dt=2023_02_22_23"
    val expect3 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360,19000200}"
    val expect4 = s"$root-monthly/pbss/monthly/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00/cust={1960180361,1960180418}"
    val t1 = Customer(pbssHourly(2, 22, 23, 2023)).id(1960180360)
    val t2 = Customer(pbssHourly(2, 22, 23, 2023)).all
    val t3 = Customer(pbssHourly(2, 22, 23, 2023)).ids(List(1960180360, 19000200))
    // val t4 = Customer(pbssMonthly(2023, 2)).names(List("MLB", "CBSCom"))
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
    assertEquals(t3, expect3)
    // assertEquals(t4, expect4)
  }
}
