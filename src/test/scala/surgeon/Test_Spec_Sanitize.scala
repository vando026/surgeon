package conviva.surgeon

import munit.FunSuite
import conviva.surgeon.Sanitize._
import conviva.surgeon.Paths._

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
    val t1 = pbssMonthly(2023, 2)
    assertEquals(t1, expect1)
  }

  test("pbssDaily is expected") {
    val expect1 = s"$root-daily/pbss/daily/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00"
    val expect2 = s"$root-daily/pbss/daily/y=2023/m=02/dt=d2023_02_{22}_08_00_to_2023_02_{23}_08_00"
    val expect3 = s"$root-daily/pbss/daily/y=2023/m=02/dt=d2023_02_{22,23}_08_00_to_2023_02_{23,24}_08_00"
    val t1 = pbssDaily(2, 22)
    val t2 = pbssDaily(2, List(22))
    val t3 = pbssDaily(2, List(22, 23))
    assertEquals(t1, expect1)
    assertEquals(t2, expect2)
    assertEquals(t3, expect3)
  }

  test("pbssHourly is expected") {
    // val expect1 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=22/dt=2023_02_22_23/cust={1960180360}"
    val expect1 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=04/dt=2023_02_04_23"
    // val expect3 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=22/dt=2023_02_22_{*}/cust={1960180360}"
    // val expect4 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=22/dt=2023_02_22_23/cust={*}"
    val expect3 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=22/dt=2023_02_22_{23,24,25}"
    val t1 = pbssHourly(2, 4, 23)
    val t3 = pbssHourly(2, 22, List(23, 24, 25))
    assertEquals(t1, expect1)
    assertEquals(t3, expect3)
  }
}
