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

  test("pbssDaily is expected") {
    val expect = s"$root-daily/pbss/daily/y=2023/m=02/dt=d2023_02_22_08_00_to_2023_02_23_08_00/cust={1960180360}"
    val t1 = pbssDaily(2, 22, 1960180360)
    val t2 = pbssDaily(2, List(22), 1960180360)
    val t3 = pbssDaily(2, List("22"), 1960180360)
    val t4 = pbssDaily(2, "22", 1960180360)
    assertEquals(t1, expect)
    assertEquals(t2, expect)
    assertEquals(t3, expect)
    assertEquals(t4, expect)
  }

  test("pbssHourly is expected") {
    val all = "{*}"
    val one = "{1960180360}"
    val expect2 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=22/dt=2023_02_22_23/cust=$all"
    val expect1 = s"$root-hourly/pbss/hourly/st=0/y=2023/m=02/d=22/dt=2023_02_22_23/cust=$one"
    val t1 = pbssHourly(2, 22, 23, 1960180360)
    val t2 = pbssHourly(2, 22, "23", 1960180360)
    val t3 = pbssHourly(2, 22, List(23), 1960180360)
    val t4 = pbssHourly(2, 22, 23)
    val t6 = pbssHourly("2", 22, 23)
    val t5 = pbssHourly(2, 23, List(23), 1960180360)
    assertEquals(t1, expect1)
    assertEquals(t2, expect1)
    assertEquals(t3, expect1)
    assertEquals(t4, expect2)
    assertNotEquals(t5, expect1)
    assertEquals(t6, expect2)
  }
}
