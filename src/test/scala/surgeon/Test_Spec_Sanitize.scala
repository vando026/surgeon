package conviva.surgeon

import munit.FunSuite
import conviva.surgeon.Sanitize._

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
}
