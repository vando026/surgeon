package conviva.surgeon

import munit.FunSuite
import conviva.surgeon.Sanitize._

class Test_Spec_Sanitize extends FunSuite {
  test("A basic test") {
    assert(1 + 2 == 3)
  }

  test("Unsigned to BigInt") {
    assertEquals(toUnsigned(-1), BigInt(4294967295L))
  }
}
