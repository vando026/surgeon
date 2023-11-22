package conviva.surgeon


class SanitizeSuite extends munit.FunSuite { 
  import conviva.surgeon.Sanitize._

  test("Unsigned to BigInt") {
    val t1 = toUnsigned(-1)
    val t2 = BigInt(4294967295L)
    assertEquals(t1, t2) 
  }

}
