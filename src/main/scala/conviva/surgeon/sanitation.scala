package conviva.surgeon

import org.apache.spark.sql.functions.{udf}

/** Methods to convert the `clientId` and `sessionId` fields from unsigned integers to
 *  signed integers or hexadecimal format. 
 * @define clientId The clientID assigned to the client by Conviva
 * @define sessionId The sessionId assigned to the session by Conviva
*/
object Sanitize {

  implicit class Unsigned(x: Long) {
      def toUnsigned() =  (BigInt(x >>> 1) << 1) + (x & 1)
  }

  val x: Array[Int] = Array(1000000L, 45000000L) 
  val tt = Array(476230728, 1293028608, -1508640558, -1180571212)

  trait SanitizeId extends ExtractCol {
    private def toHexId(field: Any): String = {
      field match {
        case s: Array[Int] => s.map(_.toHexString).mkString(":")
        case s: Int  => s.toHexString
        case _ => "Error: Int or Array[Int] needed"
      }
    }
    private def toUnsignedId(field: Any): String = {
      field match {
        case s: Array[Int] => s.map(_.toUnsigned).mkString(":")
        case s: Int  => s.toUnsigned.toString
        case _ => "Error: Int or Array[Int] needed"
      }
    }

  }
  
  /**
  case class SanitizeId1(field: String) extends SanitizeId {
    def toClientIdHex = udf((clientId: Array[Int]) => toHexId(clientId))
  }
  def clientId() = SanitizeId1("val.sessId.clientId.element")

  def toClientIdHex = udf((clientId: Array[Int], sessId: Int) => {
      toHexId(clientId)
  */
  /** Method to convert signed BigInt to Unsigned BigInt
  */

  /** UDF to construct clientId in hexadecimal format. 
   *
   *  @param clientId $clientId
   */ 
  def toClientIdHex = udf((clientId: Array[BigInt], sessId: Int) => {
      arrayToHex(clientId)
  })

  /** UDF to construct id in hexadecimal format. 
   *
   *  @param clientId $clientId
   */ 
  val toClientIdUnsigned = udf((clientId: Array[BigInt]) => {
    arrayToUnsigned(clientId)
  })

  /** UDF to construct SID5 (clientId:sessionId) in unsigned format. 
   *  @param clientId $clientId
   *  @param sessionId $sessionId 
   */ 
  val toSid5Unsigned = udf((clientId: Array[BigInt], sessId: Int) => {
    arrayToUnsigned(clientId) + ":" + toUnsigned(sessId.toInt)
  }) 

  /** UDF to construct SID5 (clientId:sessionId) in hexadecimal format. 
   *  @param clientId $clientId
   *  @param sessionId $sessionId 
   */ 
  def toSid5Hex = udf((clientId: Array[BigInt], sessId: Int) => {
      arrayToHex(clientId) + ":" +  sessId.toInt.toHexString
  })

  /** UDF to construct SID5 (clientId:sessionId) as is. 
   *  @param clientId $clientId
   *  @param sessionId $sessionId 
   */ 
  def toSid5 = udf((clientId: Array[BigInt], sessId: Int) => {
      clientId.mkString(":") + ":" +  sessId.toString
  })


}
