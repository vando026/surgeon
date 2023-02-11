package conviva.surgeon

import org.apache.spark.sql.functions.{udf}

/** Methods to convert the `clientId` and `sessionId` fields from unsigned integers to
 *  signed integers or hexadecimal format. 
 * @define clientId The clientID assigned to the client by Conviva
 * @define sessionId The sessionId assigned to the session by Conviva
*/
object Sanitize {

  /** Method to convert signed BigInt to Unsigned BigInt
  */
  def toUnsigned(x: Int): BigInt = {
    (BigInt(x >>> 1) << 1) + (x & 1)
  }

  def arrayToHex(array: Array[BigInt]): String = {
    array.map(_.toInt.toHexString).mkString(":")
  }

  def arrayToUnsigned(array: Array[BigInt]): String = {
    array.map(i => toUnsigned(i.toInt)).mkString(":")
  }

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
