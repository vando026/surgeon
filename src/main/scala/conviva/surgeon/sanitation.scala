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
  def toUnsigned(x: Column) =  (BigInt(x >>> 1) << 1) + (x & 1)
  def arrayToUnsigned(col: Column): Column = {
    array_join(transform(col, toUnsigned(_)), ":")
  }

  def toHexString(col: Column): Column = lower(conv(col, 10, 16))
  def arrayToHex(col: Column): Column = {
    array_join(transform(col, toHexString(_)), ":")
  }

  case class ExtractID(field: String) extends ExtractCol {
    def hex(): Column = arrayToHex(col(field)).alias("clientIdHex")
  }

  case class ExtractID2(field: String, field2: String) {
    def hex(): Column = {
      concat_ws(":", arrayToHex(col(field)), toHexString(col(field2)))
        .alias("sid5Hex")
    }
  }

  /** Method to convert signed BigInt to Unsigned BigInt
  */

  /**  to construct clientId in hexadecimal format. 
   *
   *  @param clientId $clientId
   */ 

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

}
