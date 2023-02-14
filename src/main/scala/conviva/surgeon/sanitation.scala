package conviva.surgeon

import org.apache.spark.sql.functions.{col, udf, array_join, transform, lower, concat_ws, conv}
import org.apache.spark.sql.{Column}

/** Methods to convert the `clientId` and `sessionId` fields from unsigned integers to
 *  signed integers or hexadecimal format. 
 * @define clientId The clientID assigned to the client by Conviva
 * @define sessionId The sessionId assigned to the session by Conviva
*/
object Sanitize {

  def toUnsigned_(x: Int): BigInt =  (BigInt(x >>> 1) << 1) + (x & 1)
  /** UDF to convert signed BigInt to Unsigned BigInt
  */
  def toUnsigned = udf[BigInt, Int](toUnsigned_)
  def arrayToUnsigned(col: Column): Column = {
    array_join(transform(col, toUnsigned(_)), ":")
  }

  def toHexString(col: Column): Column = lower(conv(col, 10, 16))
  def arrayToHex(col: Column): Column = {
    array_join(transform(col, toHexString(_)), ":")
  }

  case class ExtractID(field: String, name: String) extends ExtractCol {
    def hex(): Column = arrayToHex(col(field))
      .alias(s"${name}Hex")
    def unsigned(): Column = arrayToUnsigned(col(field))
      .alias(s"${name}AsUnsigned")
    def signed(): Column = concat_ws(":", col(field))
      .alias(s"${name}AsSigned")
  }

  case class ExtractID2(field: String, field2: String, name: String) {
    def hex(): Column = {
      concat_ws(":", arrayToHex(col(field)), toHexString(col(field2)))
        .alias(s"${name}AsHex")
    }
    def unsigned(): Column = {
      concat_ws(":", arrayToUnsigned(col(field)), toUnsigned(col(field2)))
        .alias(s"${name}AsUnsigned")
    }
    def signed(): Column = concat_ws(":", col(field), col(field2))
      .alias(s"${name}AsSigned")
  }
}
