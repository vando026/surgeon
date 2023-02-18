package conviva.surgeon

import org.apache.spark.sql.functions.{
  col, udf, array_join, transform, lower, 
  concat_ws, conv, from_unixtime, when, lit
}
import org.apache.spark.sql.{Column}

/** Methods to convert the `clientId` and `sessionId` fields from unsigned integers to
 *  signed integers or hexadecimal format. 
 * @define clientId The clientID assigned to the client by Conviva
 * @define sessionId The sessionId assigned to the session by Conviva
*/
object Sanitize {

  /** A trait for extracting columns from Session Summary and RawLog data.
  */
  trait ExtractCol {
    /** The input field */
    def field: String
    /** Method to extract the last element from the field name */
    def suffix(s: String) = s.split("\\.").last
    /** Extract the column as is with suffix name */
    def asis(): Column = col(field).alias(suffix(field))
  }

  /** A trait for extracting time-based columns from Session Summary and RawLog data.
  */
  trait ExtractColTime extends ExtractCol {
    /** The name to rename the field */
    def name: String
    val val1 = lit(1000)
    /** A value to convert ms to sec */
    val val2= lit(1)
    /** Convert to Epoch timestamp from seconds to milliseconds */
    def ms(): Column = {
      (col(field) * val2).cast("Long").alias(s"${name}Ms")
    }
    /** Convert to Epoch timestamp from milliseconds to seconds */
    def sec(): Column = {
      (col(field)  / val1).cast("Long").alias(s"${name}Sec")
    }
    /** Convert to Epoch timestamp to human readable time stamp */
    def stamp(): Column = {
      from_unixtime(col(field) / val1).alias(s"${name}Stamp")
    }
  }
  

  /** A class for extracting columns as is from Session Summary and RawLog data.
  */
  case class ExtractColAs(
      field: String
    ) extends ExtractCol 

  /** A class for extracting time-based columns in milliseconds.
  */
  case class ExtractColMs(
      field: String, 
      name: String
    ) extends ExtractColTime

  /** A class for extracting time-based columns in seconds.
  */
  case class ExtractColSec(
      field: String, 
      name: String
    ) extends ExtractColTime {
      override val val1 = lit(1)
      override val val2 = lit(1000)
    }

  /** UDF to convert signed BigInt to Unsigned BigInt
  */
  def toUnsigned_(x: Int): BigInt =  (BigInt(x >>> 1) << 1) + (x & 1)
  def toUnsigned = udf[BigInt, Int](toUnsigned_)
  def arrayToUnsigned(col: Column): Column = {
    array_join(transform(col, toUnsigned(_)), ":")
  }

  // def toHexString(col: Column): Column = lower(conv(col, 10, 16))
  def toHexString_(x: Int): String = x.toHexString
  def toHexString = udf[String, Int](toHexString_)
  def arrayToHex(col: Column): Column = {
    array_join(transform(col, toHexString(_)), ":")
  }

  /** Class to extract and convert ID related fields 
   * @param field The input field
   * @param name The new name for input field
   */
  case class ExtractID(field: String, name: String) extends ExtractCol {
    /** Method to convert to hexadecimal format */
    def hex(): Column = arrayToHex(col(field))
      .alias(s"${name}Hex")
    /** Method to convert to unsigned format */
    def unsigned(): Column = arrayToUnsigned(col(field))
      .alias(s"${name}AsUnsigned")
    /** Method to convert to signed format */
    def signed(): Column = concat_ws(":", col(field))
      .alias(s"${name}AsSigned")
  }

  /** Class to extract and convert 2 ID related fields into 1
   * @param field The first input field
   * @param field2 The second input field
   * @param name The new name for input field
   */
  case class ExtractID2(field: String, field2: String, name: String) {
    /** Method to convert to hexadecimal format */
    def hex(): Column = {
      concat_ws(":", arrayToHex(col(field)), toHexString(col(field2)))
        .alias(s"${name}AsHex")
    }
    /** Method to convert to unsigned format */
    def unsigned(): Column = {
      concat_ws(":", arrayToUnsigned(col(field)), toUnsigned(col(field2)))
        .alias(s"${name}AsUnsigned")
    }
    /** Method to convert to signed format */
    def signed(): Column = concat_ws(":", col(field), col(field2))
      .alias(s"${name}AsSigned")
  }
}
