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
    /** Name with which to rename the field */
    def newName: String = field.split("\\.").last
    /** Extract the field as is with name */
    def asis(): Column = col(field).alias(s"$newName")
  }

  /** A trait for extracting time-based columns from Session Summary and RawLog data.
  */
  trait ExtractColTime extends ExtractCol {
    /** Convert to Unix epoch time to a different timescale
     *  @param scale A multiplier such as 1000.0 that converts seconds to
     *  milliseconds, or 1.0/1000 that converts milliseconds to seconds 
     *  @param suffix A suffix added to `name` to identify time scale of field.
     *  Typically `Ms` or `Sec`.
     */
    def convert(scale: Double, suffix: String): Column = {
      (col(field) * lit(scale)).cast("Long").alias(s"${newName}${suffix}")
    }
    /** Convert to Unix epoch time to readable time stamp */
    def stamp_(scale: Double): Column = {
      from_unixtime(col(field) * lit(scale)).alias(s"${newName}Stamp")
    }
  }
  
  /** A class for extracting columns as is from Session Summary and RawLog data.
  */
  case class ExtractColAs(
      field: String,
      name: Option[String] = None
    ) extends ExtractCol 
    // {
    //   override def newName = name.getOrElse(newName)
    // }

  /** A class for extracting time-based columns in milliseconds.
  */
  case class ExtractColMs(
      field: String, name: String
    ) extends ExtractColTime {
      override def newName = name
      def ms() = convert(1.0, "Ms")
      def sec() = convert(1.0/1000, "Sec")
      def stamp() = stamp_(1.0/1000)
    }

  /** A class for extracting time-based columns in seconds.
  */
  case class ExtractColSec(
      field: String, name: String
    ) extends ExtractColTime {
      override def newName = name
      def ms() = convert(1000.0, "Ms")
      def sec() = convert(1.0, "Sec")
      def stamp() = stamp_(1.0)
    }

  /** UDF to convert signed BigInt to Unsigned BigInt
  */
  def toUnsigned(x: Int): BigInt =  (BigInt(x >>> 1) << 1) + (x & 1)
  def toUnsignedUDF = udf[BigInt, Int](toUnsigned)
  def arrayToUnsigned(col: Column): Column = {
    array_join(transform(col, toUnsignedUDF(_)), ":")
  }

  // def toHexString(col: Column): Column = lower(conv(col, 10, 16))
  def toHexString_(x: Int): String = x.toHexString
  def toHexStringUDF = udf[String, Int](toHexString_)
  def arrayToHex(col: Column): Column = {
    array_join(transform(col, toHexStringUDF(_)), ":")
  }

  trait ExtractID extends ExtractCol {
    def name: String
    override def newName = name
    /** Method to convert to hexadecimal format */
    def hex(): Column = toHexStringUDF(col(field))
      .alias(s"${name}Hex")
    /** Method to convert to unsigned format */
    def unsigned(): Column = toUnsignedUDF(col(field))
      .alias(s"${name}Unsigned")
    /** Method to convert to signed format */
    def signed(): Column = concat_ws(":", col(field))
      .alias(s"${name}Signed")
  }

  case class ExtractIDCol(field: String, name: String) extends ExtractID
  
  /** Class to extract and convert ID related fields 
   * @param field The input field
   * @param name The new name for input field
   */

  case class ExtractIDArray(field: String, name: String) extends ExtractID {
    // override def newName = name
    /** Method to convert to hexadecimal format */
   override def hex(): Column = arrayToHex(col(field))
      .alias(s"${name}Hex")
    /** Method to convert to unsigned format */
    override def unsigned(): Column = arrayToUnsigned(col(field))
      .alias(s"${name}Unsigned")
  }

  /** Class to extract and convert 2 ID related fields into 1
   * @param field The first input field
   * @param field2 The second input field
   * @param name The new name for input field
   */

  case class ExtractSID(name: String, fields: ExtractID*) {
    // override def newName = name
    /** Method to convert to hexadecimal format */
    def hex(): Column = {
      concat_ws(":", fields.map(_.hex):_*).alias(s"${name}Hex")
    }
    /** Method to convert to unsigned format */
    def unsigned(): Column = {
      concat_ws(":", fields.map(_.unsigned):_*).alias(s"${name}Unsigned")
    }
    /** Method to convert to signed format */
    def signed(): Column = {
      concat_ws(":", fields.map(_.signed):_*).alias(s"${name}Signed")
    }
  }
}
