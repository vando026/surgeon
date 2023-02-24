package conviva.surgeon

import org.apache.spark.sql.functions.{
  col, udf, array_join, transform, lower, 
  concat_ws, conv, from_unixtime, when, lit
}
import org.apache.spark.sql.{Column}

/** An object with traits and case classes to create objects named
 *  after fields that have their own methods.
*/
object Sanitize {

  class hex(val c: Column) {
    def dome = c * 10
  }
  /** A trait to extract a field, name it, and give it a default method called
   *  `asis`. 
  */
  trait ExtractCol {
    /** The input field. */
    def field: String
    /** Name for new field. */
    def name: String
    /** Extract the field as is with  the last name. */
    def asis(): Column = col(field).alias(s"$name")
  }

  /** A trait for extracting time-based columns such as `firstRecvTimeMs`. */
  trait ExtractColTime extends ExtractCol {
    /** Convert to Unix epoch time to a different timescale.
     *  @param scale A multiplier such as 1000.0 that converts seconds to
     *  milliseconds, or 1.0/1000 that converts milliseconds to seconds. 
     *  @param suffix A suffix added to `name` to identify time scale of field.
     *  Typically `Ms` or `Sec`.
     */
    def convert(scale: Double, suffix: String): Column = {
      (col(field) * lit(scale)).cast("Long").alias(s"${name}${suffix}")
    }
    /** Convert Unix epoch time to readable time stamp.
     *  @param scale A multiplier such as 1000.0 that converts seconds to
     *  milliseconds, or 1.0/1000 that converts milliseconds to seconds.
     */
    def stamp_(scale: Double): Column = {
      from_unixtime(col(field) * lit(scale)).alias(s"${name}Stamp")
    }
  }
  
  /** A class for extracting time-based columns in milliseconds.
   * @param field The input field.
   * @param name The new name for the field. 
  */
  case class ExtractColMs(
      field: String, name: String
    ) extends ExtractColTime {
      /** Method to return field in milliseconds. */
      def ms() = convert(1.0, "Ms")
      /** Method to return field in seconds. */
      def sec() = convert(1.0/1000, "Sec")
      /** Method to return the Unix epoch timestamp. */
      def stamp() = stamp_(1.0/1000)
    }

  /** A class for extracting time-based columns in seconds.
   * @param field The input field.
   * @param name The new name for the field. 
  */
  case class ExtractColSec(
      field: String, name: String
    ) extends ExtractColTime {
      def ms() = convert(1000.0, "Ms")
      def sec() = convert(1.0, "Sec")
      def stamp() = stamp_(1.0)
    }

  /** Convert value from signed to unsigned. */
  def toUnsigned(x: Int): BigInt =  (BigInt(x >>> 1) << 1) + (x & 1)
  /** UDF to convert signed BigInt to Unsigned BigInt */
  def toUnsignedUDF = udf[BigInt, Int](toUnsigned)
  /** Convert all elements in array to unsigned. */
  def arrayToUnsigned(col: Column): Column = {
    array_join(transform(col, toUnsignedUDF(_)), ":")
  }

  /** Convert integer value to hexadecimal format. */
  def toHexString_(x: Int): String = x.toHexString
  /** UDF to convert Int to hexadecimal format. */
  def toHexStringUDF = udf[String, Int](toHexString_)
  /** Convert all elements in an array to hexadecimal format. */
  def arrayToHex(col: Column): Column = {
    array_join(transform(col, toHexStringUDF(_)), ":")
  }

  /** Trait to extract and create ID fields. */
  trait ExtractID extends ExtractCol {
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

  /** Class to extract and convert IDs from non-array fields.
   * @param field The input field
   * @param name The new name for input field
   */

  case class ExtractIDCol(field: String, name: String) extends ExtractID
  
  /** Class to extract and convert IDs from arrays, such as `cliendId`. 
   * @param field The input field
   * @param name The new name for input field
   */
  case class ExtractIDArray(field: String, name: String) extends ExtractID {
    /** Method to convert to hexadecimal format */
   override def hex(): Column = arrayToHex(col(field))
      .alias(s"${name}Hex")
    /** Method to convert to unsigned format */
    override def unsigned(): Column = arrayToUnsigned(col(field))
      .alias(s"${name}Unsigned")
  }

  /** Class for creating sid5 and sid6 fields. */
  case class ExtractSID(name: String, fields: ExtractID*) {
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
