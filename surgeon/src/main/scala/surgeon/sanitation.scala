package conviva.surgeon

import org.apache.spark.sql.functions.{
  col, udf, array_join, transform, lower, 
  concat_ws, conv, from_unixtime, when, lit,
  aggregate, filter, size, array_min, array_max, 
  array_distinct, array_position, array_contains
}
import org.apache.spark.sql.{Column}

/** An object with traits and case classes to create objects named
 *  after fields that have their own methods.
*/
object Sanitize {

  /** A trait to extract a field, name it, and give it a default method called
   *  `asis`. 
  */
  trait AsCol {
    /** The input field. */
    def field: Column
    /** Name for new field. */
    def name: String
    /** Extract the field as is with  the last name. */
    def asis(): Column = field.alias(s"$name")
  }

  /** A trait for extracting time-based columns such as `firstRecvTimeMs`. */
  trait TimeCol extends AsCol {
    /** Convert to Unix epoch time to a different timescale.
     *  @param scale A multiplier such as 1000.0 that converts seconds to
     *  milliseconds, or 1.0/1000 that converts milliseconds to seconds. 
     *  @param suffix A suffix added to `name` to identify time scale of field.
     *  Typically `Ms` or `Sec`.
     */
    def convert(scale: Double, suffix: String): Column = {
      (field * lit(scale)).cast("Long").alias(s"${name}${suffix}")
    }
    /** Convert Unix epoch time to readable time stamp.
     *  @param scale A multiplier such as 1000.0 that converts seconds to
     *  milliseconds, or 1.0/1000 that converts milliseconds to seconds.
     */
    def stamp_(scale: Double): Column = {
      from_unixtime(field * lit(scale)).alias(s"${name}Stamp")
    }
  }

  /** A class for extracting time-based columns in microseconds.
   * @param field The input field.
   * @param name The new name for the field. 
  */
  case class TimeUsCol(
      field: Column, name: String
    ) extends TimeCol {
      /** Method to return field in milliseconds. */
      def ms() = convert(1.0/1000, "Ms")
      /** Method to return field in seconds. */
      def sec() = convert(1.0/(1000 * 1000), "Sec")
      /** Method to return the Unix epoch timestamp. */
      def stamp() = stamp_(1.0/(1000 * 1000))
    }
  
  /** A class for extracting time-based columns in milliseconds.
   * @param field The input field.
   * @param name The new name for the field. 
  */
  case class TimeMsCol(
      field: Column, name: String
    ) extends TimeCol {
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
  case class TimeSecCol(
      field: Column, name: String
    ) extends TimeCol {
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
  trait IdColTrait extends AsCol {
    /** Method to convert to hexadecimal format */
    def hex(): Column = toHexStringUDF(field)
      .alias(s"${name}Hex")
    /** Method to convert to unsigned format */
    def unsigned(): Column = toUnsignedUDF(field)
      .alias(s"${name}Unsigned")
    /** Method to convert to signed format */
    def signed(): Column = concat_ws(":", field)
      .alias(s"${name}Signed")
    /** Method concatenates fields unconverted. */
    override def asis(): Column = concat_ws(":", field)
      .alias(s"${name}")
  }

  /** Class to extract and convert IDs from non-array fields.
   * @param field The input field
   * @param name The new name for input field
   */

  case class IdCol(field: Column, name: String) extends IdColTrait
  
  /** Class to extract and convert IDs from arrays, such as `cliendId`. 
   * @param field The input field
   * @param name The new name for input field
   */
  case class IdArray(field: Column, name: String) extends IdColTrait {
    /** Method to convert to hexadecimal format */
    override def hex(): Column = arrayToHex(field)
      .alias(s"${name}Hex")
    /** Method to convert to unsigned format */
    override def unsigned(): Column = arrayToUnsigned(field)
      .alias(s"${name}Unsigned")
  }

  /** Class for creating sid5 and sid6 fields. */
  case class SID(name: String, fields: IdColTrait*) {
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
    /** Methods to concatenate fields as is. */
    def asis(): Column = {
      concat_ws(":", fields.map(_.asis):_*).alias(s"${name}")
    }
  }

  /** Class with methods to operate on arrays. */
  case class ArrayCol(field: Column, name: String) extends AsCol {
    /** Sum all the elements in the array. This methods first removes all Null
      *  values then does a sum reduce. */
    def sum(): Column = {
      aggregate(filter(field, x => x.isNotNull),
        lit(0), (x, y) => x  + y)
        .alias(s"${name}Sum")
    }
    /** Remove nulls, keep the same name. */
    def notNull(): Column = {
      filter(field, x => x.isNotNull)
        .alias(s"${name}")
    }
    /** Are all elements in the array null. */   
    def allNull(): Column = {
      when(size(filter(field, x => x.isNotNull)) === 0, true)
       .otherwise(false).alias(s"${name}AllNull")
    }
    /** Return only distinct elements in array. Removes nulls. */
    def distinct(): Column = {
      array_distinct(filter(field, x => x.isNotNull))
        .alias(s"${name}Distinct")
    }
    /** Return first non null element in array. */
    def first(): Column = {
        filter(field, x => x.isNotNull)(0).alias(s"${name}First")
    }
    /** Return last element in array, with null elements removed. */
    def last(): Column = {
      field.apply(size(filter(field, x => x.isNotNull))
        .minus(1)).alias(s"${name}Last")
    }
    /** Return minimum value in array. */
    def min(): Column = array_min(field).alias(s"${name}Min")
    /** Return maximum value in array. */
    def max(): Column = array_max(field).alias(s"${name}Max")
    /** Return true if the array contains a value. */
    def contains(value: String): Column = {
      array_contains(field, value).alias(s"${name}Match")
    }
  }

}
