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

  /** Convert to Unix epoch time to a different timescale.
   *  @param field The name of the field. 
   *  @param scale A multiplier such as 1000.0 that converts seconds to
   *  milliseconds, or 1.0/1000 that converts milliseconds to seconds. 
   *  @param suffix A suffix added to `name` to identify time scale of field.
   *  Typically `Ms` or `Sec`.
   */
  def convert_(field: Column, scale: Double, suffix: String): Column = {
    (field * lit(scale)).cast("Long").alias(suffix)
  }

  /** Convert Unix epoch time to readable time stamp.
   *  @param field The name of the field. 
   *  @param scale A multiplier such as 1000.0 that converts seconds to
   *  milliseconds, or 1.0/1000 that converts milliseconds to seconds.
   *  @param suffix A suffix added to `name` to identify time scale of field.
   *  Typically `Ms` or `Sec`.
   */
  def stamp_(field: Column, scale: Double, suffix: String): Column = {
    from_unixtime(field * lit(scale)).alias(suffix)
  }

  /* Method to extract last component of name.
   * @param name The name for the field. 
  */ 
  def getName(name: String) = name.split("\\.").last

  /** A class for extracting time-based columns in microseconds.
   * @param name The name for the field. 
  */
  class TimeUsCol(field: String, name: String) extends Column(field) {
      /** Method to return field in milliseconds. */
      def ms() = convert_(this, 1.0/1000, s"${name}Ms")
      /** Method to return field in seconds. */
      def sec() = convert_(this, 1.0/(1000 * 1000), s"${name}Sec")
      /** Method to return the Unix epoch timestamp. */
      def stamp() = stamp_(this, 1.0/(1000 * 1000), s"${name}Stamp")
    }
  
  /** A class for extracting time-based columns in milliseconds.
   * @param name The name of the field.
  */
  class TimeMsCol(field: String, name: String) extends Column(field) {
      /** Method to return field in seconds. */
      def sec() = convert_(this, 1.0/1000, s"${name}Sec")
      /** Method to return the Unix epoch timestamp. */
      def stamp() = stamp_(this, 1.0/1000, s"${name}Stamp")
    }


  /** A class for extracting time-based columns in seconds.
   * @param name The name of the field.
  */
  class TimeSecCol(field: String, name: String) extends Column(field) {
      def ms() = convert_(this, 1000.0, s"${name}Ms")
      def stamp() = stamp_(this, 1.0, s"${name}Stamp")
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

  /** Class to extract and convert IDs from non-array fields.
   * @param field The input field
   * @param name The new name for input field
   */
  class IdCol(field: String, name: String) extends Column(field) {
    /** Method to convert to hexadecimal format */
    def hex(): Column = toHexStringUDF(this).alias(s"${name}Hex")
    /** Method to convert to unsigned format */
    def nosign(): Column = toUnsignedUDF(this).alias(s"${name}NoSign")
  }
  
  class IdArray(field: String, name: String) extends Column(field) {
    /** Method to convert to hexadecimal format */
    def hex(): Column = arrayToHex(this).alias(s"${name}Hex")
    /** Method to convert to unsigned format */
    def nosign(): Column = arrayToUnsigned(this).alias(s"${name}NoSign")
    /** Method concatenates fields unconverted. */
    def asis(): Column = concat_ws(":", this).alias(s"${name}")
  }

  /** Class for creating sid5 and sid6 fields. */
  case class SID(name: String, clientId: IdArray, id: IdCol*) {
    /** Method to convert to hexadecimal format */
    def hex(): Column = {
      val cols = clientId.hex +: id.map(_.hex)
      concat_ws(":", cols: _*).alias(s"${name}Hex")
    }
    /** Method to convert to unsigned format */
    def nosign(): Column = {
      val cols = clientId.nosign +: id.map(_.nosign)
      concat_ws(":", cols: _*).alias(s"${name}NoSign")
    }
    /** Method to concatenate fields asis. */
    def asis(): Column =  concat_ws(":", clientId +: id:_*).alias(s"${name}")
  }

  /** Class with methods to operate on arrays. */
  class ArrayCol(field: String, name: String) extends Column(field) {
    /** Sum all the elements in the array. This methods first removes all Null
      *  values then does a sum reduce. */
    def sumInt(): Column = {
      aggregate(filter(this, x => x.isNotNull),
        lit(0), (x, y) => x.cast("int")  + y.cast("int"))
        .alias(s"${name}Sum")
    }
    /** Remove nulls, keep the same name. */
    def notNull(): Column = {
      filter(this, x => x.isNotNull)
        .alias(s"${name}")
    }
    /** Are all elements in the array null. */   
    def allNull(): Column = {
      when(size(filter(this, x => x.isNotNull)) === 0, true)
       .otherwise(false).alias(s"${name}AllNull")
    }
    /** Return only distinct elements in array. Removes nulls. */
    def distinct(): Column = {
      array_distinct(filter(this, x => x.isNotNull))
        .alias(s"${name}Distinct")
    }
    /** Return first non null element in array. */
    def first(): Column = {
        filter(this, x => x.isNotNull)(0).alias(s"${name}First")
    }
    /** Return last element in array, with null elements removed. */
    def last(): Column = {
      this.apply(size(filter(this, x => x.isNotNull))
        .minus(1)).alias(s"${name}Last")
    }
    /** Return minimum value in array. */
    def min(): Column = array_min(this).alias(s"${name}Min")
    /** Return maximum value in array. */
    def max(): Column = array_max(this).alias(s"${name}Max")
    /** Return true if the array contains a value. */
    def contains(value: String): Column = {
      array_contains(this, value).alias(s"${name}Match")
    }
  }


}
