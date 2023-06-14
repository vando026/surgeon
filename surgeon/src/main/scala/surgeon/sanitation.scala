package conviva.surgeon

import org.apache.spark.sql.{Column}
import org.apache.spark.sql.{functions => F}

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
    (field * F.lit(scale)).cast("Long").alias(suffix)
  }

  /** Convert Unix epoch time to readable time stamp.
   *  @param field The name of the field. 
   *  @param scale A multiplier such as 1000.0 that converts seconds to
   *  milliseconds, or 1.0/1000 that converts milliseconds to seconds.
   *  @param suffix A suffix added to `name` to identify time scale of field.
   *  Typically `Ms` or `Sec`.
   */
  def stamp_(field: Column, scale: Double, suffix: String): Column = {
    F.from_unixtime(field * F.lit(scale)).alias(suffix)
  }

  // trait TimeStamp {
  //   def stamp: Column
  //   val name: String
  //   def hour() = F.hour(stamp()).alias(s"${name}Hour")
  //   def day() = F.dayofmonth(stamp()).alias(s"${name}Day")
  // }

  /** A class for extracting time-based columns in microseconds.
   * @param name The name for the field. 
  */
  class TimeUsCol(col: Column, name: String) extends Column(col.expr) {
      /** Method to return field in milliseconds. */
      def ms() = convert_(col, 1.0/1000, s"${name}Ms")
      /** Method to return field in seconds. */
      def sec() = convert_(col, 1.0/(1000 * 1000), s"${name}Sec")
      /** Method to return the Unix epoch timestamp. */
      def stamp() = stamp_(col, 1.0/(1000 * 1000), s"${name}Stamp")
    }
  
  /** A class for extracting time-based columns in milliseconds.
   * @param name The name of the field.
  */
  class TimeMsCol(col: Column, name: String) extends Column(col.expr) {
      /** Method to return field in seconds. */
      def sec() = convert_(col, 1.0/1000, s"${name}Sec")
      /** Method to return the Unix epoch timestamp. */
      def stamp() = stamp_(col, 1.0/1000, s"${name}Stamp")
      def stampHour() = F.hour(stamp()).alias(s"${name}Hour")
      def stampDay() = F.dayofmonth(stamp()).alias(s"${name}Day")
    }


  /** A class for extracting time-based columns in seconds.
   * @param name The name of the field.
  */
  class TimeSecCol(col: Column, name: String) extends Column(col.expr) {
      def ms() = convert_(col, 1000.0, s"${name}Ms")
      def stamp() = stamp_(col, 1.0, s"${name}Stamp")
    }

  /** Convert value from signed to unsigned. */
  def toUnsigned(x: Int): BigInt =  (BigInt(x >>> 1) << 1) + (x & 1)
  /** UDF to convert signed BigInt to Unsigned BigInt */
  def toUnsignedUDF = F.udf[BigInt, Int](toUnsigned)
  /** Convert all elements in array to unsigned. */
  def arrayToUnsigned(col: Column): Column = {
    F.array_join(F.transform(col, toUnsignedUDF(_)), ":")
  }

  /** Convert integer value to hexadecimal format. */
  def toHexString_(x: Int): String = x.toHexString
  /** UDF to convert Int to hexadecimal format. */
  def toHexStringUDF = F.udf[String, Int](toHexString_)
  /** Convert all elements in an array to hexadecimal format. */
  def arrayToHex(col: Column): Column = {
    F.array_join(F.transform(col, toHexStringUDF(_)), ":")
  }

  /** Class to extract and convert IDs from non-array fields.
   * @param field The input field
   * @param name The new name for input field
   */
  class IdCol(col: Column, name: String) extends Column(col.expr) {
    /** Method to convert to hexadecimal format */
    def hex(): Column = toHexStringUDF(col).alias(s"${name}Hex")
    /** Method to convert to unsigned format */
    def nosign(): Column = toUnsignedUDF(col).alias(s"${name}NoSign")
  }
  
  class IdArray(col: Column, name: String) extends Column(col.expr) {
    /** Method to convert to hexadecimal format */
    def hex(): Column = arrayToHex(col).alias(s"${name}Hex")
    /** Method to convert to unsigned format */
    def nosign(): Column = arrayToUnsigned(col).alias(s"${name}NoSign")
    /** Method concatenates fields unconverted. */
    def asis(): Column = F.concat_ws(":", col).alias(s"${name}")
  }

  /** Class for creating sid5 and sid6 fields. */
  case class SID(name: String, clientId: IdArray, id: IdCol*) {
    /** Method to convert to hexadecimal format */
    def hex(): Column = {
      val cols = clientId.hex +: id.map(_.hex)
      F.concat_ws(":", cols: _*).alias(s"${name}Hex")
    }
    /** Method to convert to unsigned format */
    def nosign(): Column = {
      val cols = clientId.nosign +: id.map(_.nosign)
      F.concat_ws(":", cols: _*).alias(s"${name}NoSign")
    }
    /** Method to concatenate fields asis. */
    def asis(): Column =  F.concat_ws(":", clientId +: id:_*).alias(s"${name}")
  }

  /** Class with methods to operate on arrays. */
  class ArrayCol(col: Column, name: String) extends Column(col.expr) {
    /** Sum all the elements in the array. This methods first removes all Null
      *  values then does a sum reduce. */
    def sumInt(): Column = {
      F.aggregate(F.filter(col, x => x.isNotNull),
        F.lit(0), (x, y) => x.cast("int")  + y.cast("int"))
        .alias(s"${name}Sum")
    }
    /** Remove nulls, keep the same name. */
    def dropNull(): Column = {
      F.filter(col, x => x.isNotNull)
        .alias(s"${name}")
    }
    /** Are all elements in the array null. */   
    def allNull(): Column = {
      import org.apache.spark.sql.{functions => F}
      F.when(F.size(F.filter(col, x => x.isNotNull)) === 0, true)
       .otherwise(false).alias(s"${name}AllNull")
    }
    /** Return only distinct elements in array. Removes nulls. */
    def distinct(): Column = {
      F.array_distinct(F.filter(col, x => x.isNotNull))
        .alias(s"${name}Distinct")
    }
    /** Return first non null element in array. */
    def first(): Column = {
        F.filter(col, x => x.isNotNull)(0).alias(s"${name}First")
    }
    /** Return last element in array, with null elements removed. */
    def last(): Column = {
      col.apply(F.size(F.filter(col, x => x.isNotNull))
        .minus(1)).alias(s"${name}Last")
    }
    /** Return minimum value in array. */
    def min(): Column = F.array_min(col).alias(s"${name}Min")
    /** Return maximum value in array. */
    def max(): Column = F.array_max(col).alias(s"${name}Max")
    /** Return true if element in array contains value. */
    def has(value: Any): Column = {
      F.array_contains(col, value).alias(s"${name}HasVal")
    }
  }


}
