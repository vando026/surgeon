package conviva.surgeon

/** An object with traits and case classes to create objects named
 *  after fields that have their own methods.
*/
object Sanitize {

  import org.apache.spark.sql.{Column}
  import org.apache.spark.sql.{functions => F}
  import conviva.surgeon.GeoInfo._

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

  /** A class for extracting time-based columns in microseconds.
   * @param name The name for the field. 
  */
  class TimeUsCol(col: Column, name: String) extends Column(col.expr) {
      /** Method to return field in milliseconds. */
      def toMs() = convert_(col, 1.0/1000, s"${name}Ms")
      /** Method to return field in seconds. */
      def toSec() = convert_(col, 1.0/(1000 * 1000), s"${name}Sec")
      /** Method to return the Unix epoch timestamp. */
      def stamp() = stamp_(col, 1.0/(1000 * 1000), s"${name}Stamp")
    }
  
  /** A class for extracting time-based columns in milliseconds.
   * @param name The name of the field.
  */
  class TimeMsCol(col: Column, name: String) extends Column(col.expr) {
      /** Method to return field in seconds. */
      def toSec() = convert_(col, 1.0/1000, s"${name}Sec")
      /** Method to return the Unix epoch timestamp. */
      def stamp() = stamp_(col, 1.0/1000, s"${name}Stamp")
    }


  /** A class for extracting time-based columns in seconds.
   * @param name The name of the field.
  */
  class TimeSecCol(col: Column, name: String) extends Column(col.expr) {
      def toMs() = convert_(col, 1000.0, s"${name}Ms")
      def stamp() = stamp_(col, 1.0, s"${name}Stamp")
    }

  /** Convert value from signed to unsigned. */
  def toUnsigned(x: Int): BigInt =  (BigInt(x >>> 1) << 1) + (x & 1)
  /** UDF to convert signed BigInt to Unsigned BigInt */
  def toUnsignedUDF = F.udf[BigInt, Int](toUnsigned)
  /** Convert all elements in array to unsigned. */
  def arrayToUnsigned(col: Column, sep: String = ":"): Column = {
    F.array_join(F.transform(col, toUnsignedUDF(_)), sep)
  }

  /** Convert integer value to hexadecimal format. */
  def toHexString_(x: Int): String = x.toHexString
  /** UDF to convert Int to hexadecimal format. */
  def toHexStringUDF = F.udf[String, Int](toHexString_)
  /** Convert all elements in an array to hexadecimal format. */
  def arrayToHex(col: Column, sep: String = ":"): Column = {
    F.array_join(F.transform(col, toHexStringUDF(_)), sep)
  }

  /** Convert bytes to hexadecimal format. */
  def bytesToHex(bytes: Seq[Short]): String = {
      val sb = new StringBuilder
      for (b <- bytes) {
          sb.append(String.format("%02x", Short.box(b)))
      }
      sb.toString
  }
  def bytesToHexUDF = F.udf[String, Seq[Short]](bytesToHex)

  /** Class to extract and convert IDs from non-array fields.
   * @param field The input field
   * @param name The new name for input field
   */
  class IdCol(col: Column, name: String) extends Column(col.alias(name).expr) {
    /** Method to convert to hexadecimal format */
    def toHex(): Column = toHexStringUDF(col).alias(s"${name}Hex")
    /** Method to convert to unsigned format */
    def toUnsigned(): Column = toUnsignedUDF(col).alias(s"${name}Unsigned")
  }
  
  class IdArray(col: Column, name: String) extends Column(col.expr) {
    /** Method to convert to hexadecimal format */
    def concatToHex(): Column = arrayToHex(col).alias(s"${name}Hex")
    /** Method to convert to unsigned format */
    def concatToUnsigned(): Column = arrayToUnsigned(col).alias(s"${name}Unsigned")
    /** Method concatenates fields unconverted. */
    def concat(): Column = F.concat_ws(":", col).alias(s"${name}")
  }

  /** Class for creating sid5 and sid6 fields. */
  case class SID(name: String, clientId: IdArray, id: IdCol*) {
    /** Method to convert to hexadecimal format */
    def concatToHex(): Column = {
      val cols = clientId.concatToHex +: id.map(_.toHex)
      F.concat_ws(":", cols: _*).alias(s"${name}Hex")
    }
    /** Method to convert to unsigned format */
    def concatToUnsigned(): Column = {
      val cols = clientId.concatToUnsigned +: id.map(_.toUnsigned)
      F.concat_ws(":", cols: _*).alias(s"${name}Unsigned")
    }
    /** Method to concatenate fields asis. */
    def concat(): Column =  F.concat_ws(":", clientId +: id:_*).alias(s"${name}")
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

  class GeoCol(col: Column, field: String, labels: Map[Int, String]) extends Column(col.expr) {
    def label(): Column  = {
      val gLit: Column = F.typedLit(labels) 
      gLit(col).alias(s"${field}Label")
    }
  }

  /** Recode `c3 Video.is.Ad` field. */
  class c3isAd(col: Column) extends Column(col.expr) {
    def recode(): Column = {
      F.when(F.lower(col).isin("true", "t"), true)
      .when(F.lower(col).isin("false", "f"), false)
      .otherwise(null)
      .alias("c3_isAd_rc")
    }
  }

  /** Class for formating IPV6. */
  class IP6(col: Column, name: String) extends Column(col.alias("publicipv6").expr) {
      /** Method to convert to hexadecimal format and concatenate*/
      def concatToHex(): Column = {
        val hexArray = bytesToHexUDF(col)
        // Split into groups of 4
        val strSplit = F.split(hexArray, "(?<=\\G....)")
        // remove empty strings
        val dropEmpty = F.array_remove(strSplit, "")
        F.array_join(dropEmpty, ":").alias(s"${name}Hex")
      }
      def concat(): Column = {
        F.array_join(col, ":").alias(s"${name}")
      }
  }

  /** Class for formating IPV4. */
  class IP4(col: Column, name: String) extends Column(col.alias("publicIp").expr) {
      def concat(): Column = {
        F.array_join(col, ":").alias(s"${name}")
      }
  }
}
