package conviva.surgeon

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions.{col, udf, when, from_unixtime, lit}
import conviva.surgeon.Sanitize._

// val dat = spark.read.parquet("data/pbssHourly1.parquet")

trait ExtractCol {
  def field: String
  def suffix(s: String) = s.split("\\.").last
  def asis(): Column = col(field).alias(suffix(field))
}

trait ExtractColTime extends ExtractCol {
  def name: String
  val val1 = lit(1000)
  val val2= lit(1)
  def ms(): Column = {
    (col(field) * val2).cast("Long").alias(s"${name}Ms")
  }
  def sec(): Column = {
    (col(field)  / val1).cast("Long").alias(s"${name}Sec")
  }
  def stamp(): Column = {
    from_unixtime(col(field) / val1).alias(s"${name}Stamp")
  }
}

case class ExtractColAs(
    field: String
  ) extends ExtractCol

case class ExtractColMs(
    field: String, 
    name: String
  ) extends ExtractColTime

case class ExtractColSec(
    field: String, 
    name: String
  ) extends ExtractColTime {
    override val val1 = lit(1)
    override val val2 = lit(1000)
  }
  
/**
 * SQL and UDF Methods to create columns from the PbSS hourly, daily and monthly data. 
 * These methods can be chained to make the code for reading in columns more
 * concise. 
 * @define clientId The clientID assigned to the client by Conviva
 * @define sessionId The sessionId assigned to the session by Conviva
 * @example {{{
 * df.customerId.clientIdHex.hasEnded.justJoined
 *   .intvStartTime.ms
 * }}}
 */

object PbSS {
  /**
   * SQL and UDF Methods to create columns from the PbSS hourly, daily and monthly data. 
   * These methods can be chained to make the code for reading in columns more
   * concise. 
   * @define timestamp in seconds, milliseconds, or timestamp
   * @example {{{
   * df.customerId.clientIdHex.hasEnded.justJoined
   * }}}
   */
  // implicit class pbssMethods(df: DataFrame) {

    /** Get the customerId column as is. */ 
    def customerId() = ExtractColAs("key.sessId.customerId")

    /** Get clientSessionId as is. */ 
    def sessionId() = ExtractColAs("key.sessId.clientSessionId")

    /** Create the clientId column in unsigned format. */ 
    def clientIdUnsigned() = toClientIdUnsigned(
        col("key.sessId.clientId.element"))
      .alias("clientIdUnsigned")

    /** Create the clientId column in hexadecimal format. */ 
    def clientIdHex() = toClientIdHex(
          col("key.sessId.clientId.element"))
        .alias("clientIdHex")

    /** Create the SID5 column in hexadecimal format. */ 
    def sid5Hex() = toSid5Hex(
        col("key.sessId.clientId.element"),
        col("key.sessId.clientSessionId"))
      .alias("sid5Hex")

    /** Create the SID5 column in unsigned format. */ 
    def sid5Unsigned = toSid5Unsigned(
        col("key.sessId.clientId.element"), col("key.sessId.clientSessionId"))
      .alias("sid5Unsigned")

    /** Get the shouldProcess column. */ 
    def shouldProcess() = ExtractColAs("val.sessSummary.shouldProces")

    /** Get the hasEnded column. */ 
    def hasEnded() = ExtractColAs("val.sessSummary.hasEnded")

    /** Get the justEnded column. */ 
    def justEnded() = ExtractColAs("val.sessSummary.justEnded")

    /** Get the endedStatus column. */ 
    def endedStatus() = ExtractColAs("val.sessSummary.endedStatus")

    /** Get the joinState column. */ 
    def joinState() = ExtractColAs("val.sessSummary.joinState")

    /** Create a column with valid joinTimes (in milliseconds) if the session is a valid join.
     *  The logic is:
     *  if (joinState > 0 | (joinState = -4 & joinTimeMs != 3) then joinTimeMs else null)
     * The source for this logic needs to be cited. 
    def validJoinTimesMs(): DataFrame = { 
      df.withColumn("joinTimeMsValid", 
        when((col("val.sessSummary.joinState") > 0)
          .or((col("val.sessSummary.joinState") === -4)
          .and(col("val.sessSummary.joinTimeMs") =!= -3)),
        col("val.sessSummary.joinTimeMs")).otherwise(null))
    }
    */

    /**
      * Create a column indicating if session is an AdSession or
      * ContentSession.
    def isAd(): DataFrame = {
      df.withColumn("sessionType", when(col("val.invariant.summarizedTags")
        .getItem("c3.video.isAd") === "T", "adSession").otherwise("contentSession"))
    }
    */

    /**
      * Creates the intvStartTime column $timestamp.
      * @example {{{
      * df.select(
      *   intvStartTime.asis, 
      *   intvStartTime.ms,
      *   intvStartTime.sec,
      *   intvStartTime.stamp)
      * }}}
      */
    def intvStartTime = ExtractColSec(
      field = "val.sessSummary.intvStartTimeSec", name = "intvStartTime")

    /**
      * Parse the lifeFirstRecvTime column $timestamp.
      * @example {{{
      * df.lifeFirstRecvTime.sec // seconds
      * df.lifeFirstRecvTime.ms // milliseconds
      * df.lifeFirstRecvTime.timestamp 
      * }}}
      */
      def lifeFirstRecvTime = ExtractColMs( 
        field = "val.sessSummary.lifeFirstRecvTimeMs", name = "lifeFirstRecvTime")

    /**
      * Parse the firstRecvTime column $timestamp
      * @example {{{
      * df.firstRecvTime.ms
      * df.firstRecvTime.sec
      * df.firstRecvTime.timestamp
      * }}}
      */
    def firstRecvTime = ExtractColMs(
      field = "key.firstRecvTimeMs", name = "firstRecvTime")

    /**
      * Parse the lastRecvTime column $timestamp.
      * @example {{{
      * df.lastRecvTime.ms
      * df.lastRecvTime.sec
      * df.lastRecvTime.timestamp
      * }}}
      */
    def lastRecvTime = ExtractColMs(
      field = "val.sessSummary.lastRecvTimeMs", name = "lastRecvTime") 

    /**
      * Creates the sessionCreationTime column $timestamp.
      * @example {{{
      * df.sessionCreationTime.ms
      * df.sessionCreationTime.sec
      * df.sessionCreationTime.timestamp
      * }}}
      */
    def sessionCreationTime = ExtractColMs( 
      field = "val.invariant.sessionCreationTimeMs", name = "sessionCreationTime")
  // }
}

// object readcsv {
//   def main(path: String): Unit = {
//     val spark = SparkSession.builder
//       .master("local[1]")
//       .appName("Simple Application").getOrCreate()
//     val dat = spark.read.csv(path).limit(10)
//     println("=========> This might have worked")
//     dat.show()
//     spark.stop()
//   }
// }

// readcsv.main("~/Workspace/tmp/mm68.csv")
// val tt =  spark.read.parquet("/Users/avandormael/Workspace/tmp/data")

/** consistency between joinTimeMs, joinState and lifePlayingTimeMs
consistent combinations are:
|isJoinTimeMs|joinState|isLifePlayingTimeMs|
|-1          |-1       |0                  | didn't join, zero life playing time
|1           |1        |1                  | joined, known join time, positive life playing time
|-3          |0        |1                  | joined, unknown join time, positive life playing time
Any other combination is inconsistent.

Fox-DCG
+-------------+------------+---------+-------------------+-------+-------+------------+
|shouldProcess|isJoinTimeMs|joinState|isLifePlayingTimeMs|sessCnt|sessPct|isConsistent|
+-------------+------------+---------+-------------------+-------+-------+------------+
|false        |-3          |0        |0                  |25     |0.0022 |0           |
|false        |-1          |-1       |0                  |637051 |56.7207|1           |
|false        |-1          |0        |0                  |387    |0.0345 |0           |
|true         |-3          |0        |1                  |73963  |6.5854 |1           |
|true         |-3          |1        |1                  |44     |0.0039 |0           |
|true         |-1          |-1       |0                  |147219 |13.1078|1           |
|true         |-1          |0        |0                  |1      |1.0E-4 |0           |
|true         |-1          |0        |1                  |223    |0.0199 |0           |
|true         |0           |0        |1                  |257    |0.0229 |0           |
|true         |1           |1        |1                  |263967 |23.5027|1           |
+-------------+------------+---------+-------------------+-------+-------+------------+
testSDF.groupBy("shouldProcess", "isJoinTimeMs", "joinState", "isLifePlayingTimeMs").agg(count("*").as("sessCnt"))
.withColumn("sessPct", round(lit(100.0)*$"sessCnt"/lit(totSessCnt),4))
.withColumn(
  "isConsistent", 
  when($"isJoinTimeMs" === -1 && $"joinState" === -1 && $"isLifePlayingTimeMs" === 0, 1) 
  .when($"isJoinTimeMs" === 1 && $"joinState" === 1 && $"isLifePlayingTimeMs" === 1, 1)
  .when($"isJoinTimeMs" === -3 && $"joinState" === 0 && $"isLifePlayingTimeMs" === 1, 1) 
  .otherwise(0)
)
.sort("shouldProcess", "isJoinTimeMs", "joinState", "isLifePlayingTimeMs").toShow()
*/ 


