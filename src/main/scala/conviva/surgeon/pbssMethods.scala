package conviva.surgeon

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{col, udf, when, from_unixtime, lit}
import conviva.surgeon.Sanitize._

trait ParseTime {
  def scale1 = lit(1)
  def scale2 = lit(1/1000)
}

case class ParseTimeMs(df: DataFrame, field: String, name: String)  {
  def ms(): DataFrame = {
    df.withColumn(s"${name}Ms", col(field) * scale1)
  }
  def sec(): DataFrame = {
    df.withColumn(s"${name}Sec", col(field) * scale2)
  }
  def timestamp(): DataFrame = {
    df.withColumn(s"${name}", from_unixtime(col(field) * scale2))
  }
}

/**
 * SQL and UDF Methods to create columns from the PbSS hourly, daily and monthly data. 
 * These methods can be chained to make the code for reading in columns more
 * concise. 
 * @define clientId The clientID assigned to the client by Conviva
 * @define sessionId The sessionId assigned to the session by Conviva
 * @define timestamp in seconds, milliseconds, or timestamp
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
   * @example {{{
   * df.customerId.clientIdHex.hasEnded.justJoined
   * }}}
   */
  implicit class pbssMethods(df: DataFrame) {
    /** Create the customerId column.
     */ 
    def customerId(): DataFrame = {
      df.withColumn("customerId", col("key.sessId.customerId"))
    }
    /** Create the clientId column in unsigned format.
     */ 
    def clientIdUnsigned(): DataFrame = {
      df.withColumn("clientId", 
        toClientIdUnsigned(col("key.sessId.clientId.element")))
    }
    /** Create the clientId column in hexadecimal format.
     */ 
    def clientIdHex(): DataFrame = {
      df.withColumn("clientId", 
        toClientIdHex(col("key.sessId.clientId.element")))
    }
    /** Create the SID5 column in hexadecimal format.
     */ 
    def sid5Hex(): DataFrame = {
      df.withColumn("sid5", toSid5Hex(
        col("key.sessId.clientId.element"), 
        col("key.sessId.clientSessionId")))
    }
    /** Create the SID5 column in unsigned format.
     */ 
    def sid5Unsigned(): DataFrame = {
      df.withColumn("sid5", toSid5Unsigned(
        col("key.sessId.clientId.element"), 
        col("key.sessId.clientSessionId")))
    }
    /** Create the shouldProcess column.
     */ 
    def shouldProcess(): DataFrame = {
      df.where(col("val.sessSummary.shouldProces"))
    }
    /** Create the hasEnded column.
     */ 
    def hasEnded(): DataFrame = {
      df.withColumn("hasEnded", col("val.sessSummary.hasEnded"))
    }
    /** Create the justEnded column.
     */ 
    def justEnded(): DataFrame = {
      df.withColumn("justEnded", col("val.sessSummary.justEnded"))
    }
    /** Create the justEnded column.
     */ 
    def endedStatus(): DataFrame = {
      df.withColumn("endedStatus",  col("val.sessSummary.endedStatus"))
    }
    /** Create the joinState column.
     */ 
    def joinState(): DataFrame = {
      df.withColumn("joinState", col("val.sessSummary.joinState"))
    }
    /** Create a column with valid joinTimes (in milliseconds) if the session is a valid join.
     *  The logic is:
     *  if (joinState > 0 | (joinState = -4 & joinTimeMs != 3) then joinTimeMs else null)
     * The source for this logic needs to be cited. 
     */
    def validJoinTimesMs(): DataFrame = { 
      df.withColumn("joinTimeMsValid", 
        when((col("val.sessSummary.joinState") > 0)
          .or((col("val.sessSummary.joinState") === -4)
          .and(col("val.sessSummary.joinTimeMs") =!= -3)),
        col("val.sessSummary.joinTimeMs")).otherwise(null))
    }
    /**
      * Create a column indicating if session is an AdSession or
      * ContentSession.
      */
    def isAd(): DataFrame = {
      df.withColumn("sessionType", when(col("val.invariant.summarizedTags")
        .getItem("c3.video.isAd") === "T", "adSession").otherwise("contentSession"))
    }
    /**
      * Creates the intvStartTime column in seconds or milliseconds.
      * @param seconds If true, the column returns `intvStartTimeSec` in
      * seconds else it returns `intvStartTimeMs` in milliseconds. 
      * @example {{{
      * df.intvStartTime(seconds = false)
      * }}}
      */
    def intvStartTime(seconds: Boolean = true): DataFrame = {
      val unit = if (seconds) 1 else 1000
      val name = if (seconds) "intvStartTimeSec" else "intvStartTimeMs"
      df.withColumn(name, 
          col("val.sessSummary.intvStartTimeSec") * unit)
    }
    /**
      * Parse the lifeFirstRecvTime column $timestamp.
      * @example {{{
      * df.lifeFirstRecvTime.sec // seconds
      * df.lifeFirstRecvTime.ms // milliseconds
      * df.lifeFirstRecvTime.timestamp 
      * }}}
      */
      def lifeFirstRecvTime = ParseTimeMs(df, 
        "val.sessSummary.lifeFirstRecvTimeMs", "lifeFirstRecvTime")

    /**
      * Parse the firstRecvTime column $timestamp
      * @example {{{
      * df.lifeFirstRecvTime.ms
      * df.lifeFirstRecvTime.sec
      * df.lifeFirstRecvTime.timestamp
      * }}}
      */
    def firstRecvTime = ParseTimeMs(df, "key.firstRecvTimeMs", 
        "firstRecvTime")

    /**
      * Parse the lastRecvTime column $timestamp.
      * @example {{{
      * df.lifeFirstRecvTime.ms
      * df.lifeFirstRecvTime.sec
      * df.lifeFirstRecvTime.timestamp
      * }}}
      */
    def lastRecvTime = ParseTimeMs(df, "val.sessSummary.lastRecvTimeMs", 
      "lastRecvTime") 

    /**
      * Creates the sessionCreationTime column $timestamp.
      * @example {{{
      * df.lifeFirstRecvTime.ms
      * df.lifeFirstRecvTime.sec
      * df.lifeFirstRecvTime.timestamp
      * }}}
      */
    def sessionCreationTime = ParseTimeMs(df, 
      "val.invariant.sessionCreationTimeMs", "sessionCreationTime")
  }
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


