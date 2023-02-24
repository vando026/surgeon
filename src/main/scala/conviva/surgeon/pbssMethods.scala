package conviva.surgeon

import conviva.surgeon.Sanitize._
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{Column}
  
/**
 * Perform operations on the PbSS hourly, daily and monthly data. The main
 * operation is to select columns from the data. Objects are named after
 * fields (e.g., customerId) and have custom methods (e.g. intvStartTime.ms,
 * which converts intvStartTimeSec to milliseconds). The Objects return type
 * Column, so you can invoke Column.methods on the result (e.g.,
 * dat.select(shouldProcess.asis.alias("myNewName")).
 * @define clientId The clientID assigned to the client by Conviva
 * @define sessionId The sessionId assigned to the session by Conviva
 * @define timestamp to seconds, milliseconds, timestamp or asis methods
 * @define signed as a signed, unsigned, or hexadecimal string
 * @example {{{
 * df.select(customerId.asis, clientId.hex, hasEnded.asis, justJoined.asis)
 * }}}
 */

object PbSS {

    /** Extract the `customerId` column as is.
     * @example{{{
     * df.select(customerId.asis)
     * }}}
    */ 
    def customerId(): Column = col("key.sessId.customerId")

    /** Extract the `clientSessionId` column as is.
     * @example{{{
     * df.select(sessionId.asis)
     * }}}
    */ 
    def sessionId() = ExtractIDCol(field = "key.sessId.clientSessionId",
      name = "sessionId") 

    /** Create the `clientId` column $signed. 
     * @example{{{
     * df.select(
     *  clientId.asis,
     *  clientId.signed, 
     *  clientId.unsigned, 
     *  clientId.hex)
     * }}}  
    */ 
    def clientId = ExtractIDArray(field = "key.sessId.clientId.element", 
      name = "clientId")

    /** Create sessionCreationTime as an object with hex, signed, and unsigned
     *  methods. Note that this is not the same as `sessionCreationTime` which
     *  has ms, sec, and timestamp methods. 
     *  @example{{{
     *  df.select(
     *    sessionCreationId.asis,
     *    sessionCreationId.hex,
     *    sessionCreationId.signed, 
     *    sessionCreationId.unsigned
     *  )
     *  }}}
     */
    def sessionCreationId = ExtractIDCol(field = "val.invariant.sessionCreationTimeMs", 
      name = "sessionCreationId")

    /** Create an sid5 object which concatenates `clientId` and `clientSessionId` with $signed. 
     * @example{{{
     * df.select(
     *  sid5.signed, 
     *  sid5.unsigned, 
     *  sid5.hex)
     * }}}  
    */ 
    def sid5 = ExtractSID(name = "sid5", clientId, sessionId)

    /** Create an sid6 object which concatenates `clientId`, `clientSessionId`, 
     *  `sessionCreationTime` with $signed. 
     * @example{{{
     * df.select(
     *  sid6.signed, 
     *  sid6.unsigned, 
     *  sid6.hex)
     * }}}  
    */ 
    def sid6 = ExtractSID(name = "sid6", clientId, sessionId, sessionCreationId)

    /** Extract the `shouldProcess` field as is.
     * @example{{{
     * df.select(shouldProcess)
     * }}}
    */ 
    def shouldProcess(): Column = col("val.sessSummary.shouldProcess")

    /** Extract the `hasEnded` field as is. 
     * @example{{{
     * df.select(hadEnded)
     * }}}
    */ 
    def hasEnded(): Column = col("val.sessSummary.hasEnded")

    /** Extract the `justEnded` field as is.
     * @example{{{
     * df.select(justEnded)
     * }}}
    */ 
    def justEnded(): Column = col("val.sessSummary.justEnded")

    /** Extract the `endedStatus` field as is.
     * @example{{{
     * df.select(endedStatus)
     * }}}
    */ 
    def endedStatus(): Column = col("val.sessSummary.endedStatus")

    /** Extract the `joinState` field as is.
     * @example{{{
     * df.select(joinState)
     * }}}
    */ 
    def joinState(): Column = col("val.sessSummary.joinState")

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

    /** Create `joinTime` object with methods.
     *  @example{{{
     *  df.select(
     *    joinTime.asis, 
     *    joinTime.ms, 
     *    joinTime.sec,
     *    joinTime.stamp
     *  )
     *  }}}
     */
    def joinTime() = ExtractColMs(field = "val.sessSummary.joinTimeMs", 
      name = "joinTime")

    /** Create `lifePlayingTime` object with methods. 
     * @example{{{
     * df.select(
     *   lifePlayingTime.asis,
     *   lifePlayingTime.ms, 
     *   lifePlayingTime.sec, 
     *   lifePlayingTime.stamp
     * )
     * }}}
    */
    def lifePlayingTime() = ExtractColMs(field = "val.sessSummary.lifePlayingTimeMs",
      name = "lifePlayingTime")

    /** Create a field that flags if `joinTimeMs` is greater than zero,
     *  otherwise gets values less than or equal zero. 
     *  @example{{{
     *  df.select(isJoinTime)
     *  }}}
     */
    def isJoinTime(): Column = {
      when(joinTime.asis > 0, 1).otherwise(joinTime.asis).alias("isJoinTime")
    }

    /** Create a field that flags if `lifePlayingTimeMs` is greater than zero,
     *  otherwise gets values less than or equal zero. 
     *  @example{{{
     *  df.select(isLifePlayingTime)
     *  }}}
     */
    def isLifePlayingTime(): Column = {
      when(lifePlayingTime.asis > 0, 1).otherwise(lifePlayingTime.asis)
        .alias("isLifePlayingTime")
    }

    /** Create a field that flags if the session joined. 
     *  @example{{{
     *  df.select(isLifePlayingTime)
     *  }}}
     */
    def isJoined(): Column = {
      when(joinTime.asis === -1, 0).otherwise(1).alias("isJoined")
    }

    /** Create a field to flag consistency between joinTimeMs, joinState and lifePlayingTimeMs.
     * Consistent combinations are:
     * |isJoinTimeMs|joinState|isLifePlayingTimeMs| Comment |
     * |---         |---      |---                |---      |
     * |-1          |-1       |0                  | didn't join, zero life playing time |
     * |1           |1        |1                  | joined, known join time, positive life playing time |
     * |-3          |0        |1                  | joined, unknown join time, positive life playing time |
     * Any other combination is inconsistent.
    */
    def isConsistent(
      joinTime: String = "val.sessSummary.joinTimeMs",
      joinState: String = "val.sessSummary.joinState", 
      lifePlayingTime: String = "val.sessSummary.lifePlayingTimeMs"): 
    Column = {
      val isJoinTime = when(col(joinTime) > 0, 1).otherwise(col(joinTime))
      val isLifePlayingTime = when(col(lifePlayingTime) > 0, 1)
        .otherwise(col(lifePlayingTime))
      when(isJoinTime === -1 && col(joinState)  === -1 && isLifePlayingTime  === 0, 1) 
        .when(isJoinTime === 1 && col(joinState)  === 1 && isLifePlayingTime  === 1, 1)
        .when(isJoinTime === -3 && col(joinState) === 0 &&  isLifePlayingTime  === 1, 1) 
      .otherwise(0).alias("isConsistent")
    }

    /** Create a column flagging if session is an AdSession or ContentSession.
    */
     def c3IsAd(): Column =  {
       when(col("val.invariant.summarizedTags")
         .getItem("c3.video.isAd") === "T", "adSession").otherwise("contentSession")
         .alias("c3IsAd")
     }

    /** Get field for the m3 Device Name. */
     def m3DeviceName(): Column =  {
       col("val.invariant.summarizedTags")
         .getItem("m3.dv.n") 
         .alias("m3DeviceName")
     }

    /** Get field for ad technology, client or server side. */
     def c3AdTechnology(): Column =  {
       col("val.invariant.summarizedTags")
         .getItem("c3.ad.technology") 
         .alias("c3AdTechnology")
     }

    /** Get field for AdContentMetadata. */
     def adContentMetadata = AdContentMetadata(
       field = "val.sessSummary.AdContentMetadata",
       name = "adContentMetadata")

    /** Get field for c3 Viewer Id. */
    def c3ViewerId(): Column = {
      col("val.invariant.summarizedTags")
        .getItem("c3.viewer.id")
        .alias("c3ViewerId")
    }

    /** Get field for ad client session Id (c3.csid). */ 
    def c3CsId(): Column = {
      col("val.invariant.summarizedTags").getItem("c3.csid")
        .alias("c3CsId")
    }

    /** Creates the intvStartTime column $timestamp.
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
      * df.select(
      *   lifeFirstRecvTime.asis,
      *   lifeFirstRecvTime.sec,
      *   lifeFirstRecvTime.ms, 
      *   lifeFirstRecvTime.stamp)
      * }}}
      */
      def lifeFirstRecvTime = ExtractColMs( 
        field = "val.sessSummary.lifeFirstRecvTimeMs", name = "lifeFirstRecvTime")

    /**
      * Parse the firstRecvTime column $timestamp
      * @example {{{
      * df.select(
      *   firstRecvTime.asis,
      *   firstRecvTime.ms,
      *   firstRecvTime.sec,
      *   firstRecvTime.stamp)
      * }}}
      */
    def firstRecvTime = ExtractColMs(
      field = "key.firstRecvTimeMs", name = "firstRecvTime")

    /**
      * Parse the lastRecvTime column $timestamp.
      * @example {{{
      * df.select(
      *   lastRecvTime.asis,
      *   lastRecvTime.ms,
      *   lastRecvTime.sec,
      *   lastRecvTime.stamp)
      * }}}
      */
    def lastRecvTime = ExtractColMs(
      field = "val.sessSummary.lastRecvTimeMs", name = "lastRecvTime") 

    /**
      * Creates the sessionCreationTime object with $timestamp.
      * @example {{{
      * df.select(
      *   sessionCreationTime.asis,
      *   sessionCreationTime.ms,
      *   sessionCreationTime.sec,
      *   sessionCreationTime.stamp)
      * }}}
      */
    def sessionCreationTime = ExtractColMs( 
      field = "val.invariant.sessionCreationTimeMs", name = "sessionCreationTime")

    /**
      * Creates the sessionTime object with $timestamp.
      * @example {{{
      * df.select(
      *   sessionTime.asis,
      *   sessionTime.ms,
      *   sessionTime.sec,
      *   sessionTime.stamp)
      * }}}
      */
    def sessionTime = ExtractColMs(field = "val.sessSummary.sessionTimeMs",
      name = "sessionTime")

    /**
      * Creates the lifeBufferingTime object with $timestamp.
      * @example {{{
      * df.select(
      *   lifeBufferingTime.asis,
      *   lifeBufferingTime.ms,
      *   lifeBufferingTime.sec,
      *   lifeBufferingTime.stamp)
      * }}}
      */
    def lifeBufferingTime() = ExtractColMs(field = "val.sessSummary.lifeBufferingTimeMs", 
      name = "lifeBufferingTime")

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


