package conviva.surgeon

import conviva.surgeon.Sanitize._
  
/**
 * Perform operations on the PbSS hourly, daily and monthly data. The main
 * operation is to select columns from the data. Objects are named after
 * fields (e.g., customerId) and have custom methods (e.g. intvStartTime.ms,
 * which converts intvStartTimeSec to milliseconds). The Objects return type
 * Column, so you can invoke Column.methods on the result (e.g.,
 * dat.select(shouldProcess.asis.alias("myNewName")).
 * @define clientId The clientID assigned to the client by Conviva
 * @define sessionId The sessionId assigned to the session by Conviva
 * @define timestamp in seconds, milliseconds, or timestamp
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
    def customerId() = ExtractColAs("key.sessId.customerId")

    /** Extract the `clientSessionId` column as is.
     * @example{{{
     * df.select(sessionId.asis)
     * }}}
    */ 
    def sessionId() = ExtractColAs("key.sessId.clientSessionId")

    /** Create the `clientId` column $signed. 
     * @example{{{
     * df.select(clientId.signed, clientId.unsigned, clientId.hex)
     * }}}  
    */ 
    def clientId = ExtractID("key.sessId.clientId.element", "clientId")

    /** Create the sid5 column which concatenates `clientId` and `clientSessionId`
     *  $signed. 
     * @example{{{
     * df.select(sid5.signed, sid5.unsigned, sid5.hex)
     * }}}  
    */ 
    def sid5 = ExtractID2("key.sessId.clientId.element", 
      "key.sessId.clientSessionId", "sid5")

    /** Get the shouldProcess column as is.
     * @example{{{
     * df.select(shouldProcess.asis)
     * }}}
    */ 
    def shouldProcess() = ExtractColAs("val.sessSummary.shouldProces")

    /** Get the `hasEnded` column as is. 
     * @example{{{
     * df.select(hadEnded.asis)
     * }}}
    */ 
    def hasEnded() = ExtractColAs("val.sessSummary.hasEnded")

    /** Get the justEnded column.
     * @example{{{
     * df.select(justEnded.asis)
     * }}}
    */ 
    def justEnded() = ExtractColAs("val.sessSummary.justEnded")

    /** Get the endedStatus column.
     * @example{{{
     * df.select(endedStatus.asis)
     * }}}
    */ 
    def endedStatus() = ExtractColAs("val.sessSummary.endedStatus")

    /** Get the joinState column.
     * @example{{{
     * df.select(joinState.asis)
     * }}}
    */ 
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
      * Creates the sessionCreationTime column $timestamp.
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


