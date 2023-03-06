package conviva.surgeon

import conviva.surgeon.Sanitize._
import org.apache.spark.sql.functions.{lower, col, when}
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
    def sessionId() = IdCol(field = col("key.sessId.clientSessionId"),
      name = "sessionId") 

    /** Create the `clientId` column as is or $signed. 
     * @example{{{
     * df.select(
     *  clientId.asis,
     *  clientId.signed, 
     *  clientId.unsigned, 
     *  clientId.hex)
     * }}}  
    */ 
    def clientId = IdArray(field = col("key.sessId.clientId.element"), 
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
    def sessionCreationId = IdCol(field = col("val.invariant.sessionCreationTimeMs"), 
      name = "sessionCreationId")

    /** Create an sid5 object which concatenates `clientId` and `clientSessionId` $signed. 
     * @example{{{
     * df.select(
     *  sid5.signed, 
     *  sid5.unsigned, 
     *  sid5.hex)
     * }}}  
    */ 
    def sid5 = SID(name = "sid5", clientId, sessionId)

    /** Create an sid6 object which concatenates `clientId`, `clientSessionId`, 
     *  `sessionCreationTime` $signed. 
     * @example{{{
     * df.select(
     *  sid6.signed, 
     *  sid6.unsigned, 
     *  sid6.hex)
     * }}}  
    */ 
    def sid6 = SID(name = "sid6", clientId, sessionId, sessionCreationId)

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
    def joinTime() = TimeMsCol(field = col("val.sessSummary.joinTimeMs"), 
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
    def lifePlayingTime() = TimeMsCol(field = col("val.sessSummary.lifePlayingTimeMs"),
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
      joinTime: Column = col("val.sessSummary.joinTimeMs"),
      joinState: Column = col("val.sessSummary.joinState"), 
      lifePlayingTime: Column = col("val.sessSummary.lifePlayingTimeMs")): 
    Column = {
      val isJoinTime = when(joinTime > 0, 1).otherwise(joinTime)
      val isLifePlayingTime = when(lifePlayingTime > 0, 1)
        .otherwise(lifePlayingTime)
      when(isJoinTime === -1 && joinState  === -1 && isLifePlayingTime  === 0, 1) 
        .when(isJoinTime === 1 && joinState   === 1 && isLifePlayingTime  === 1, 1)
        .when(isJoinTime === -3 && joinState  === 0 &&  isLifePlayingTime  === 1, 1) 
      .otherwise(0).alias("isConsistent")
    }

    /** Create a column flagging if session is an AdSession or ContentSession. */
    def c3IsAd(): Column = {
      when(col("val.invariant.summarizedTags")
        .getItem("c3.video.isAd") === "T", "adSession").otherwise("contentSession")
        .alias("c3IsAd")
     }

    /** Get field for the m3 Device Name. */
    def m3DeviceName() = invTag("m3.dv.n", "m3DeviceName")

     /** Get field for m3 Device OS. */
    def m3DvOs() = invTag("m3.dv.os", "m3DvOs")

    /** Get the field for the m3 Device name with f. */
    def m3DvOsf() = invTag("m3.dv.osf", "m3DvOsf")

    /** Get the field for the m3 Device name with version. */
    def m3DvOsv() = invTag("m3.dv.osv", "m3DvOsv")

    /** Get the field for is Live or Video on Demands. */
    def c3VideoIsLive() = invTag("c3.video.isLive", "c3VideoIsLive")

    def liveOrVod(): Column = {
      col("val.sessSummary.d3SessSummary.liveOrVod")
        .alias("liveOrVod")
    }

    /** Extract `Ad Technology` field with methods. */
    case class AdTech(field: Column, name: String) extends AsCol {
      def recode(): Column = {
        when(lower(field).rlike("server|ssai|sever"), "server")
          .when(lower(field).rlike("client|csai"), "client")
          .otherwise("unknown")
        .alias(s"${name}RC")
      }
    }
    /** Create a c3 `Ad Technology` object with an `asis` and `recode` method. The
     *  `recode` method standardizes the field values into server, client, or
     *  unknown.
     *  @example{{{
     *  df.select(
     *    c3AdTechnology.asis, 
     *    c3AdTechnology.recode
     *  )
     *  }}}
     */
    def c3AdTechnology = AdTech(
      field = col("val.invariant.summarizedTags").getItem("c3.ad.technology"),
      name = "c3AdTechnology")

    /** Extract fields from `AdContentMetadata` with methods. */
    case class AdContentMetadata(field: Column, name: String) extends AsCol {
      def adRequested(): Column = field.getItem("adRequested").alias("adRequested")
      def preRollStatus(): Column = field.getItem("preRollStatus").alias("preRollStatus")
      def hasSSAI(): Column = field.getItem("hasSSAI").alias("hasSSAI")
      def hasCSAI(): Column = field.getItem("hasCSAI").alias("hasCSAI")
      def preRollStartTime = TimeMsCol(field = field.getItem("preRollStartTimeMs"),
        name = "preRollStartTime")
    }
    /** Get field for AdContentMetadata. */
     def adContentMetadata = AdContentMetadata(
       field = col("val.sessSummary.AdContentMetadata"),
       name = "adContentMetadata")

    /** Get field for c3 Viewer Id. */
    def c3ViewerId(): Column = {
      col("val.invariant.summarizedTags")
        .getItem("c3.viewer.id")
        .alias("c3ViewerId")
    }

    /** Creates a client session Id (c3.csid) object asis or $signed. 
     * @example{{{
     * df.select(
     *   c3CsId.asis,
     *   c3CsId.signed, 
     *   c3CsId.hex, 
     *   c3CsId.unsigned
     * )
     * }}}
     */ 
    def c3CsId = IdCol(
      field = col("val.invariant.summarizedTags").getItem("c3.csid"),
      name = "c3CsId")

    /** Creates an Ad SID5 object which concatenates `clientId` and `c3CsId`
     *  $signed. 
     *  @example{{{
     *  df.select(
     *    sid5Ad.hex, 
     *    sid5Ad.signed, 
     *    sid5Ad.unsigned
     *  )
     *  }}}
     */

    def sid5Ad = SID(name = "sid5Ad", clientId, c3CsId)

    /** Creates the intvStartTime column $timestamp.
      * @example {{{
      * df.select(
      *   intvStartTime.asis, 
      *   intvStartTime.ms,
      *   intvStartTime.sec,
      *   intvStartTime.stamp)
      * }}}
      */
    def intvStartTime = TimeSecCol(
      field = col("val.sessSummary.intvStartTimeSec"), name = "intvStartTime")

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
      def lifeFirstRecvTime = TimeMsCol( 
        field = col("val.sessSummary.lifeFirstRecvTimeMs"), name = "lifeFirstRecvTime")

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
    def firstRecvTime = TimeMsCol(
      field = col("key.firstRecvTimeMs"), name = "firstRecvTime")

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
    def lastRecvTime = TimeMsCol(
      field = col("val.sessSummary.lastRecvTimeMs"), name = "lastRecvTime") 

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
    def sessionCreationTime = TimeMsCol( 
      field = col("val.invariant.sessionCreationTimeMs"), name = "sessionCreationTime")

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
    def sessionTime = TimeMsCol(field = col("val.sessSummary.sessionTimeMs"),
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
    def lifeBufferingTime() = TimeMsCol(field = col("val.sessSummary.lifeBufferingTimeMs"), 
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

