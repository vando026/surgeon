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

object PbRL {

    /** Extract the `customerId` column as is.
     * @example{{{
     * df.select(customerId.asis)
     * }}}
    */ 
    def customerId(): Column = col("payload.heartbeat.customerId")

    /** Extract the `clientSessionId` column as is.
     * @example{{{
     * df.select(sessionId.asis)
     * }}}
    */ 
    def sessionId() = IdCol(field = col("payload.heartbeat.clientSessionId"),
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
    def clientId = IdArray(field = col("payload.heartbeat.clientId.element"), 
      name = "clientId")

    /** Create timeStamp $timestamp.
     *  @example{{{
     *  df.select(
      *   timeStamp.ms,
      *   timeStamp.sec,
      *   timeStamp.stamp)
     *  )
     *  }}}
     */
    def timeStamp = TimeUsCol(field = col("header.timeStampUs"),
      name = "timeStamp")

    /** Create an sid5 object which concatenates `clientId` and `clientSessionId` $signed. 
     * @example{{{
     * df.select(
     *  sid5.signed, 
     *  sid5.unsigned, 
     *  sid5.hex)
     * }}}  
    */ 
    def sid5 = SID(name = "sid5", clientId, sessionId)

    /** Extract `playerState` field as is. */ 
    def playerState() = ArrayCol(
      field = col("payload.heartbeat.pbSdmEvents.cwsPlayerMeasurementEvent.playerState"),
      name = "playerState")

    /** Extract the `seqNumber` field as is.
     * @example{{{
     * df.select(seqNumber)
     * }}}
    */ 
    def seqNumber(): Column = col("payload.heartbeat.seqNumber")

    /** Extract `encodedFps` field. */
    def encodedFps() = ArrayCol(
      field = col("payload.heartbeat.pbSdmEvents.cwsPlayerMeasurementEvent.encodedFps"),
      name = "encodedFps")

    /** Extract `averageFps` field as is, */
    def averageFps() = ArrayCol(
      field = col("payload.heartbeat.pbSdmEvents.cwsPlayerMeasurementEvent.averageFps"),
      name = "averageFps") 

    /** Extract `renderedFpsTotal` field with array methods. */
    def renderedFpsTotal()  = ArrayCol(
      field = col("payload.heartbeat.pbSdmEvents.cwsPlayerMeasurementEvent.renderedFpsTotal"),
      name = "renderedFpsTotal")

    /** Extract `renderedFpsCount` field with array methods. */
    def renderedFpsCount() = ArrayCol(
      field = col("payload.heartbeat.pbSdmEvents.cwsPlayerMeasurementEvent.renderedFpsCount"),
      name = "renderedFpsCount")

    /** Extract dropped frames total. */
    def droppedFramesTotal(): Column = 
      col("payload.heartbeat.pbSdmEvents.cwsPlayerMeasurementEvent.genericDictLong")(0)
        .getItem("dftot").alias("droppedFramesTotal")

    /** Extract dropped frames total. */
    def droppedFramesCount(): Column = {
      col("payload.heartbeat.pbSdmEvents.cwsPlayerMeasurementEvent.genericDictLong")(0)
        .getItem("dfcnt").alias("droppedFramesCount")
    }

    def sessionTime() = ArrayCol(
      field = col("payload.heartbeat.pbSdmEvents.sessionTimeMs"),
      name = "sessionTimeMs")

    // def hbDuration(): Column = {
    //   val windowSpec = Window
    //     .partitionBy("sid5Hex")
    //     .orderBy("timeStampSec")
    //   withColumn("hbDuration", 
    //     lag(col(""))
    //     )
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

