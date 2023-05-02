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

object PbRl {

  // def genericEvent(name: String): ArrayCol = {
  //   val event = new ArrayCol("payload.heartbeat.pbSdmEvents.genericEvent")
  //   event.getItem(name),
  // }

  /** Method for extracting fields from `val.invariant.summarizedTags`. Fields
   *  with periods are replaced with underscores by default.*/
  def c3Tag(field: String): Column = {
    col("payload.heartbeat.c3Tags").getItem(field)
      .alias(field.replaceAll("\\.", "_"))
  }

  /** Method for extracting fields from `val.invariant.summarizedTags`. Fields
   *  with periods are replaced with underscores by default.*/
  def clientTag(field: String): Column = {
    col("payload.heartbeat.clientTags").getItem(field)
      .alias(field.replaceAll("\\.", "_"))
  }

  /** Method to extract fields from the `cwsPlayerMeasurementEvent` container.*/
  def cwsPlayer(name: String): ArrayCol = {
      new ArrayCol(s"payload.heartbeat.pbSdmEvents.cwsPlayerMeasurementEvent.${name}")
  }

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

  /** Creates an Ad SID5 object which concatenates `clientId` and `c3_csid`
   *  $signed. 
   *  @example{{{
   *  df.select(
   *    sid5Ad.hex, 
   *    sid5Ad.signed, 
   *    sid5Ad.unsigned
   *  )
   *  }}}
   */
  def sid5Ad = SID(name = "sid5Ad", clientId, c3_csid)

  /** Creates a client session Id (c3.csid) object asis or $signed. 
   * @example{{{
   * df.select(
   *   c3_csid.asis,
   *   c3_csid.signed, 
   *   c3_csid.hex, 
   *   c3_csid.unsigned
   * )
   * }}}
   */ 
  def c3_csid = IdCol(field = clientTag("c3.csid"), name = "c3_csid")

  /** Extract the `seqNumber` field as is.
   * @example{{{
   * df.select(seqNumber)
   * }}}
  */ 
  def seqNumber(): Column = col("payload.heartbeat.seqNumber")

  /** Extract dropped frames total. */
  def dftot(): Column = 
    col("payload.heartbeat.pbSdmEvents.cwsPlayerMeasurementEvent.genericDictLong")
      .apply(0).getItem("dftot").alias("dftot")

  /** Extract dropped frames count. */
  def dfcnt(): Column = {
    col("payload.heartbeat.pbSdmEvents.cwsPlayerMeasurementEvent.genericDictLong")
      .apply(0).getItem("dfcnt").alias("dfcnt")
  }

  /** Extract the session time. */
  def sessionTimeMs() = new ArrayCol("payload.heartbeat.pbSdmEvents.sessionTimeMs")

}
