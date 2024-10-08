package org.surgeon

/**
 * Perform operations on the PbSS hourly, daily and monthly data. The main
 * operation is to select columns from the data. Objects are named after
 * fields (e.g., customerId) and have custom methods (e.g. intvStartTime.ms,
 * which converts intvStartTimeSec to milliseconds). The Objects return type
 * Column, so you can invoke Column.methods on the result (e.g.,
 * dat.select(shouldProcess.asis.alias("myNewName")).
 * @define clientId The clientID assigned to the client.
 * @define sessionId The sessionId assigned to the session.
 * @define timestamp to seconds, milliseconds, timestamp or asis methods
 * @define signed as a signed, unsigned, or hexadecimal string
 * @example {{{
 * df.select(customerId.asis, clientId.hex, hasEnded.asis, justJoined.asis)
 * }}}
 */

object PbRl {

  import org.surgeon.Sanitize._
  import org.apache.spark.sql.functions.{lower, col, when, typedLit, array_join, array_remove, split}
  import org.apache.spark.sql.{Column}
  import org.surgeon.GeoInfo._
  import org.surgeon.Paths._
  import org.surgeon.Customer._

  /** Instantiate a class for C3 methods.**/
  val c3 = C3(ProdPbRl())

  def pbrl(date: String) = SurgeonPath(ProdPbRl()).make(date)
  
  val pbsdm = "payload.heartbeat.pbSdmEvents"

  def genericEvent(name: String): ArrayCol = {
    new ArrayCol(col(s"$pbsdm.genericEvent").getItem(name), s"$name")
  }

  /** Method for extracting fields from `payload.heartbeat.pbSdmEvents`. Fields
   *  with periods are replaced with underscores by default.*/
  def pbSdm(field: String = ""): Column = { 
    if (field.isEmpty) col(s"$pbsdm")
    else col(s"$pbsdm").getItem(field).alias(field.replaceAll("\\.", "_"))
  }

  def payload(name: String): Column = col(s"payload.heartbeat.$name")

  /** Method for extracting fields from `payload.heartbeat.c3Tag`. Fields
   *  with periods are replaced with underscores by default.*/
  def c3Tags(field: String): Column = {
    payload("c3Tags").getItem(field).alias(field.replaceAll("\\.", "_"))
  }

  /** Method for extracting fields from `payload.heartbeat.geoInfo`. */
  def geoInfo(field: String) = GeoBuilder(ProdPbRl().geoUtilPath).make(field)

  /** Method for extracting fields from `payload.heartbeat.clientTags`. Fields
   *  with periods are replaced with underscores by default.*/
  def clientTags(field: String): Column = {
    payload("clientTags").getItem(field).alias(field.replaceAll("\\.", "_"))
  }

  /** Method to extract fields from the `cwsPlayerMeasurementEvent` container.*/
  def cwsPlayerEvent(name: String): ArrayCol =  {
    new ArrayCol(col(s"$pbsdm.cwsPlayerMeasurementEvent.${name}"), s"$name")
  }
  /** Method to extract fields from the `cwsStateChangeEvent` container.*/
  def cwsStateChangeNew(name: String): ArrayCol = {
      new ArrayCol(col(s"$pbsdm.cwsStateChangeEvent.newCwsState.${name}"), s"$name")
  }

  /** Method to extract the type of pbSdm event.*/
  def pbSdmType(): Column = col(s"$pbsdm.type")

  class CWSStateChangeEvent(name: String) {
    def newState(): Column = {
      col(s"${pbsdm}.cwsStateChangeEvent.newCwsState.${name}")
    }
    /** Remove nulls, keep the same name. */
    def oldState(): Column = {
      col(s"${pbsdm}.cwsStateChangeEvent.oldCwsState.${name}").alias(s"${name}Old")
    }
  }

  def cwsStateChangeEvent(name: String) = new CWSStateChangeEvent(name)

  /** Method to extract seek timeline from CWS. */ 
  def cwsSeekEvent(): Column = {
    col("payload.heartbeat.pbSdmEvents.cwsSeekEvent")
  }

  /** Extract the `customerId` column as is.
   * @example{{{
   * df.select(customerId)
   * }}}
  */ 
  def customerId(): Column = payload("customerId")

  /** Extract the `customerId` column as is.
   * @example{{{
   * df.select(customerName)
   * }}}
  */ 
  def customerName(): Column = CustomerName(ProdPbRl().geoUtilPath).make(customerId)

  /** Extract the `clientSessionId` column as is.
   * @example{{{
   * df.select(sessionId.asis)
   * }}}
  */ 
  def sessionId() = new IdCol(payload("clientSessionId"), name = "sessionId") 

  /** Create the `clientId` column as is or $signed. 
   * @example{{{
   * df.select(
   *  clientId.asis,
   *  clientId.nosign, 
   *  clientId.hex)
   * }}}  
  */ 
  def clientId = new IdArray(payload("clientId.element").alias("clientId"), "clientId")

  /** Create timeStamp $timestamp.
   *  @example{{{
   *  df.select(
    *   timeStamp.ms,
    *   timeStamp.sec,
    *   timeStamp.stamp)
   *  )
   *  }}}
   */
  def timeStamp() = new TimeUsCol(col("header.timeStampUs"), "timeStamp")

  /** Create an sid5 object which concatenates `clientId` and `clientSessionId` $signed. 
   * @example{{{
   * df.select(
   *  sid5.nosign, 
   *  sid5.hex)
   * }}}  
  */ 
  def sid5 = SID(name = "sid5", clientId, sessionId)

  /** Creates an Ad SID5 object which concatenates `clientId` and `c3csid`
   *  $signed. 
   *  @example{{{
   *  df.select(
   *    sid5Ad.asis, 
   *    sid5Ad.hex, 
   *    sid5Ad.nosign
   *  )
   *  }}}
   */
  def sid5Ad = SID(name = "sid5Ad", clientId, c3csid)

  /** Creates a client session Id (c3.csid) object asis or $signed. 
   * @example{{{
   * df.select(
   *   c3csid.asis,
   *   c3csid.hex, 
   *   c3csid.nosign
   * )
   * }}}
   */ 
  def sessionAdId = new IdCol(clientTags("c3.csid"), name = "sessionAdId")
  def c3csid = new IdCol(clientTags("c3.csid"), name = "c3_csid")

  /** Extract the `seqNumber` field as is.
   * @example{{{
   * df.select(seqNumber)
   * }}}
  */ 
  def seqNumber(): Column = payload("seqNumber")

  /** Extract dropped frames total. */
  def dftot(): Column = 
    cwsPlayerEvent("genericDictLong")
      .apply(0).getItem("dftot").alias("dftot")

  /** Extract dropped frames count. */
  def dfcnt(): Column = {
    cwsPlayerEvent("genericDictLong")
      .apply(0).getItem("dfcnt").alias("dfcnt")
  }

  /** Extract the session time. */
  def sessionTimeMs() = new ArrayCol(pbSdm("sessionTimeMs"), "sessionTimeMs")

  /**
    * Creates the sessionCreationTime object with $timestamp.
    * @example {{{
    * df.select(
    *   sessionCreationTime,
    *   sessionCreationTime.sec,
    *   sessionCreationTime.stamp)
    * }}}
    */
  def sessionCreationTime = new TimeMsCol(payload("sessionCreationTimeMs"), "sessionCreationTime")

  /** Create a c3 `isAd` object with an `asis` and `recode` method. The
   *  `recode` method standardizes the field values into true, false, or
   *  null.
   *  @example{{{
   *  df.select(
   *    c3isAd, 
   *    c3isAd.recode
   *  )
   *  }}}
   */
  def c3isAd = new c3isAd(c3Tags("c3.video.isAd").alias("c3_isAd"))


  def ipv6() = new IP6(col("payload.heartbeat.publicipv6.element"), "ipv6")
  def ipv4() = new IP4(col("payload.heartbeat.publicip.element"), "ipv4")
  def publicIp24Msb() = new IP4(col("payload.heartbeat.publicIp24Msb"), "ip24Msb")
}

