package conviva.surgeon

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

  import conviva.surgeon.Sanitize._
  import org.apache.spark.sql.functions.{lower, col, when, typedLit}
  import org.apache.spark.sql.{Column}
  import conviva.surgeon.GeoInfo._
  
  val pbsdm = "payload.heartbeat.pbSdmEvents"

  def genericEvent(name: String): ArrayCol = {
    new ArrayCol(col(s"$pbsdm.genericEvent").getItem(name), s"$name")
  }

  /** Method for extracting fields from `payload.heartbeat.pbSdmEvents`. Fields
   *  with periods are replaced with underscores by default.*/
  def pbSdm(field: String): Column = { 
    col(s"$pbsdm").getItem(field).alias(field.replaceAll("\\.", "_"))
  }

  def payload(name: String): Column = col(s"payload.heartbeat.$name")

  /** Method for extracting fields from `payload.heartbeat.c3Tag`. Fields
   *  with periods are replaced with underscores by default.*/
  def c3Tags(field: String): Column = {
    payload("c3Tags").getItem(field).alias(field.replaceAll("\\.", "_"))
  }

  /** Method for extracting fields from `payload.heartbeat.geoInfo`. */
  def geoInfo(field: String, geomap: Option[Map[Int, String]] = None): GeoCol = {
    val gcol = col(s"payload.heartbeat.geoInfo.$field")
    val gMap = geomap.getOrElse(getGeoData(field))
    new GeoCol(gcol, field, gMap)
  }

  /** Method for extracting fields from `payload.heartbeat.clientTags`. Fields
   *  with periods are replaced with underscores by default.*/
  def clientTags(field: String): Column = {
    payload("clientTags").getItem(field).alias(field.replaceAll("\\.", "_"))
  }

  /** Method to extract fields from the `cwsPlayerMeasurementEvent` container.*/
  def cwsPlayer(name: String): ArrayCol =  {
    new ArrayCol(col(s"$pbsdm.cwsPlayerMeasurementEvent.$name"), s"$name")
  }
  /** Method to extract fields from the `cwsStateChangeEvent` container.*/
  def cwsStateChangeNew(name: String): ArrayCol = {
      new ArrayCol(col(s"$pbsdm.cwsStateChangeEvent.newCwsState.$name"), s"$name")
  }

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
  def customerName(): Column = {
    val gMap = getGeoData("resource")
    val gLit: Column = typedLit(gMap) 
    gLit(customerId).alias(s"customerName")
  }

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

  /** Creates an Ad SID5 object which concatenates `clientId` and `c3_csid`
   *  $signed. 
   *  @example{{{
   *  df.select(
   *    sid5Ad.asis, 
   *    sid5Ad.hex, 
   *    sid5Ad.nosign
   *  )
   *  }}}
   */
  def sid5Ad = SID(name = "sid5Ad", clientId, c3_csid)

  /** Creates a client session Id (c3.csid) object asis or $signed. 
   * @example{{{
   * df.select(
   *   c3_csid.asis,
   *   c3_csid.hex, 
   *   c3_csid.nosign
   * )
   * }}}
   */ 
  def sessionAdId = new IdCol(clientTags("c3.csid"), name = "sessionAdId")
  def c3_csid = new IdCol(clientTags("c3.csid"), name = "c3_csid")

  /** Extract the `seqNumber` field as is.
   * @example{{{
   * df.select(seqNumber)
   * }}}
  */ 
  def seqNumber(): Column = payload("seqNumber")

  /** Extract dropped frames total. */
  def dftot(): Column = 
    cwsPlayer("genericDictLong")
      .apply(0).getItem("dftot").alias("dftot")

  /** Extract dropped frames count. */
  def dfcnt(): Column = {
    cwsPlayer("genericDictLong")
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

}
/*
  def EventTimeStamp(data: DataFrame): DataFrame {

    val dat1 = dat
      .select(sessionId, sessionCreationTime, timeStamp.ms)
      .groupBy("sessionId").agg(
        min(col("sessionCreationTimeMs")).alias("createTime"),
        min(col("timeStampMs")).alias("timeStamp")
      )
      .withColumn("offset", (col("createTime") - col("timeStamp")))
      .select("sessionId", "offset") 

    val dat2 = dat.join(dat1, List("sessionId"), "left")
      .withColumn("gatewayTimeMs", col("timeStampMs") + col("offset"))
      .withColumn("gatewayTimeStamp", from_unixtime(col("gatewayTimeMs") / 1000))

    def eventTimeStamp = new TimeMsCol("gatewayTimeStamp" , "eventTimeStamp")
    dat2.withColumn("test", eventTimeStamp)
  }
display(playerData)
*/

