package conviva.surgeon

import conviva.surgeon.Sanitize._
import org.apache.spark.sql.functions.{lower, col, when}
import org.apache.spark.sql.{Column}
import conviva.surgeon.Metrics._
  
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
 * @define ss `val.sessSummary`
 * @example {{{
 * df.select(customerId.asis, clientId.hex, hasEnded, justJoined)
 * }}}
 */

object PbSS {

  /** Method to extract fields from the `lifeSwitchInfos` container. */
  def lifeSwitch(name: String): ArrayCol = {
    new ArrayCol(s"val.sessSummary.lifeSwitchInfos.$name")
  }

  /** Method to extract fields from the `intvSwitchInfos` container. */
  def intvSwitch(name: String): ArrayCol = {
    new ArrayCol(s"val.sessSummary.intvSwitchInfos.$name")
  }

  /** Method to extract fields from the `joinSwitchInfos` container. */
  def joinSwitch(name: String): ArrayCol = {
    new ArrayCol(s"val.sessSummary.joinSwitchInfos.$name")
  }

  /** Method for extracting fields from `val.invariant`. */
  def invTag(field: String): Column = {
    col(s"val.invariant.${field}").alias(field)
  }

  /** Method for extracting fields from `val.invariant.summarizedTags`. */
  def sumTag(field: String): Column = {
    col("val.invariant.summarizedTags").getItem(field).alias(field)
  }

  /** Method for extracting fields from `val.sessSummary.d3SessSummary`. */
  def d3SS(field: String): Column = {
    col(s"val.sessSummary.d3SessSummary.${field}").alias(field)
  }

  /** Method for extracting fields from `val.sessSummary`. */
  def sessSum(field: String): Column = {
    col(s"val.sessSummary.${field}").alias(field)
  }

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

  /** Extract the `justJoined` field as is from $ss. 
   * @example{{{
   * df.select(justJoined)
   * }}}
  */ 
  def justJoined(): Column = col("val.sessSummary.justJoined")

  /** Construct `hasJoined` from $ss using the `PbSS-Core-Utils` UDF from Conviva3D UDF-Lib on Databricks. 
   * @example{{{
   * df.select(hasJoined)
   * }}}
  */ 
  def hasJoined(): Column = hasJoinedUDF(col("val.sessSummary")).alias("hasJoined")

  /** Construct `isEBVS` (Exit Before Video Start) from $ss using the 
   *  `PbSS-Core-Utils` UDF from Conviva3D UDF-Lib on Databricks. 
   * @example{{{
   * df.select(isEBVS)
   * }}}
  */ 
  def isEBVS(): Column = EBVSUDF(col("val.sessSummary"), col("key.sessId")).alias("isEBVS")

  /** Construct `isVSF` (Video Start Failure) from $ss using the
   *  `PbSS-Core-Utils` UDF from Conviva3D UDF-Lib on Databricks. 
   * @example{{{
   * df.select(isVSF)
   * }}}
  */ 
  def isVSF(): Column = VSFUDF(col("val.sessSummary"), col("key.sessId")).alias("isVSF")

  /** Extract the `joinState` field as is from $ss
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

  /** Extract the `joinTime` field as is. */
  def joinTimeMs(): Column = col("val.sessSummary.joinTimeMs")

  /** Extract `lifePlayingTime` field as is from $ss. 
   * @example{{{
   * df.select(
   *   lifePlayingTimeMs
   * )
   * }}}
  */
  def lifePlayingTimeMs(): Column = col("val.sessSummary.lifePlayingTimeMs")


  /** Extract `lifeBufferingTimeMs` field as is from $ss. 
   * @example{{{
   * df.select(
   *   lifeBufferingTimeMs
   * )
   * }}}
  */
  def lifeBufferingTimeMs(): Column = col("val.sessSummary.lifeBufferingTimeMs")

  /** Create a field that flags if `joinTimeMs` is greater than zero,
   *  otherwise gets values less than or equal zero. 
   *  @example{{{
   *  df.select(isJoinTime)
   *  }}}
   */
  def isJoinTime(): Column = {
    when(joinTimeMs > 0, 1).otherwise(joinTimeMs).alias("isJoinTime")
  }

  /** Create a field that flags if `lifePlayingTimeMs` is greater than zero,
   *  otherwise gets values less than or equal zero. 
   *  @example{{{
   *  df.select(isLifePlayingTime)
   *  }}}
   */
  def isLifePlayingTime(): Column = {
    when(lifePlayingTimeMs > 0, 1).otherwise(lifePlayingTimeMs)
      .alias("isLifePlayingTime")
  }

  /** Create a field that flags if the session joined. 
   *  @example{{{
   *  df.select(isLifePlayingTime)
   *  }}}
   */
  def isJoined(): Column = {
    when(joinTimeMs === -1, 0).otherwise(1).alias("isJoined")
  }

  /** Create a field that indicates if there was any play over lifetime of the
   *  session. 
   *  @example{{{
   *  df.select(isPlay)
   *  }}}
   */
  def isPlay(): Column = {
    when(sessSum("lifePlayingTimeMs") > 0, true).otherwise(false).alias("isPlay")
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
    when(isJoinTime === -1 && joinState  === -1 && isPlay  === false, 1) 
      .when(isJoinTime === 1 && joinState   === 1 && isPlay  === true, 1)
      .when(isJoinTime === -3 && joinState  === 0 &&  isPlay  === true, 1) 
    .otherwise(0).alias("isConsistent")
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
    def preRollStartTime = field.getItem("preRollStartTimeMs")
  }
  /** Get field for AdContentMetadata. */
   def adContentMetadata = AdContentMetadata(
     field = col("val.sessSummary.AdContentMetadata"),
     name = "adContentMetadata")

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
  def c3_csid = IdCol(
    field = col("val.invariant.summarizedTags").getItem("c3.csid"),
    name = "c3_csid")

  /** Extracts the field `exitDuringPreRoll` as is from $ss. */ 
  def exitDuringPreRoll(): Column = col("val.sessSummary.exitDuringPreRoll")
  
  /** Creates the intvStartTime column $timestamp.
    * @example {{{
    * df.select(
    *   intvStartTime, 
    *   intvStartTime.ms,
    *   intvStartTime.stamp)
    * }}}
    */
  def intvStartTime = new TimeSecCol("val.sessSummary.intvStartTimeSec", "intvStartTime")

  /**
    * Parse the lifeFirstRecvTime column $timestamp.
    * @example {{{
    * df.select(
    *   lifeFirstRecvTime,
    *   lifeFirstRecvTime.sec,
    *   lifeFirstRecvTime.stamp)
    * }}}
    */
    def lifeFirstRecvTime = 
      new TimeMsCol("val.sessSummary.lifeFirstRecvTimeMs", "lifeFirstRecvTime")

  /**
    * Parse the firstRecvTime column $timestamp
    * @example {{{
    * df.select(
    *   firstRecvTime,
    *   firstRecvTime.sec,
    *   firstRecvTime.stamp)
    * }}}
    */
  def firstRecvTime = new TimeMsCol("key.firstRecvTimeMs", "firstRecvTime")

  /**
    * Parse the lastRecvTime column $timestamp.
    * @example {{{
    * df.select(
    *   lastRecvTime,
    *   lastRecvTime.sec,
    *   lastRecvTime.stamp)
    * }}}
    */
  def lastRecvTime = new TimeMsCol("val.sessSummary.lastRecvTimeMs", "lastRecvTime")

  /**
    * Creates the sessionCreationTime object with $timestamp.
    * @example {{{
    * df.select(
    *   sessionCreationTime,
    *   sessionCreationTime.sec,
    *   sessionCreationTime.stamp)
    * }}}
    */
  def sessionCreationTime = 
    new TimeMsCol("val.invariant.sessionCreationTimeMs", "sessionCreationTime")

  /**
    * Creates the sessionTimeMs field.
    * @example {{{
    * df.select(sessionTimeMs)
    * }}}
    */
  def sessionTimeMs(): Column = col("val.sessSummary.sessionTimeMs")

  /** Extract the `intvMaxEncodedFps` field. */
  def intvMaxEncodedFps(): Column = {
    col("val.sessSummary.d3SessSummary.intvMaxEncodedFps")
  }
  
  /** Extract the `lifeFramesEncoded` field. */
  def lifeEncodedFrames(): Column = {
    col("val.sessSummary.lifeEncodedFrames")
  }

  /** Extract the `lifeFramesRendered` field. */
  def lifeRenderedFrames(): Column = {
    col("val.sessSummary.lifeRenderedFrames")
  }

}

