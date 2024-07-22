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
 * @define ss `val.sessSummary`
 * @example {{{
 * df.select(customerId, clientId.hex, hasEnded, justJoined)
 * }}}
 */

object PbSS {

  import conviva.surgeon.Sanitize._
  import org.apache.spark.sql.functions.{lower, col, when, lit, typedLit}
  import org.apache.spark.sql.{Column, Row}
  import conviva.surgeon.GeoInfo._
  import conviva.surgeon.Paths._
  import conviva.surgeon.Customer._
  import org.apache.spark.sql.{functions => F}

  /** Instantiate a class for C3 methods.**/
  val c3 = C3(ProdPbSS())

  /** Instantiate a method for Path methods. **/
  def pbss(date: String) = SurgeonPath(ProdPbSS()).make(date)

  /** Method to extract fields from the `lifeSwitchInfos` container. */
  def lifeSwitch(name: String): ArrayCol = {
    new ArrayCol(col(s"val.sessSummary.lifeSwitchInfos.$name"), name)
  }

  /** Method to extract fields from the `intvSwitchInfos` container. */
  def intvSwitch(name: String): ArrayCol = {
    new ArrayCol(col(s"val.sessSummary.intvSwitchInfos.$name"), name)
  }

  /** Method to extract fields from the `joinSwitchInfos` container. */
  def joinSwitch(name: String): ArrayCol = {
    new ArrayCol(col(s"val.sessSummary.joinSwitchInfos.$name"), name)
  }

  /** Method for extracting fields from `val.invariant`. */
  def invTags(field: String): Column = {
    col(s"val.invariant.${field}").alias(field)
  }

  /** Method for extracting fields from `val.invariant.c3Tags`. */
  def c3Tags(field: String = ""): Column = {
    val tag = col("val.invariant.c3Tags")
    if (field.isEmpty) tag 
    else tag.getField(field).alias(field.replaceAll("\\.", "_"))
  }

  /** Method for extracting fields from `val.invariant.summarizedTags`. Fields
   *  with periods are replaced with underscores by default.*/
  def sumTags(field: String  = ""): Column = {
    val tag = col("val.invariant.summarizedTags")
    if (field.isEmpty) tag 
    else tag.getItem(field).alias(field.replaceAll("\\.", "_"))
  }

  /** Method for extracting fields from `val.sessSummary.d3SessSummary`. */
  def d3SessSum(field: String): Column = {
    col(s"val.sessSummary.d3SessSummary.${field}").alias(field)
  }

  /** Method for extracting fields from `val.sessSummary`. */
  def sessSum(field: String): Column = {
    col(s"val.sessSummary.${field}").alias(field)
  }

  /** Method for extracting fields from `payload.heartbeat.geoInfo`. */
  def geoInfo(field: String) = GeoBuilder(ProdPbSS().geoUtilPath).make(field)

  /** Extract the `customerId` column as is.
   * @example{{{
   * df.select(customerId.asis)
   * }}}
  */ 
  def customerId(): Column = col("key.sessId.customerId")

  /** Extract the name of the `customerId` column.
   * @example{{{
   * df.select(
   *  customerId, 
   *  customerName
   * )
   * }}}
  */ 
  def customerName(): Column = CustomerName(ProdPbSS().geoUtilPath).make(customerId)

  /** Extract the `clientSessionId` column as is or $signed. The field is
   *  renamed to `sessionId`.
   * @example{{{
   * df.select(
   *  sessionId
   *  sessionId.toHex,
   *  sessionId.toUnsigned)
   * }}}
  */ 
  def sessionId() = new IdCol(col("key.sessId.clientSessionId"), "sessionId") 

  /** Create the `clientId` column asis or $signed. 
   * @example{{{
   * df.select(
   *  clientId,
   *  clientId.concat,
   *  clientId.concatToUnsigned, 
   *  clientId.concatToHex)
   * }}}  
  */ 
  def clientId = new IdArray(col("key.sessId.clientId.element").alias("clientId"), name = "clientId")

  /** Create sessionCreationId as an object with hex, signed, and nosign
   *  methods. Note that this is not the same as `sessionCreationTime` which
   *  has ms, sec, and timestamp methods. 
   *  @example{{{
   *  df.select(
   *    sessionCreationId,
   *    sessionCreationId.toHex,
   *    sessionCreationId.toUnsigned
   *  )
   *  }}}
   */
  def sessionCreationId = new IdCol(invTags("sessionCreationTimeMs"), "sessionCreationId")

  /** Create an sid5 object which concatenates `clientId` and `clientSessionId` $signed. 
   * @example{{{
   * df.select(
   *  sid5, 
   *  sid5.concat,
   *  sid5.concatToUnsigned, 
   *  sid5.concatToHex)
   * }}}  
  */ 
  def sid5 = SID(name = "sid5", clientId, sessionId)

  /** Create an sid6 object which concatenates `clientId`, `clientSessionId`, 
   *  `sessionCreationTime` $signed. 
   * @example{{{
   * df.select(
   *  sid6,
   *  sid6.concat, 
   *  sid6.concatToUnsigned, 
   *  sid6.concatToHex)
   * }}}  
  */ 
  def sid6 = SID(name = "sid6", clientId, sessionId, sessionCreationId)

  /** Creates an Ad SID5 object which concatenates `clientId` and `c3csid`
   *  $signed. 
   *  @example{{{
   *  df.select(
   *    sid5Ad, 
   *    sid5Ad.concat, 
   *    sid5Ad.concatToUnsigned, 
   *    sid5Ad.concatToHex
   *  )
   *  }}}
   */
  def sid5Ad = SID(name = "sid5Ad", clientId, c3csid)

  /** Extract the `shouldProcess` field as is.
   * @example{{{
   * df.select(shouldProcess)
   * }}}
  */ 
  def shouldProcess(): Column = sessSum("shouldProcess")

  /** Extract the `c3.viewer.id` field as is.
   * @example{{{
   * df.select(viewerId)
   * }}}
  */ 

  def viewerId(): Column = sumTags("c3.viewer.id")

  /** Extract the `hasEnded` field as is. 
   * @example{{{
   * df.select(hadEnded)
   * }}}
  */ 
  def hasEnded(): Column = sessSum("hasEnded")

  /** Extract the `justEnded` field as is.
   * @example{{{
   * df.select(justEnded)
   * }}}
  */ 
  def justEnded(): Column = sessSum("justEnded")

  /** Extract the `endedStatus` field as is.
   * @example{{{
   * df.select(endedStatus)
   * }}}
  */ 
  def endedStatus(): Column = sessSum("endedStatus")

  /** Extract the `justJoined` field as is from $ss. 
   * @example{{{
   * df.select(justJoined)
   * }}}
  */ 
  def justJoined(): Column = sessSum("justJoined")

  /** Extract the `joinState` field as is from $ss
   * @example{{{
   * df.select(joinState)
   * }}}
  */ 
  def joinState(): Column = sessSum("joinState")

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

  /** Extract `sessionState` field as is from $ss. 
   * @example{{{
   * df.select(
   *   sessionState
   * )
   * }}}
  */
 def sessionState(): Column = sessSum("sessionState")

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

  /** Extract `lifeNetworkBufferingTimeMs` field as is from $ss. 
   * @example{{{
   * df.select(
   *   lifeNetworkBufferingTimeMs
   * )
   * }}}
  */
  def lifeNetworkBufferingTimeMs(): Column = col("val.sessSummary.lifeNetworkBufferingTimeMs")

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

  /** Create a field that flags if `joinTimeMs` is greater than zero,
   *  otherwise gets values less than or equal zero. 
   *  @example{{{
   *  df.select(isJoinTime)
   *  }}}
   */
  def isJoinTime(): Column = {
    when(joinTimeMs > 0, 1).otherwise(joinTimeMs).alias("isJoinTime")
  }

  /** Create a field that flags if the session joined. 
   *  @example{{{
   *  df.select(isLifePlayingTime)
   *  }}}
   */
  def isJoined(): Column = {
    when(joinTimeMs === -1, 0).otherwise(1).alias("isJoined")
  }

  /** Extract the `joinTime` field as is. */
  def joinTimeMs(): Column = col("val.sessSummary.joinTimeMs")

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
  def isConsistent(): Column = {
    val joinTime = col("val.sessSummary.joinTimeMs")
    val joinState = col("val.sessSummary.joinState")
    val lifePlayingTime = col("val.sessSummary.lifePlayingTimeMs")
    val isJoinTime = when(joinTime > 0, 1).otherwise(joinTime)
    when(isJoinTime === -1 && joinState  === -1 && isPlay  === false, 1) 
      .when(isJoinTime === 1 && joinState   === 1 && isPlay  === true, 1)
      .when(isJoinTime === -3 && joinState  === 0 &&  isPlay  === true, 1) 
    .otherwise(0).alias("isConsistent")
  }

  /** Extract `Ad Technology` field with methods. */
  class AdTech(col: Column) extends Column(col.expr) {
    import org.apache.spark.sql.{Column, functions => F}
    // def check = this("val.sessSummary.joinTimeMs")
    def recode(): Column = {
       F.when(lower(col).rlike("client|csai"), "client")
        .when(lower(col).rlike("server|ssai"), "server")
        .otherwise("unknown")
        .alias("c3_adTech_rc")
    }
  }

  /** Create a c3 `Ad Technology` object with an `asis` and `recode` method. The
   *  `recode` method standardizes the field values into server, client, or
   *  unknown.
   *  @example{{{
   *  df.select(
   *    c3adTech, 
   *    c3adTech.recode
   *  )
   *  }}}
   */
  def c3adTech = new AdTech(sumTags("c3.ad.technology").alias("c3_adTech"))

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
  def c3isAd = new c3isAd(sumTags("c3.video.isAd").alias("c3_isAd"))

  /** Extract fields from `AdContentMetadata` with methods. */
  case class AdContentMetadata(col: Column) extends Column(col.expr) {
    def adRequested(): Column = col.getItem("adRequested").alias("adRequested")
    def preRollStatus(): Column = col.getItem("preRollStatus").alias("preRollStatus")
    def hasSSAI(): Column = col.getItem("hasSSAI").alias("hasSSAI")
    def hasCSAI(): Column = col.getItem("hasCSAI").alias("hasCSAI")
    def preRollStartTime = col.getItem("preRollStartTimeMs").alias("preRollStartTimeMs")
  }
  /** Get field for AdContentMetadata. */
  def adContentMetadata = AdContentMetadata(col("val.sessSummary.AdContentMetadata"))

  /** Creates a client session Id (c3.csid) object asis or $signed. 
   * @example{{{
   * df.select(
   *   c3csid.asis,
   *   c3csid.hex, 
   *   c3csid.nosign
   * )
   * }}}
   */ 
  def c3csid = new IdCol(sumTags("c3.csid"), "c3_csid")
  def sessionAdId = new IdCol(sumTags("c3.csid"), "sessionAdId")

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
  def intvStartTime = new TimeSecCol(sessSum("intvStartTimeSec"), "intvStartTime")

  /** Creates the intvStartTimeSec column $timestamp. */
  def intvStartTimeSec = new TimeSecCol(sessSum("intvStartTimeSec"), "intvStartTimeSec")

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
      new TimeMsCol(sessSum("lifeFirstRecvTimeMs"), "lifeFirstRecvTime")

  /**
    * Parse the firstRecvTime column $timestamp
    * @example {{{
    * df.select(
    *   firstRecvTime,
    *   firstRecvTime.sec,
    *   firstRecvTime.stamp)
    * }}}
    */
  def firstRecvTime = new TimeMsCol(col("key.firstRecvTimeMs"), "firstRecvTime")

  /**
    * Parse the lastRecvTime column $timestamp.
    * @example {{{
    * df.select(
    *   lastRecvTime,
    *   lastRecvTime.sec,
    *   lastRecvTime.stamp)
    * }}}
    */
  def lastRecvTime = new TimeMsCol(sessSum("lastRecvTimeMs"), "lastRecvTime")

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
    new TimeMsCol(invTags("sessionCreationTimeMs"), "sessionCreationTime")

  /**
    * Creates the sessionTimeMs field.
    * @example {{{
    * df.select(sessionTimeMs)
    * }}}
    */
  def sessionTimeMs(): Column = sessSum("sessionTimeMs")

  /** Extract the `intvMaxEncodedFps` field. */
  def intvMaxEncodedFps(): Column = d3SessSum("intvMaxEncodedFps")
  
  /** Extract the `lifeFramesEncoded` field. */
  def lifeEncodedFrames(): Column = sessSum("lifeEncodedFrames") 

  /** Extract the `lifeFramesRendered` field. */
  def lifeRenderedFrames(): Column = sessSum("lifeRenderedFrames") 

  /** Calculate Connection Induced Rebuffering Ratio (CIRR). */ 
  def CIRR(): Column = {
     (sessSum("lifeNetworkBufferingTimeMs") /
     (sessSum("lifeBufferingTimeMs").plus(sessSum("lifePlayingTimeMs"))) * lit(100))
      .alias("CIRR")
      
  }

  /** Extract the IPV6 field. **/
  def ipv6() = new IP6(invTags("publicipv6.element"), "ipv6")
  


}

