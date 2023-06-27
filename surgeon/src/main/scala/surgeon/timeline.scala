object Timeline {

  import org.apache.spark.sql.{Column}
  import org.apache.spark.sql.{DataFrame}
  import org.apache.spark.sql.{functions => F}
  import conviva.surgeon.PbRl._
  import org.apache.spark.sql.expressions.Window

  case class PlayerState(dat: DataFrame) {

    val playerData = dat.select(
        sessionId, seqNumber, 
        timeStamp.stamp,
        sessionCreationTime,
        F.explode(F.arrays_zip(
          sessionTimeMs, 
          cwsStateChangeNew("playingState"), 
          pbSdm("cwsSeekEvent").getItem("actionType").alias("actionType"),
        )).alias("dict")
      )
      .withColumn("time", F.col("dict").getItem("sessionTimeMs"))
      .withColumn("eventTimeStamp", F.from_unixtime( (F.col("sessionCreationTimeMs") + F.col("time")) / 1000))
      .withColumn("state", F.col("dict").getItem("playingState"))
      .withColumn("seek", F.col("dict").getItem("actionType"))
      .drop("dict", "sessionCreationTimeMs")
      .orderBy("sessionId", "eventTimeStamp")

      // Calculate player state changes in the timeline: player timeline
    private val dw = Window.partitionBy("sessionId").orderBy(F.asc("time"))
    private val sw = dw.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val stateData = playerData
      .withColumn("timeIntv", (F.lag(F.col("time"), -1).over(dw)) - F.col("time")) // calculate interval time
      .withColumn("state", F.last(F.col("state"), true).over(sw)) // forward fill player state vals
      .withColumn("lagState1", F.lag(F.col("state"), 1).over(dw)) // detect player state change
      // assign Id to state change
      .withColumn("stateChange", F.when(F.col("state") =!= F.col("lagState1"), F.row_number.over(dw))) 
      .withColumn("stateId", F.last(F.col("stateChange"), true).over(sw)) // carry IDs forward
      .drop("stateChange", "lagState1")

    // Calculate seek timeline: seek buffering for 10 second window from pss/pse, 
    // and includes contiguous buffering states after 10 sec window (until state change to something else)
    val seekData = stateData
      .withColumn("pssTime", F.last(F.when(F.col("seek") === "pss", F.col("time")), true).over(dw)) 
      // detect pss state, carryforward
      .withColumn("pssTimeDiff", F.col("Time") - F.col("pssTime")) // timeDiff since first pss state change
      .withColumn("pssSpan", F.when(F.col("pssTimeDiff") < 10e3, 1)) // is this timeDiff <= 10 seconds
      // detect pse state, carryforward
      .withColumn("pseTime", F.last(F.when(F.col("seek") === "pse", F.col("time")), true).over(dw)) 
      .withColumn("pseTimeDiff", F.col("Time") - F.col("pseTime")) // timeDiff since first pse state change
      .withColumn("pseSpan", F.when(F.col("pseTimeDiff") < 10e3, 1)) // is this timeDiff <= 10 seconds
      .withColumn("seekSpan10", F.when(F.col("pssSpan") === 1 || F.col("pseSpan") === 1, 1)) // is the union of pss/pse spans
      // check if buffering is the next state after the seek10Span
      .withColumn("leadState1", F.lead(F.col("state"), 1, 0).over(dw))  
      // if these two states overlap, then seek10Span followed by buffering
      .withColumn("bufAfterSeek10", F.when(F.col("leadState1") === 6 && F.col("seekSpan10") === 1, 1)) 
      .withColumn("seekSpanBuff", // carry bufAfterSeek10 forward until next state change (ie not buffering)
        F.last(F.col("bufAfterSeek10"), true).over(Window.partitionBy("sessionId", "stateId").orderBy(F.asc("time")))) 
      // is the union of seekSpan10 and buffAfterSeek10
      .withColumn("seekSpan", F.when(F.col("seekSpan10") === 1 || F.col("seekSpanBuff") === 1, 1)) 
      .drop("pssTime", "pssTimeDiff", "pseTime", 
        "pseTimeDiff", "pssSpan", "pseSpan", "leadState1", 
        "seekSpan10", "bufAfterSeek10", "seekSpanBuff")

    // calculate main metrics: playTime, bufferTime, and hasJoined
    val metricData = seekData
      .withColumn("isJoin", F.last(F.when(F.col("state") === 3, 1), true).over(dw))
      .withColumn("playTime", F.when(F.col("state") === 3, F.col("timeIntv")))
      .withColumn("bufferTime", F.when(F.col("state") === 6, F.col("timeIntv")).otherwise(F.lit(0)))
      .withColumn("seekBufferTime", F.when(F.col("seekSpan") === 1 && F.col("state") === 6, F.col("timeIntv"))
        .otherwise(F.lit(0)))

    // condition: drop consecutive bufferState for > 90 seconds
    val buffer90Drop = metricData
      .withColumn("leadState1", F.lag(F.col("state"), 1).over(dw)) // detect player state change
      // assign Id to state change
      .withColumn("stateChange", F.when(F.col("state") =!= F.col("leadState1"), F.row_number.over(dw))) 
      .withColumn("stateGroup", F.last(F.col("stateChange"), true).over(dw)) // carry IDs forward
      // sum all time in state
      .withColumn("stateSum", F.sum("timeIntv").over(Window.partitionBy("sessionId", "stateGroup"))) 
      // exclude buffer time F.when >= 120 seconds
      .withColumn("bufferTime", F.when(F.col("state") === 6 && F.col("stateSum") > 12e4, 0)
        .otherwise(F.col("bufferTime"))) 
      .withColumn("seekBufferTime", F.when(F.col("state") === 6 && F.col("stateSum") > 12e4, 0)
        .otherwise(F.col("seekBufferTime"))) // exclude buffer time F.when >= 120 seconds
      .drop("leadState1", "stateChange")
  }
}
