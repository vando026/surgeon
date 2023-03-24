package conviva.surgeon 
import conviva.surgeon.PbSSCoreLib._
import org.apache.spark.sql.functions.{udf}
import org.apache.spark.sql.{SparkSession, Row}

object Metrics {

  val joinTimeMsUDF = udf((ss: Row) => buildSessSummary(ss).joinTimeMs().toDouble)
  val hasJoinedUDF = udf((ss: Row) => buildStdSs(ss).hasJoined())
  val justJoinedUDF = udf((ss: Row) => buildStdSs(ss).isSessJustJoined())
  val VSFUDF = udf((ss: Row, id: Row) => buildStdSsWithId(ss, id).isVideoStartFailure()) 
  val EBVSUDF = udf((ss: Row, id: Row) => buildStdSsWithId(ss, id).isExitsBeforeVideoStart())

  // Life based metric
  val LifeBitrateUDF = udf((ss: Row) => buildStdSs(ss).lifeAvgBitrateKbp(0L).toDouble )
  val LifeBufferingUDF = udf((ss: Row) => buildSessSummary(ss).lifeBufferingTimeMs().toDouble )
  val LifePlayingUDF = udf((ss: Row) => buildSessSummary(ss).lifePlayingTimeMs().toDouble )
  val FirstHbTimeMsUDF = udf((ss: Row) => buildSessSummary(ss).lifeFirstRecvTimeMs().toDouble )
  val HasEndedUDF = udf((ss: Row) => buildSessSummary(ss).hasEnded() )

  // interval based metric
  // val UDFIntvBitrate = udf((ss: Row) => buildStdSs(ss).intvBitrateKbps().toDouble)
  // val UDFIntvBuffering = udf((ss: Row) => buildStdSs(ss).bufferingTimeMs().toDouble)
  // val UDFIntvPlaying = udf((ss: Row) => buildStdSs(ss).playingTimeMs().toDouble)
}

