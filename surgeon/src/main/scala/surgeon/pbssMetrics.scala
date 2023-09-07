// Project: surgeon
// Description: This code is taken directlty from Databricks
// Date: 23-Mar-2023

package conviva.surgeon


object PbSSCoreLib {

  // Conversion from Parquet to PacketBrain is based on com.conviva.parquet.Converters
  import org.apache.spark.sql.Row
  import com.conviva.parquet.{Converters, ReadPbFromParquet}
  import com.conviva.utils._
  import com.conviva.vmaStdMetrics.utils.TagMap
  import org.apache.parquet.io.api.{Converter, GroupConverter, PrimitiveConverter, Binary}
  import scala.collection.JavaConversions._
  import scala.collection.mutable.WrappedArray
  import scala.reflect.runtime.universe._


  def convertToPbMap(map: Map[String, String], mapConverter: Converters.MapConverter): Unit = {
    // mapConverter.getConverter(fieldIndex): fieldIndex is not important
    val topConverter = mapConverter.getConverter(0).asGroupConverter
    val keyConverter = topConverter.getConverter(0).asPrimitiveConverter
    val valConverter = topConverter.getConverter(1).asPrimitiveConverter
    
    mapConverter.start
    
    for((k, v) <- map) {
      keyConverter.addBinary(Binary.fromString(k))
      valConverter.addBinary(Binary.fromString(v))
    }
    
    mapConverter.end
  }

  def convertToPbArray(array: WrappedArray[_], arrayConverter: Converters.ArrayConverter[_]): Unit = {
    // com.conviva.parquet.Converters.ArrayConverter contains an "element-converter" to convert elements
    // in an array. If the element is of a primitive type, ArrayConverter will wrap a PrimitiveConverter
    // by a GroupConverter so that the "element-converter" is always of GroupConverter.
    if (array.length > 0) {
      val elemConverter = arrayConverter.getConverter(0).asGroupConverter
      for (elem <- array) {
        elem match {
          case row: Row => convertToPbStruct(row, elemConverter)
          case map: Map[String, String] @ unchecked => convertToPbMap(map, elemConverter.asInstanceOf[Converters.MapConverter])
          case arr: WrappedArray[_] => convertToPbArray(arr, elemConverter.asInstanceOf[Converters.ArrayConverter[_]])
          case pri => {
              elemConverter.start;
              convertToPbStruct(Row(pri), elemConverter);
              elemConverter.end;
          }
        }
      }
      
    }
  }

  def convertToPbStruct(row: Row, grpConverter: GroupConverter): Unit = {
    grpConverter.start
    
    for (i <- (0 until row.length)) {
      val converter = grpConverter.getConverter(i)
      val v = row.get(i)
      if (v != null) {
        converter match {
          case primitiveConverter: PrimitiveConverter => v match {
            case s: String => primitiveConverter.addBinary(Binary.fromString(s))
            case b: Boolean => primitiveConverter.addBoolean(b) 
            case i: Int => primitiveConverter.addInt(i) 
            case sh: Short => primitiveConverter.addInt(sh)
            case by: Byte => primitiveConverter.addInt(by)
            case l: Long => primitiveConverter.addLong(l)
            case f: Float => primitiveConverter.addFloat(f)
            case d: Double => primitiveConverter.addDouble(d)
          }
          case arrayConverter: Converters.ArrayConverter[_] => convertToPbArray(v.asInstanceOf[WrappedArray[_]], arrayConverter)
          case mapConverter: Converters.MapConverter => convertToPbMap(v.asInstanceOf[Map[String, String]], mapConverter)
          case groupConverter: Converters.SimpleGroupConverter[_] => convertToPbStruct(v.asInstanceOf[Row], groupConverter)
        }
      }
     }
    
    grpConverter.end
  }

  def getGroupConverter[T](targetPbClassName: String): Converters.SimpleGroupConverter[T] = {
    val packageName = "com.conviva.parquet.records"
    val parquetClassName = packageName + "." + targetPbClassName + "Parquet"
    val readSupport = Class.forName(parquetClassName).getDeclaredMethod("getReader").invoke(null).asInstanceOf[ReadPbFromParquet[_]]
    readSupport.getRecordMaterializer.getRootConverter.asInstanceOf[Converters.SimpleGroupConverter[T]]
  }

  def convertToPb[T](row: Row, groupConverter: Converters.SimpleGroupConverter[T]): T = {
    convertToPbStruct(row, groupConverter)
    groupConverter.getCurrentRecord.asInstanceOf[T]  
  }

  import com.conviva.messages.livepass.SessSummary
  import com.conviva.vmaStdMetrics.sess.StdSess
  import com.conviva.messages.livepass.SessId
  import com.conviva.messages.livepass.SessInvariantPb

  def buildSessId(ssId: Row): SessId = {
    convertToPb[SessId](ssId, getGroupConverter[SessId]("SessId"))
  }

  def buildSessSummary(ss: Row): SessSummary = {
    convertToPb[SessSummary](ss, getGroupConverter[SessSummary]("SessSummary"))
  }

  def buildStdSs(ss: Row, ssId: Row = null): StdSess = {
      val ssPb = buildSessSummary(ss)

      StdSess.builder.withSessSummary(ssPb).build()
  }

  def buildStdSsWithId(ss: Row, ssId: Row): StdSess = {
      val id = buildSessId(ssId)
      val ssPb = buildSessSummary(ss)
      StdSess.builder.withSessSummary(ssPb).withSessId(id).build()
  }
  def buildStdSsInv(inv: Row) = {
    if (inv == null) {
      null
    }
    val invPb = convertToPb[SessInvariantPb](inv, getGroupConverter[SessInvariantPb]("SessInvariantPb"))
    StdSess.builder.withInvariant(invPb).build()
  }

  def buildFullStdSs(inv: Row, ss: Row) =  {
    val invPb = convertToPb[SessInvariantPb](inv, getGroupConverter[SessInvariantPb]("SessInvariantPb"))
    val sessSummaryPb = convertToPb[SessSummary](ss, getGroupConverter[SessSummary]("SessSummary"))
    StdSess.builder.withInvariant(invPb).withSessSummary(sessSummaryPb).build()
  }

}


object Metrics {
  import org.apache.spark.sql.functions.{udf}
  import org.apache.spark.sql.{SparkSession, Row}

  val joinTimeMsUDF = udf((ss: Row) => buildSessSummary(ss).joinTimeMs().toDouble)
  val hasJoinedUDF = udf((ss: Row) => buildStdSs(ss).hasJoined())
  val justJoinedUDF = udf((ss: Row) => buildStdSs(ss).isSessJustJoined())
  val VSFUDF = udf((ss: Row, id: Row) => buildStdSsWithId(ss, id).isVideoStartFailure()) 
  val EBVSUDF = udf((ss: Row, id: Row) => buildStdSsWithId(ss, id).isExitsBeforeVideoStart())
  val isVPFUDF = udf((ss: Row) => buildStdSs(ss).isVideoMidstreamFailure())

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


