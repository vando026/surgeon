package conviva.surgeon

object PbSSCoreLib {

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql._
  import com.conviva.parquet.{Converters, ReadPbFromParquet}
  import com.conviva.utils._
  import com.conviva.vmaStdMetrics.utils.TagMap
  import org.apache.parquet.io.api.{Converter, GroupConverter, PrimitiveConverter, Binary}
  import scala.collection.JavaConversions._
  import scala.collection.mutable.WrappedArray
  import scala.reflect.runtime.universe._
  import com.conviva.messages.livepass.SessInvariantPb
  import com.conviva.messages.livepass.SessSummary
  import com.conviva.vmaStdMetrics.sess.StdSess
  import com.conviva.messages.livepass.SessId
  import com.conviva.utils.LogInetAddress
  import com.conviva.utils.GeoCoder
  import org.apache.spark.sql.{functions => F}

  // Code taken from Conviva3D/UDF-Lib/PbSS-Core-Lib.scala
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

    def makeTagMap(inv: Row):Map[String, String] = {
      if (inv == null) 
          Map()
      else {
          val invPb = convertToPb[SessInvariantPb](inv, getGroupConverter[SessInvariantPb]("SessInvariantPb"))  
          (new TagMap(invPb)).toString.split("&")
          .map(_.split("=").map(_.trim))
          .filter(_.length == 2) 
          .map{case Array(key, value) => key -> value}.toMap
      }
  }

  def getStreamUrl(ss: Row) = {
    val streamUrl = buildSessSummary(ss).streamUrl()
    if (streamUrl == null)
      ""
    else
      streamUrl.toStr()
  }

  def getLongIPAddress(inv: Row): String = {
    if (inv == null) {
      return ""
    }
    val ip = buildStdSsInv(inv).invariant.publicIp()
    new LogInetAddress(ip).toString()
  }

}
