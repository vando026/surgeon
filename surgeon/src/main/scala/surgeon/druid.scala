package conviva.surgeon

object Druid {

  import org.apache.spark.sql.{SparkSession, DataFrame, Column}
  import org.apache.spark.sql.functions._

  def readDruid(jfile: String): DataFrame = {

    val jdat = SparkSession
      .builder.master("local[*]")
      .getOrCreate()
      .read.json(jfile)
    val segIds = jdat.select("segmentId").collect.map(_.getString(0))
    def getDat(seg: String): DataFrame = {
      val j1 = jdat.filter(col("segmentId") === seg)
      val j2 = j1.select(explode(col("events")).alias("Z"))
      j2.select(col("Z.*"))
    }

    val dat = segIds.map(getDat(_)).reduce(_ union _)
    dat.toDF(dat.columns.map(_.replace(".", "_")): _*)
  }

}
