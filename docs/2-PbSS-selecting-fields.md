## Session Summary 

Surgeon tries to simply the selection of fields when reading a dataset for the
first time. 

### Container methods

Surgeon provides several methods which make it easier to select fields. These
container methods are listed below, and are so called because they select
fields from various data containers (arrays, maps, structs) in session summary. 

Using Surgeon, first import the `PbSS`  class and other relevant `Spark`
classes. 

```scala mdoc 
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import conviva.surgeon.Paths._
import conviva.surgeon.PbSS._
```

Then do:

``` scala 
// Make path and read data
val path = Cust(Hourly(month = 5, days = 25, hours = 18), take = 1)
val dat = spark.read.parquet(path)

val dat2 = dat.select(
  sessSum("playerState"), // val.sessSummary
  d3SessSum("lifePausedTimeMs"), // val.sessSummary.d3SessSummary
  joinSwitch("playingTimeMs"), // val.sessSummary.joinSwitchInfos
  lifeSwitch("sessionTimeMs"), // val.sessSummary.lifeSwitchInfos
  intvSwitch("networkBufferingTimeMs"), // val.sessSummary.intvSwitchInfos
  invTags"sessionCreationTimeMs"), // val.invariant
  sumTags("c3.video.isAd"), // val.invariant.summarizedTags
)
```

The long version of this would be:

``` scala 
val dat3 = dat.select(
  col("val.sessSummary.playerState"),
  col("val.sessSummary.d3SessSummary.lifePausedTimeMs"),
  col("val.sessSummary.joinSwitchInfos.playingTimeMs"),
  col("val.sessSummary.lifeSwitchInfos.sessionTimeMs"),
  col("val.sessSummary.intvSwitchInfos.networkBufferingTimeMs"),
  col("val.invariant.sessionCreationTimeMs"),
  col("val.invariant.summarizedTags").getItem("c3.video.isAd"),
)
```
