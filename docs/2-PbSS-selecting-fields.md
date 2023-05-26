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
  sessSum("playerState"), 
  d3SessSum("lifePausedTimeMs"),
  joinSwitch("playingTimeMs"),
  lifeSwitch("sessionTimeMs"),
  intvSwitch("networkBufferingTimeMs"), 
  invTag("sessionCreationTimeMs"), 
  sumTags("c3.video.isAd"), 
)
```
You can, for example, use any string name that is in the container `val.sessSummary`, similarly for the other containers. 
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

### Id methods

Surgeon provides several methods for constructing and formatting Ids, as
described in the comments below. `clientId`, `sessionId`, `sid5`, and `sid6` all have
a signed, nosign (unsigned), or hexadecimal versions. 

```scala 
val idat = dat.select(
  customerId,
  clientId,         // signed
  clientId.nosign,  // unsigned
  clientId.hex,     // hexadecimal
  clientAdId,       // AdId (c3.csid) signed
  clientId.nosign,  // AdId (c3.csid) unsigned
  clientId.hex,     // AdId (c3.csid) hexadecimal
  sessionId,        // signed
  sessionId.nosign, // unsigned
  sessionId.hex,    // hexadecimal
  sid5.asis,        // clientId:sessionId signed
  sid5.nosign,      // clientId:sessionId unsigned
  sid5.hex,         // clientId:sessionId hexadecimal
  sid5Ad.asis,      // clientAdId:sessionId signed
  sid5Ad.nosign,    // clientAdId:sessionId unsigned
  sid5Ad.hex,       // clientAdId:sessionId hexadecimal
  sid6.asis,        // clientAdId:sessionId:sessionCreationTime signed
  sid6.nosign,      // clientAdId:sessionId:sessionCreationTime unsigned
  sid6.hex          // clientAdId:sessionId:sessionCreationTime hexadecimal
)
```


