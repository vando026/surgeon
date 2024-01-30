## Parquet RawLog (PbRl)

Surgeon tries to simply the selection of columns when reading a PbSS or PbRl dataset for the
first time. To demonstrate,  first import surgeon's `PbRl`  object and other `Spark` necessities,
set the file path, and read the data. 

```scala mdoc
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
val spark = SparkSession.builder
  .master("local[*]")
  .getOrCreate
```

```scala mdoc
import conviva.surgeon.PbRl._
// Read test data
val pbrlTestPath = "./surgeon/src/test/data" 
// Select only one client session Id
val dat = spark.read.parquet(s"${pbrlTestPath}/pbrlHourly1.parquet").cache
  .where(sessionId === 701891892)
val dat2 = spark.read.parquet(s"${pbrlTestPath}/pbrlHourly2.parquet")
```
### Quick selection

Quick ways to select frequently used columns:

```scala mdoc
dat.select(
  customerId,
  sessionId,
  clientId,
  timeStamp,
  sessionAdId, 
  seqNumber,
  dftot,
  dfcnt,
  sessionTimeMs,
  ipv6
).show(false)
```


Surgeon makes it easy to select data from arrays, maps, and other structs. This eliminates the
need for typing out long path names. The available container
methods with examples are:

```scala mdoc
dat.select(
  pbSdm("cwsSeekEvent"),             // root: payload.heartbeat.pbSdmEvents
  c3Tags("c3.client.osf"),           // root: payload.heartbeat.c3Tags
  clientTags("serviceName"),         // root: payload.heartbeat.clientTag
  cwsPlayer("playerState"),          // root: payload.heartbeat.cwsPlayerMeasurementEvent
  cwsStateChangeNew("playingState")  // root: payload.heartbeat.cwsStateChangeEvent.newCwsState
  ).show(false)
```


### Columns conversion and formatting

Surgeon provides methods for selecting, converting and formatting column
values. For example, the `clientId` column is an array of 4 values, which can
be easily concatenated into a single string using the `concat` method. Some
fields like `clientId`, `clientSessionId`, `publicIpv6` are  inconsistently
formatted as unsigned, signed, or hexadecimal across the PbSS and PbRl
datasets. Surgeon makes it easy to both concat and format these values as is
(`concat`), as unsigned (`concatToUnsigned`), or as hexadecimal
(`concatToHex`). Another example is `sid5`, a column concatenated from the
`clientId` and `sessionId` columns, which is easy to do with Surgeon.



```scala mdoc
dat.select(
  customerId,
  sessionId,
  sessionId.toUnsigned,
  sessionId.toHex,
  sessionAdId,
  sessionAdId.toUnsigned,
  sessionAdId.toHex,
  clientId,
  clientId.concat,
  clientId.concatToUnsigned,
  clientId.concatToHex,
  sid5.concat,
  sid5.concatToUnsigned,
  sid5.concatToHex,
  sid5Ad.concat,
  sid5Ad.concatToUnsigned,
  sid5Ad.concatToHex,
).show(false)
```

Another useful column is `customerName`, which returns the name of the `customerId`. 

```scala
dat.select(
  customerId, 
  customerName 
)
```

The `ipv4` and `ipv6` columns have methods for formatting: 

```scala 
dat.select(
  ipv4,            // Array asis, payload.heartbeat.publicIp
  ipv4.concat      // concat values
  ipv6,            // Array asis, payload.heartbeat.publicipv6
  ipv6.concat      // concat values 
  ipv6.concatToHex // concat values and convert to hexadecimal
)

```
### Time columns

Surgeon provides `TimeSecCol`, `TimeMsCol`, and `TimeUsCol` classes with
methods to work with Unix Epoch time columns. These classes extends the base
class of a column to add `toMs()`, `toSec()` or `stamp()` methods to existing
column methods (i.e., `alias`, `when`, etc). The list below shows the available
`Time*Col` columns with methods.

```scala mdoc
dat.select(
  timeStamp,                  // as is, microseconds (us) since unix epoch
  timeStamp.toMs,             // converts to ms since unix epoch
  timeStamp.toSec,            // converts to seconds since unix epoch
  timeStamp.stamp,            // converts to timestamp (HH:mm:ss)
  sessionCreationTime,
  sessionCreationTime.toSec,
  sessionCreationTime.stamp,
).show(false)
```


### GeoInfo columns
Surgeon has a method to extract geoInfo data and provides a convenient
method called `label` to assign labels to the numeric coded geo fields. 

```scala
dat.select(
  geoInfo("city"),    
  geoInfo("country"),
  geoInfo("city").label,
  geoInfo("country").label
).show
```

You can also assign your own labels to Ids using a custom Map.

```scala mdoc
val cityMap = Some(Map(289024 -> "Epernay"))
val countryMap = Some(Map(165 -> "Norway"))
dat.select(
  geoInfo("city", cityMap),    
  geoInfo("country", countryMap),
  geoInfo("city", cityMap).label,
  geoInfo("country", countryMap).label
).show
```

