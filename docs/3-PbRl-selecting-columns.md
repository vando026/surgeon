## Parquet RawLog (PbRl)

Surgeon tries to simply the selection of columns or fields when reading a dataset for the
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
```
### Container selection

Surgeon makes it select columns from containers (arrays, maps, structs). This  eliminates the
need for typing out long path names to the columns. The available container
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

A useful column is `customerName`, which returns the name of
the `customerId`. 

```scala
dat.select(
  customerId, 
  customerName 
)
```

The `ipv6` column comes with two methods, 

```scala 
dat.select(
  ipv6,            // Array asis, root: payload.heartbeat.publicIpv6
  ipv6.concat      // concat values in Array 
  ipv6.concatToHex // concat values and convert to hexadecimal
)

```

### Id selection, conversion, formatting

Surgeon provides methods for converting and formatting ID values or
Arrays. The `ID` class handles the conversion to hexadecimal or unsigned and
the `SID` class handles the concatenations of columns, for example,
`clientSessionId` and `sessionId`, and assigns a name based on the format
selected. 


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

### Time selection

Surgeon provides `TimeSecCol`, `TimeMsCol`, and `TimeUsCol` classes with methods to work with Unix Epoch time columns.
These classes extends the base class of a column (`Column(expr)`) to
add `toMs()`, `toSec()` or `stamp()` methods to existing column methods (i.e., 
`alias`, `when`, etc). The list below shows the available `Time*Col` columns with methods.

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


### Geo selection
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

