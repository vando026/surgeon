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
### Container methods

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

### Shorthand methods

There are several methods to select frequently used columns:

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
).show(false)
```
### Id methods

Surgeon provides the `IdCol` and `SID` classes for constructing and formatting Ids,
with asis (signed), nosign (unsigned), or hexadecimal methods. The `ID` class
handles the formatting. The `SID` class handles the concatenations of columns, for example,
`clientId` and `sessionId`, and assigns a name based on
the format selected. 


```scala mdoc
dat.select(
  customerId,
  sessionId,
  sessionId.nosign,
  sessionId.hex,
  sessionAdId,
  sessionAdId.nosign,
  sessionAdId.hex,
  clientId,
  clientId.asis,
  clientId.nosign,
  clientId.hex,
  sid5.asis,
  sid5Ad.asis,
  sessionAdId, 
).show(false)
```

### Time methods

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


### Geo methods
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

