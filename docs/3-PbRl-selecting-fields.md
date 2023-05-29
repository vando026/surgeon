## Parquet RawLog (PbRl)

Surgeon tries to simply the selection of columns or fields when reading a dataset for the
first time. To demonstrate,  first import surgeon's `PbRl`  object and other `Spark` necessities,
set the file path, and read the data. 

```scala  
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import conviva.surgeon.Paths._
import conviva.surgeon.PbRl._

// Make path and read data
val path = Cust(HourlyRaw(month = 5, days = 25, hours = 18), take = 1)
val dat = spark.read.parquet(path)
```


### Container methods

Surgeon provides several methods which make it easier to select columns or
columns from containers (arrays, maps, structs). These methods eliminate the
need for typing out long path names to the columns. The available container
methods are: 


```scala
dat.select(
  pbSdm("cwsSeekEvent"),             // root: payload.heartbeat.pbSdmEvents
  c3Tag("c3.client.osf"),            // root: payload.heartbeat.c3Tags
  clientTag("serviceName"),          // root: payload.heartbeat.clientTag
  cwsPlayer("playerState"),          // root: payload.heartbeat.cwsPlayerMeasurementEvent
  cwsStateChangeNew("playingState")  // root: payload.heartbeat.cwsStateChangeEvent.newCwsState
  )
```

### Shorthand methods

There are several methods that make the selection of frequently used columns as simple as
possible: 

```scala
dat.select(
  customerId,
  sessionId,
  clientId,
  timeStamp,
  clientAdId, 
  seqNumber,
  dftot,
  dfcnt,
  sessionTimeMs,
  )
```

### Id methods

Surgeon provides the `IdCol` and `SID` classes for constructing and formatting Ids,
with asis (signed), nosign (unsigned), or hexadecimal methods. The `ID` class
handles the formatting. The `SID` class handles the concatenations of columns, for example,
`clientId` and `sessionId`, and assigns a name based on
the format selected. 


```scala 
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
  clientAdId, 
  )
```

### Time methods

Surgeon provides `TimeSecCol`, `TimeMsCol`, and `TimeUsCol` classes with methods to work with time-related columns.
These classes extends the base class of a column (`Column(expr)`) to
add `ms()`, `sec()` or `stamp()` methods to existing column methods (i.e., 
`alias`, `when`, etc). The list below shows the available `Time*Col` columns with methods.

```scala 
dat.select(
  timeStamp,                // as is, microseconds (us) since unix epoch
  timeStamp.ms,             // converts to ms since unix epoch
  timeStamp.sec,            // converts to seconds since unix epoch
  timeStamp.stamp,          // converts to timestamp (HH:mm:ss)
  sessionCreationTime,
  sessionCreationTime.sec,
  sessionCreationTime.stamp,
  )
```


