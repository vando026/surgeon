## Parquet Session Summary  (PbSS)

Surgeon simplifies the selection of columns when reading a
dataset for the first time. To demonstrate, first import surgeon's `PbSS`  object and
other `Spark` necessities, set the file path, and read the data. 

```scala  
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import conviva.surgeon.Paths._
import conviva.surgeon.PbSS._

// Make path and read data
val path = Cust(Hourly(month = 5, days = 25, hours = 18), take = 1)
val dat = spark.read.parquet(path)
```

### Container methods

Surgeon provides several methods which make it easier to select columns or
columns from containers (arrays, maps, structs). These methods eliminate the
need for typing out long path names to the columns. The available container
methods are: 

``` scala 
dat.select(
  sessSum("playerState"), 
  d3SessSum("lifePausedTimeMs"),
  joinSwitch("playingTimeMs"),
  lifeSwitch("sessionTimeMs"),
  intvSwitch("networkBufferingTimeMs"), 
  invTags("sessionCreationTimeMs"), 
  sumTags("c3.video.isAd"), 
)
```
Any valid  string name can be used, provided the column exists. The container names are abbreviations of the root paths to the column names, as shown below:

``` scala 
dat.select(
  col("val.sessSummary.playerState"),
  col("val.sessSummary.d3SessSummary.lifePausedTimeMs"),
  col("val.sessSummary.joinSwitchInfos.playingTimeMs"),
  col("val.sessSummary.lifeSwitchInfos.sessionTimeMs"),
  col("val.sessSummary.intvSwitchInfos.networkBufferingTimeMs"),
  col("val.invariant.sessionCreationTimeMs"),
  col("val.invariant.summarizedTags").getItem("c3.video.isAd"),
)
```

### Shorthand methods

There are several methods that make the selection of frequently used columns as simple as
possible: 

```scala
dat.select(
  customerId, 
  clientId,
  sessionId,
  shouldProcess,
  hasEnded,
  justEnded,
  justJoined,
  endedStatus,
  joinState, 
  joinTimeMs,
  lifeBufferingTimeMs,
  lifeFirstRecvTime,
  lifeNetworkBufferingTimeMs,
  lifePlayingTimeMs,
  isLifePlayingTime,
  isJoinTime,
  isJoined,
  isPlay,
  isConsistent(),
  intvStartTime,
  firstRecvTime,
  lastRecvTime,
  sessionCreationTime,
  sessionTimeMs,
  intvMaxEncodedFps,
  exitDuringPreRoll
)
```

The `isConsistent` column is based on Oleg White's code: 

|isJoinTimeMs|joinState|isLifePlayingTimeMs| Comment |
|---         |---      |---                |---      |
|-1          |-1       |0                  | didn't join, zero life playing time |
|1           |1        |1                  | joined, known join time, positive life playing time |
|-3          |0        |1                  | joined, unknown join time, positive life playing time |
Any other combination is inconsistent.


### Id methods

Surgeon provides the `IdCol` and `SID` classes for constructing and formatting Ids,
with asis (signed), nosign (unsigned), or hexadecimal methods. The `ID` class
handles the formatting. The `SID` class handles the concatenations of columns, for example,
`clientSessionId` and `sessionId`, and assigns a name based on
the format selected. 

```scala 
dat.select(
  customerId,
  clientId,         // returns as is, signed array
  clientId.asis.    // returns as signed String
  clientId.nosign,  // returns unsigned String
  clientId.hex,     // returns hexadecimal String
  clientAdId,       // AdId (c3.csid) signed array
  clientAdId.asis,  // AdId (c3.csid) signed String
  clientId.nosign,  // AdId (c3.csid) unsigned String
  clientId.hex,     // AdId (c3.csid) hexadecimal String
  sessionId,        // signed
  sessionId.nosign, // unsigned
  sessionId.hex,    // hexadecimal
  sid5.asis,        // clientId:sessionId signed String
  sid5.nosign,      // clientId:sessionId unsigned String
  sid5.hex,         // clientId:sessionId hexadecimal String
  sid5Ad.asis,      // clientAdId:sessionId signed String
  sid5Ad.nosign,    // clientAdId:sessionId unsigned String
  sid5Ad.hex,       // clientAdId:sessionId hexadecimal String
  sid6.asis,        // clientAdId:sessionId:sessionCreationTime signed String
  sid6.nosign,      // clientAdId:sessionId:sessionCreationTime unsigned String
  sid6.hex          // clientAdId:sessionId:sessionCreationTime hexadecimal String
)
```
### Time methods

Surgeon provides a `TimeMsCol` with methods to work with time-related columns.
The `TimeMsCol` or `TimeSecCol` classes extends the base class of a column (`Column(expr)`) to
add `ms()`, `sec()` or `stamp()` methods to existing column methods (i.e., 
`alias`, `when`, etc). The list below shows the available `Time*Col` columns with methods.
 
```scala
dat.select(
  lifeFirstRecvTime,       // as is, ms since unix epoch
  lifeFirstRecvTime.sec,   // converts ms to seconds since unix epoch
  lifeFirstRecvTime.stamp, // converts to timestamp (HH:mm:ss)
  lifeFirstRecvTime, 
  lifeFirstRecvTime.sec,  
  lifeFirstRecvTime.stamp,
  lastRecvTime, 
  lastRecvTime.sec,  
  lastRecvTime.stamp,
  sessionCreationTime,
  sessionCreationTime.sec,
  sessionCreationTime.stamp,
  intvStartTime,           // as is, seconds since unix epoch
  intvStartTime.ms,        // converts seconds to ms since unix epoch
  intvStartTime.stamp
)
```

### Array methods
Surgeon provides an `ArrayCol` class with methods to work on array based
columns. The methods are shown below:

```scala
dat.select(
  lifeSwitch("framesPlayingTimeMs").sumInt,   // sum all non-null values in an array, returns Int
  lifeSwitch("framesPlayingTimeMs").notNull,  // returns array of non-null values
  lifeSwitch("framesPlayingTimeMs").allNull,  // checks if all values in array are null, returns boolean
  lifeSwitch("framesPlayingTimeMs").distinct, // returns array of distinct values
  lifeSwitch("framesPlayingTimeMs").first,    // returns first value of array
  lifeSwitch("framesPlayingTimeMs").last,     // returns last value of array
  lifeSwitch("framesPlayingTimeMs").min,      // returns min value of array
  lifeSwitch("framesPlayingTimeMs").max,      // returns max value of array
)
```

The `ArrayCol` classess are thus far limited to the `lifeSwitch`, `joinSwitch`, and
`intvSwitch` containers only. It goes without saying that you can use any valid
string argument to the containers (i.e., something other than
`framesPlayingTimeMs`).


### Ad methods
Surgeon also provides several methods for selecting Ad related fields.


```scala 
dat.select(
  clientAdId,      // short for sumTags("c3.csid")
  c3_csid,         // same as clientAdId
  c3isAd,          // short for sumTags("c3.video.isAd') 
  c3isAd.recode,   // recode labels as either true, false, or null
  adTech,          // field that identifies if ad tech is server or client side
  adTech.recode,   // recode the labels into either server, client, or unknown
  exitDuringPreRoll, 
  adContentMetadata, // get the container for adContentMetaData
  adContentMetadata.adRequested, // get specific fields by name
  adContentMetadata.preRollStatus,
  adContentMetadata.hasSSAI,
  adContentMetadata.hasCSAI, 
  adContentMetadata.preRollStartTime
)
```
