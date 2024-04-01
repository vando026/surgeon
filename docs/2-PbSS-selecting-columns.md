```scala mdoc
// setup code
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
val spark = SparkSession.builder.master("local[*]").getOrCreate
```

## Parquet Session Summary  (PbSS)

Surgeon simplifies the selection of columns when reading a dataset for the
first time. For this demonstration, set the file paths to the test environment
and load the data. 

```scala mdoc
import conviva.surgeon.PbSS._ 
import conviva.surgeon.GeoInfo._
import conviva.surgeon.Paths._

// Get data from the test env, not prod env
val path = Path.pbss("2023-02-07T02").c3id(1960180360).toList(0)
```

```scala mdoc
// now we can read the pbss data
val dat0 = spark.read.parquet(path).cache
// Select only one client session Id
val dat = dat0.where(sessionId === 89057425)
```

### Container selction

Surgeon makes it easy to select columns or columns from containers (arrays,
maps, structs), which eliminates the need to type out long path names to the
columns: 

```scala mdoc
dat.select(
  sessSum("playerState"), 
  d3SessSum("lifePausedTimeMs"),
  joinSwitch("playingTimeMs"),
  lifeSwitch("sessionTimeMs"),
  intvSwitch("networkBufferingTimeMs"), 
  invTags("sessionCreationTimeMs"), 
  sumTags("c3.video.isAd"), 
).show
```
Any valid  string name can be used, provided the column exists. The container names are abbreviations of the root paths to the column names, as shown below:

```scala mdoc
dat.select(
  col("val.sessSummary.playerState"),
  col("val.sessSummary.d3SessSummary.lifePausedTimeMs"),
  col("val.sessSummary.joinSwitchInfos.playingTimeMs"),
  col("val.sessSummary.lifeSwitchInfos.sessionTimeMs"),
  col("val.sessSummary.intvSwitchInfos.networkBufferingTimeMs"),
  col("val.invariant.sessionCreationTimeMs"),
  col("val.invariant.summarizedTags").getItem("c3.video.isAd"),
).show
```

### Quick selection

This makes the selection of frequently used columns as simple as
possible: 

```scala mdoc
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
).show
```

The `isConsistent` column is based on the logic: 

|isJoinTimeMs|joinState|isLifePlayingTimeMs| Comment |
|---         |---      |---                |---      |
|-1          |-1       |0                  | didn't join, zero life playing time |
|1           |1        |1                  | joined, known join time, positive life playing time |
|-3          |0        |1                  | joined, unknown join time, positive life playing time |
Any other combination is inconsistent.

Another useful shorthand method is `customerName`, which returns the name of
the `customerId`. On DataBricks production environment, you should be able to the run the command as
is.

```scala
dat.select(
  customerId, 
  customerName
).show

// +----------+-------------+
// |customerId|customerName |                                   
// +----------+-------------+
// |1960180360|c3.TopServe  |
// +----------+-------------+
```


### Id selection, conversion, formatting

Surgeon an easy way to convert and format ID values or
Arrays. The `ID` class handles the conversion to hexadecimal or unsigned and
the `SID` class handles the concatenations of columns, for example,
`clientSessionId` and `sessionId`, and assigns a name based on the format
selected. 

```scala mdoc
dat.select(
  customerId,
  clientId,                     // returns Array as is
  clientId.concat,              // returns concated String
  clientId.concatToUnsigned,    // returns concated unsigned String
  clientId.concatToHex,         // returns concated hexadecimal String
  sessionAdId,                  // AdId (c3.csid) returns as is
  sessionAdId.toUnsigned,       // AdId (c3.csid) unsigned 
  sessionAdId.toHex,            // AdId (c3.csid) hexadecimal 
  sessionId,                    // return as is
  sessionId.toUnsigned,         // unsigned
  sessionId.toHex,              // hexadecimal
  sid5.concat,                  // clientId:sessionId String
  sid5.concatToUnsigned,        // clientId:sessionId unsigned String
  sid5.concatToHex,             // clientId:sessionId hexadecimal String
  sid5Ad.concat,                // clientAdId:sessionId String
  sid5Ad.concatToUnsigned,      // clientAdId:sessionId unsigned String
  sid5Ad.concatToHex,           // clientAdId:sessionId hexadecimal String
  sid6.concat,                  // clientAdId:sessionId:sessionCreationTime String
  sid6.concatToUnsigned,        // clientAdId:sessionId:sessionCreationTime unsigned String
  sid6.concatToHex              // clientAdId:sessionId:sessionCreationTime hexadecimal String
).show(false)
```

### Time selection

Surgeon provides a `TimeMsCol` with methods to work with time-related columns.
The `TimeMsCol` or `TimeSecCol` classes extends the base class of a column (`Column(expr)`) to
add `toMs()`, `toSec()` or `stamp()` methods to existing column methods (i.e., 
`alias`, `when`, etc). The list below shows the available `Time*Col` columns with methods.
 
```scala mdoc
dat.select(
  lifeFirstRecvTime,         // as is, ms since unix epoch
  lifeFirstRecvTime.toSec,   // converts ms to seconds since unix epoch
  lifeFirstRecvTime.stamp,   // converts to timestamp (HH:mm:ss)
  lifeFirstRecvTime, 
  lifeFirstRecvTime.toSec,  
  lifeFirstRecvTime.stamp,
  lastRecvTime, 
  lastRecvTime.toSec,  
  lastRecvTime.stamp,
  sessionCreationTime,
  sessionCreationTime.toSec,
  sessionCreationTime.stamp,
  intvStartTime,            // as is, seconds since unix epoch
  intvStartTime.toMs,       // converts seconds to ms since unix epoch
  intvStartTime.stamp
).show
```

### Array methods
Surgeon provides an `ArrayCol` class with methods to work on array based
columns. The methods are shown below:

```scala mdoc
dat.select(
  lifeSwitch("framesPlayingTimeMs").sumInt,   // sum all non-null values in an array, returns Int
  lifeSwitch("framesPlayingTimeMs").dropNull, // returns array of non-null values
  lifeSwitch("framesPlayingTimeMs").allNull,  // checks if all values in array are null, returns boolean
  lifeSwitch("framesPlayingTimeMs").distinct, // returns array of distinct values
  lifeSwitch("framesPlayingTimeMs").first,    // returns first value of array
  lifeSwitch("framesPlayingTimeMs").last,     // returns last value of array
  lifeSwitch("framesPlayingTimeMs").min,      // returns min value of array
  lifeSwitch("framesPlayingTimeMs").max,      // returns max value of array
).show(false)
```

The `ArrayCol` classess are thus far limited to the `lifeSwitch`, `joinSwitch`, and
`intvSwitch` containers only. It goes without saying that you can use any valid
string argument to the containers (i.e., something other than
`framesPlayingTimeMs`).


### Ad selection
Surgeon also provides several methods for selecting Ad related fields.


```scala mdoc
dat.select(
  sessionAdId,                    // short for sumTags("c3.csid")
  c3csid,                         // same as clientAdId
  c3isAd,                         // short for sumTags("c3.video.isAd') 
  c3isAd.recode,                  // recode labels as either true, false, or null
  c3adTech,                       // field that identifies if ad tech is server or client side
  c3adTech.recode,                // recode the labels into either server, client, or unknown
  exitDuringPreRoll, 
  adContentMetadata,              // get the container for adContentMetaData
  adContentMetadata.adRequested,  // get specific fields by name
  adContentMetadata.preRollStatus,
  adContentMetadata.hasSSAI,
  adContentMetadata.hasCSAI, 
  adContentMetadata.preRollStartTime
).show(false)
```

### GeoInfo selection

You can select `GeoInfo` columns and their labels from `val.invariant.geoInfo` (of class `geoInfo`) like so:


```scala 
import conviva.surgeon.GeoInfo._
hourly_df.select(
  geoInfo("city")      // Int: the city codes
  geoInfo("country")   // Int: the country codes
)
```
To see the labels rather than the numeric codes, you can do:

```scala 
hourly_df.select(
  geoInfo("city").label      // Int: the city codes
  geoInfo("country").label   // Int: the country codes
)

```


You can provide your own custom Map of labels for the `geoInfo` codes. Make sure to
pass it to `Some`. 

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

> Compiled using version @VERSION@. 
