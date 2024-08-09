
## Parquet Session Summary  (PbSS)

Surgeon simplifies the selection of columns when reading a dataset for the
first time. Data from Surgeon's test data (on Github) is used for this
demonstration. 

```scala
import org.surgeon.PbSS._
```


```scala
val path = pbss("2023-02-07T02").c3id(1960180360)
// path: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}"
val dat0 = spark.read.parquet(path).cache
// dat0: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [key: struct<sessId: struct<customerId: int, clientId: array<struct<element:int>> ... 1 more field>, type: tinyint ... 1 more field>, val: struct<type: tinyint, sessSummary: struct<intvStartTimeSec: int, joinTimeMs: int ... 119 more fields> ... 14 more fields>]
// Select only one client session Id
val dat = dat0.where(sessionId === 89057425)
// dat: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [key: struct<sessId: struct<customerId: int, clientId: array<struct<element:int>> ... 1 more field>, type: tinyint ... 1 more field>, val: struct<type: tinyint, sessSummary: struct<intvStartTimeSec: int, joinTimeMs: int ... 119 more fields> ... 14 more fields>]
```

### Container selction

Surgeon makes it easy to select columns or columns from containers (arrays,
maps, structs), which eliminates the need to type out long path names to the
columns: 

```scala
dat.select(
  sessSum("playerState"), 
  d3SessSum("lifePausedTimeMs"),
  joinSwitch("playingTimeMs"),
  lifeSwitch("sessionTimeMs"),
  intvSwitch("networkBufferingTimeMs"), 
  invTags("sessionCreationTimeMs"), 
  sumTags("c3.video.isAd"), 
).show(false)
// +-----------+----------------+-------------+-------------+----------------------+---------------------+-------------+
// |playerState|lifePausedTimeMs|playingTimeMs|sessionTimeMs|networkBufferingTimeMs|sessionCreationTimeMs|c3_video_isAd|
// +-----------+----------------+-------------+-------------+----------------------+---------------------+-------------+
// |3          |0               |[0]          |[1906885]    |[2375]                |1675765692087        |F            |
// +-----------+----------------+-------------+-------------+----------------------+---------------------+-------------+
//
```
Any valid  string name can be used, provided the column exists. The container names are abbreviations of the root paths to the column names, as shown below:

```scala
dat.select(
  col("val.sessSummary.playerState"),
  col("val.sessSummary.d3SessSummary.lifePausedTimeMs"),
  col("val.sessSummary.joinSwitchInfos.playingTimeMs"),
  col("val.sessSummary.lifeSwitchInfos.sessionTimeMs"),
  col("val.sessSummary.intvSwitchInfos.networkBufferingTimeMs"),
  col("val.invariant.sessionCreationTimeMs"),
  col("val.invariant.summarizedTags").getItem("c3.video.isAd"),
).show(false)
// +-----------+----------------+-------------+-------------+----------------------+---------------------+-------------------------------------------+
// |playerState|lifePausedTimeMs|playingTimeMs|sessionTimeMs|networkBufferingTimeMs|sessionCreationTimeMs|val.invariant.summarizedTags[c3.video.isAd]|
// +-----------+----------------+-------------+-------------+----------------------+---------------------+-------------------------------------------+
// |3          |0               |[0]          |[1906885]    |[2375]                |1675765692087        |F                                          |
// +-----------+----------------+-------------+-------------+----------------------+---------------------+-------------------------------------------+
//
```

### Quick selection

This makes the selection of frequently used columns as simple as
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
).show
// +----------+--------------------+---------+-------------+--------+---------+----------+-----------+---------+----------+-------------------+-------------------+--------------------------+-----------------+-----------------+----------+--------+------+------------+----------------+---------------+--------------+---------------------+-------------+-----------------+-----------------+
// |customerId|            clientId|sessionId|shouldProcess|hasEnded|justEnded|justJoined|endedStatus|joinState|joinTimeMs|lifeBufferingTimeMs|lifeFirstRecvTimeMs|lifeNetworkBufferingTimeMs|lifePlayingTimeMs|isLifePlayingTime|isJoinTime|isJoined|isPlay|isConsistent|intvStartTimeSec|firstRecvTimeMs|lastRecvTimeMs|sessionCreationTimeMs|sessionTimeMs|intvMaxEncodedFps|exitDuringPreRoll|
// +----------+--------------------+---------+-------------+--------+---------+----------+-----------+---------+----------+-------------------+-------------------+--------------------------+-----------------+-----------------+----------+--------+------+------------+----------------+---------------+--------------+---------------------+-------------+-----------------+-----------------+
// |1960180360|[476230728, 12930...| 89057425|         true|   false|    false|      true|          0|        1|      4978|               2375|      1675765693115|                      2375|          1742812|                1|         1|       1|  true|           1|      1675764000|  1675765693115| 1675767600000|        1675765692087|      1906885|               -1|            false|
// +----------+--------------------+---------+-------------+--------+---------+----------+-----------+---------+----------+-------------------+-------------------+--------------------------+-----------------+-----------------+----------+--------+------+------------+----------------+---------------+--------------+---------------------+-------------+-----------------+-----------------+
//
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
).show(false)
// +----------+------------+
// |customerId|customerName|
// +----------+------------+
// |1960180360|c3.TopServe |
// +----------+------------+
//
```


### Id selection, conversion, formatting

Surgeon an easy way to convert and format ID values or
Arrays. The `ID` class handles the conversion to hexadecimal or unsigned and
the `SID` class handles the concatenations of columns, for example,
`clientSessionId` and `sessionId`, and assigns a name based on the format
selected. 

```scala
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
// +----------+-------------------------------------------------+--------------------------------------------+------------------------------------------+-----------------------------------+-----------+-------------------+--------------+---------+-----------------+------------+-----------------------------------------------------+---------------------------------------------------+-------------------------------------------+--------------------------------------------+------------------------------------------+-----------------------------------+-------------------------------------------------------------------+-------------------------------------------------------------+----------------------------------------------------+
// |customerId|clientId                                         |clientId                                    |clientIdUnsigned                          |clientIdHex                        |sessionAdId|sessionAdIdUnsigned|sessionAdIdHex|sessionId|sessionIdUnsigned|sessionIdHex|sid5                                                 |sid5Unsigned                                       |sid5Hex                                    |sid5Ad                                      |sid5AdUnsigned                            |sid5AdHex                          |sid6                                                               |sid6Unsigned                                                 |sid6Hex                                             |
// +----------+-------------------------------------------------+--------------------------------------------+------------------------------------------+-----------------------------------+-----------+-------------------+--------------+---------+-----------------+------------+-----------------------------------------------------+---------------------------------------------------+-------------------------------------------+--------------------------------------------+------------------------------------------+-----------------------------------+-------------------------------------------------------------------+-------------------------------------------------------------+----------------------------------------------------+
// |1960180360|[476230728, 1293028608, -1508640558, -1180571212]|476230728:1293028608:-1508640558:-1180571212|476230728:1293028608:2786326738:3114396084|1c62b448:4d120d00:a613f8d2:b9a1e9b4|null       |null               |null          |89057425 |89057425         |54ee891     |476230728:1293028608:-1508640558:-1180571212:89057425|476230728:1293028608:2786326738:3114396084:89057425|1c62b448:4d120d00:a613f8d2:b9a1e9b4:54ee891|476230728:1293028608:-1508640558:-1180571212|476230728:1293028608:2786326738:3114396084|1c62b448:4d120d00:a613f8d2:b9a1e9b4|476230728:1293028608:-1508640558:-1180571212:89057425:1675765692087|476230728:1293028608:2786326738:3114396084:89057425:728446647|1c62b448:4d120d00:a613f8d2:b9a1e9b4:54ee891:2b6b36b7|
// +----------+-------------------------------------------------+--------------------------------------------+------------------------------------------+-----------------------------------+-----------+-------------------+--------------+---------+-----------------+------------+-----------------------------------------------------+---------------------------------------------------+-------------------------------------------+--------------------------------------------+------------------------------------------+-----------------------------------+-------------------------------------------------------------------+-------------------------------------------------------------+----------------------------------------------------+
//
```

### Time selection

Surgeon provides a `TimeMsCol` with methods to work with time-related columns.
The `TimeMsCol` or `TimeSecCol` classes extends the base class of a column (`Column(expr)`) to
add `toMs()`, `toSec()` or `stamp()` methods to existing column methods (i.e., 
`alias`, `when`, etc). The list below shows the available `Time*Col` columns with methods.
 
```scala
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
// +-------------------+--------------------+----------------------+-------------------+--------------------+----------------------+--------------+---------------+-------------------+---------------------+----------------------+------------------------+----------------+---------------+-------------------+
// |lifeFirstRecvTimeMs|lifeFirstRecvTimeSec|lifeFirstRecvTimeStamp|lifeFirstRecvTimeMs|lifeFirstRecvTimeSec|lifeFirstRecvTimeStamp|lastRecvTimeMs|lastRecvTimeSec|  lastRecvTimeStamp|sessionCreationTimeMs|sessionCreationTimeSec|sessionCreationTimeStamp|intvStartTimeSec|intvStartTimeMs| intvStartTimeStamp|
// +-------------------+--------------------+----------------------+-------------------+--------------------+----------------------+--------------+---------------+-------------------+---------------------+----------------------+------------------------+----------------+---------------+-------------------+
// |      1675765693115|          1675765693|   2023-02-07 02:28:13|      1675765693115|          1675765693|   2023-02-07 02:28:13| 1675767600000|     1675767600|2023-02-07 03:00:00|        1675765692087|            1675765692|     2023-02-07 02:28:12|      1675764000|  1675764000000|2023-02-07 02:00:00|
// +-------------------+--------------------+----------------------+-------------------+--------------------+----------------------+--------------+---------------+-------------------+---------------------+----------------------+------------------------+----------------+---------------+-------------------+
//
```

### Array methods
Surgeon provides an `ArrayCol` class with methods to work on array based
columns. The methods are shown below:

```scala
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
// +----------------------+-------------------+--------------------------+---------------------------+------------------------+-----------------------+----------------------+----------------------+
// |framesPlayingTimeMsSum|framesPlayingTimeMs|framesPlayingTimeMsAllNull|framesPlayingTimeMsDistinct|framesPlayingTimeMsFirst|framesPlayingTimeMsLast|framesPlayingTimeMsMin|framesPlayingTimeMsMax|
// +----------------------+-------------------+--------------------------+---------------------------+------------------------+-----------------------+----------------------+----------------------+
// |1742812               |[1742812]          |false                     |[1742812]                  |1742812                 |1742812                |1742812               |1742812               |
// +----------------------+-------------------+--------------------------+---------------------------+------------------------+-----------------------+----------------------+----------------------+
//
```

The `ArrayCol` classess are thus far limited to the `lifeSwitch`, `joinSwitch`, and
`intvSwitch` containers only. It goes without saying that you can use any valid
string argument to the containers (i.e., something other than
`framesPlayingTimeMs`).


### Ad selection
Surgeon also provides several methods for selecting Ad related fields.


```scala
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
// +-----------+-------+-------+----------+---------+------------+-----------------+------------------------------+-----------+-------------+-------+-------+------------------+
// |sessionAdId|c3_csid|c3_isAd|c3_isAd_rc|c3_adTech|c3_adTech_rc|exitDuringPreRoll|AdContentMetadata             |adRequested|preRollStatus|hasSSAI|hasCSAI|preRollStartTimeMs|
// +-----------+-------+-------+----------+---------+------------+-----------------+------------------------------+-----------+-------------+-------+-------+------------------+
// |null       |null   |F      |false     |null     |unknown     |false            |{false, 1, false, false, null}|false      |1            |false  |false  |null              |
// +-----------+-------+-------+----------+---------+------------+-----------------+------------------------------+-----------+-------------+-------+-------+------------------+
//
```

### GeoInfo selection

You can select `GeoInfo` columns and their labels from `val.invariant.geoInfo` (of class `geoInfo`) like so:


```scala
dat.select(
  geoInfo("city"),      // Int: the city codes
  geoInfo("country")   // Int: the country codes
)
// res8: org.apache.spark.sql.package.DataFrame = [city: int, country: smallint]
```
To see the labels rather than the numeric codes, you can do:

```scala
dat.select(
  geoInfo("city").label,      // Int: the city codes
  geoInfo("country").label   // Int: the country codes
)
// res9: org.apache.spark.sql.package.DataFrame = [cityLabel: string, countryLabel: string]
```

> Compiled using version 0.1.7. 
