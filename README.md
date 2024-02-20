<p align="center">
<img src="./media/surgeon-283.png" alt="" width="200" >
</p>

<h1 align="center"> conviva-surgeon</h1>

A Scala library with that makes it easy to read and query parquet Session Summary (PbSS) and RawLog (PbRl) data. For example, Surgeon reduces this mess (taken from a sample notebook on Databricks):

```scala
val hourly_df = sqlContext.read.parquet("/mnt/conviva-prod-archive-pbss-hourly/pbss/hourly/
  st=0/y=2022/m=12/d=25/dt=2022_12_25_{16,17,18,19}/cust={1960184999}")
hourly_df.createOrReplaceTempView("hourly_df")

val sessionSummary_simplified = sqlContext.sql(s"""
select key.sessId.customerId customerId
      , key.sessId.clientId clientId
      , key.sessId.clientSessionId sessionId
      , printf("%x:%x:%x:%x:%x",
           key.sessId.clientId[0].element, key.sessId.clientId[1].element,
           key.sessId.clientId[2].element, key.sessId.clientId[3].element,
           key.sessId.clientSessionId) sid5
      , from_unixtime(val.sessSummary.intvStartTimeSec, "yyyy-MM-dd_HH") date_hr
      , from_unixtime(val.sessSummary.lifeFirstRecvTimeMs/1000, "yyyy-MM-dd HH:mm:ss") startTimeUnix
      , val.invariant.c3Tags["c3.viewer.id"] viewerId
      , val.invariant.c3Tags["c3.video.isAd"] videoIsAd
      , val.sessSummary.lifeFirstRecvTimeMs startTime 
      , hasEnded(val.sessSummary) ended
      , justJoined(val.sessSummary) justJoined
      , lifePlayingTimeMs(val.sessSummary) lifePlayingTimeMs
      , val.sessSummary.endedStatus endedStatus
      , val.sessSummary.shouldProcess
      , val.sessSummary.intvStartTimeSec as intvStartTimeSec
from hourly_df
""")
sessionSummary_simplified.createOrReplaceTempView("sessionSummary_simplified")
```

to this:

``` scala
val path = Cust(PbSS.hourly(2022, 12, 24, List.range(16, 20)), ids = 1960184999)
val hourly_df = spark.read.parquet(path)
  .select(
    customerId, 
    sessionId, 
    sid5.concatToHex, 
    intvStartTime.stamp,
    lifeFirstRecvTime.stamp, 
    sumTags("c3.viewer.id"),
    sumTags("c3.video.isAd"),
    lifeFirstRecvTime, 
    hasEnded, 
    justJoined, 
    lifePlayingTimeMs,
    lifeFirstRecvTime, 
    endedStatus, 
    shouldProcess, 
    intvStartTime
  )
``` 

```scala 

// +----------+---------+-------------------------------------------+-------------------+----------------------+------------+-------------+-------------------+--------+----------+-----------------+-------------------+-----------+-------------+----------------+
// |customerId|sessionId|sid5Hex                                    |intvStartTimeStamp |lifeFirstRecvTimeStamp|c3_viewer_id|c3_video_isAd|lifeFirstRecvTimeMs|hasEnded|justJoined|lifePlayingTimeMs|lifeFirstRecvTimeMs|endedStatus|shouldProcess|intvStartTimeSec|
// +----------+---------+-------------------------------------------+-------------------+----------------------+------------+-------------+-------------------+--------+----------+-----------------+-------------------+-----------+-------------+----------------+
// |1960180360|89057425 |1c62b448:4d120d00:a613f8d2:b9a1e9b4:54ee891|2023-02-07 02:00:00|2023-02-07 02:28:13   |2640043     |F            |1675765693115      |false   |true      |1742812          |1675765693115      |0          |true         |1675764000      |
// +----------+---------+-------------------------------------------+-------------------+----------------------+------------+-------------+-------------------+--------+----------+-----------------+-------------------+-----------+-------------+----------------+
```

Below is a brief vignette of Surgeon's many features. Please see the 
[Wiki home page](https://github.com/Conviva-Internal/conviva-surgeon/wiki/0-Installation) for installation instructions and more detailed demos. 

### Quick column selection

Surgeon makes it easy to select columns that are frequently used in analysis:

```scala 
hourly_df.select(
  customerId, 
  clientId,
  sessionId,
  shouldProcess,
  hasEnded,
  justEnded,
  justJoined,
  endedStatus,
  joinState, 
  joinTimeMs
) 

// +----------+-------------------------------------------------+---------+-------------+--------+---------+----------+-----------+---------+----------+
// |customerId|clientId                                         |sessionId|shouldProcess|hasEnded|justEnded|justJoined|endedStatus|joinState|joinTimeMs|
// +----------+-------------------------------------------------+---------+-------------+--------+---------+----------+-----------+---------+----------+
// |1960180360|[476230728, 1293028608, -1508640558, -1180571212]|89057425 |true         |false   |false    |true      |0          |1        |4978      |
// +----------+-------------------------------------------------+---------+-------------+--------+---------+----------+-----------+---------+----------+

``` 

It is also easy to query from structs, maps or arrays in PbSS and PbRl:

```scala
hourly_df.select(
  sessSum("playerState"), 
  d3SessSum("lifePausedTimeMs"),
  joinSwitch("playingTimeMs"),
  lifeSwitch("sessionTimeMs"),
  intvSwitch("networkBufferingTimeMs"), 
  invTags("sessionCreationTimeMs"), 
  sumTags("c3.video.isAd"), 
  geoInfo("city")
)
```

These `Column` classes come with their own methods. See for example `geoInfo` below; the `lifeSwitch`, `joinSwitch`, and `intvSwitch` are array columns which have several methods such as `first`, `last`, and
`distinct`, to name a few. See the 
[PbSS wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/2-PbSS-selecting-columns)
and 
[PbRl wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/3-PbRl-selecting-columns)
for more details about this functionality.

### ID selction, formatting and conversion

Surgeon makes it easier to work with ID columns. For example, the `clientId`
column is an array of 4 values, which can be easily concatenated into a single
string using the `concat` method. Some fields like `clientId`,
`clientSessionId`, `publicIpv6` are  inconsistently formatted as unsigned,
signed, or hexadecimal across the PbSS and PbRl datasets. Surgeon
makes it easy to both concat and format these values as is (`concat`), as unsigned
(`concatToUnsigned`), or as hexadecimal (`concatToHex`). Further,  a `sid5` column
is often constructed from the `clientId` and `sessionId` columns, which is easy
to do with Surgeon.


```scala 
hourly_df.select(
  sid5.concat,   
  sid5.concatToHex, 
  sid5.concatToUnsigned 
)

// +-----------------------------------------------------+-------------------------------------------+---------------------------------------------------+
// |sid5                                                 |sid5Hex                                    |sid5Unsigned                                       |
// +-----------------------------------------------------+-------------------------------------------+---------------------------------------------------+
// |476230728:1293028608:-1508640558:-1180571212:89057425|1c62b448:4d120d00:a613f8d2:b9a1e9b4:54ee891|476230728:1293028608:2786326738:3114396084:89057425|
// +-----------------------------------------------------+-------------------------------------------+---------------------------------------------------+

```

The same applies for `sid6`, which appends `sessionCreationTimeMs`:

```scala 
hourly_df.select(
  sid6.concat, 
  sid6.concatToHex, 
  sid6.concatToUnsigned, 
)

// +-------------------------------------------------------------------+----------------------------------------------------+-------------------------------------------------------------+
// |sid6                                                               |sid6Hex                                             |sid6Unsigned                                                 |
// +-------------------------------------------------------------------+----------------------------------------------------+-------------------------------------------------------------+
// |476230728:1293028608:-1508640558:-1180571212:89057425:1675765692087|1c62b448:4d120d00:a613f8d2:b9a1e9b4:54ee891:2b6b36b7|476230728:1293028608:2786326738:3114396084:89057425:728446647|
// +-------------------------------------------------------------------+----------------------------------------------------+-------------------------------------------------------------+

```

You can select the customer column using `customerId` and customer names using `customerName`.

```scala 
hourly_df.select(
  customerId,  // Int: The customer Id
  customerName // String: Pulls the customer names from GeoUtils/c3ServiceConfig.xml
)
```

See the [PbSS wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/2-PbSS-selecting-columns) and 
[PbRl wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/3-PbRl-selecting-columns) for more details about this functionality.

#### Querying time columns

Surgeon makes it easy to work with Unix epoch time columns. You can select
these columns using the short name, which come with a `stamp` method to format
values as HH:mm:ss, a `toSec` method to convert values from milliseconds to
seconds since Unix epoch, and a `toMs` method. 

```scala 
hourly_df.select(
  lifeFirstRecvTime,                 // its original form, milliseconds since unix epoch
  lifeFirstRecvTime.toSec,           // converted to seconds since unix epoch
  lifeFirstRecvTime.stamp,           // as a timestamp (HH:mm:ss)
  dayofweek(lifeFirstRecvTime.stamp).alias("dow"), // get the day of the week (Spark method)
  hour(lifeFirstRecvTime.stamp).alias("hour")      // get hour of the time (Spark method)
)

// +-------------------+--------------------+----------------------+---+----+
// |lifeFirstRecvTimeMs|lifeFirstRecvTimeSec|lifeFirstRecvTimeStamp|dow|hour|
// +-------------------+--------------------+----------------------+---+----+
// |1675765693115      |1675765693          |2023-02-07 02:28:13   |3  |2   |
// +-------------------+--------------------+----------------------+---+----+
```

### PbSS Core Library metrics
Surgeon makes it easy to select columns that are constructed from the PbSS Core Library, which cannot be found in the PbSS data:

```scala
hourly_df.select(
  isAttempt,
  hasJoined,
  isVSF, 
  isVSFT,
  isVPF, 
  isVPFT,
  isEBVS,
  lifeAvgBitrateKbps,
  firstHbTimeMs,
  isSessDoneNotJoined,
  isSessJustJoined,
  isJoinTimeAccurate,
  justJoinedAndLifeJoinTimeMsIsAccurate,
  intvAvgBitrateKbps,
  intvBufferingTimeMs, 
  intvPlayingTimeMs
).show
// +---------+---------+-----+------+-----+------+------+------------------+-----------------+-------------------+----------------+------------------+-------------------------------------+------------------+-------------------+-----------------+
// |isAttempt|hasJoined|isVSF|isVSFT|isVPF|isVPFT|isEBVS|lifeAvgBitrateKbps|    firstHbTimeMs|isSessDoneNotJoined|isSessJustJoined|isJoinTimeAccurate|justJoinedAndLifeJoinTimeMsIsAccurate|intvAvgBitrateKbps|intvBufferingTimeMs|intvPlayingTimeMs|
// +---------+---------+-----+------+-----+------+------+------------------+-----------------+-------------------+----------------+------------------+-------------------------------------+------------------+-------------------+-----------------+
// |     true|     true|false| false|false| false| false|            5807.0|1.675765693115E12|              false|            true|              true|                                 true|            5806.0|             2375.0|        1742812.0|
// +---------+---------+-----+------+-----+------+------+------------------+-----------------+-------------------+----------------+------------------+-------------------------------------+------------------+-------------------+-----------------+
//
```

Previously, to achieve this functionality, one had to run a chain of Notebooks
on Databricks, which took long and produced much verbose output. 

#### GeoInfo class 

Surgeon makes it easy to work with the `geoInfo` struct.  You can select `geoInfo`
columns like so:

```scala 
hourly_df.select(
  geoInfo("city"),        // Int: the city codes
  geoInfo("country"),     // Int: the country codes
  geoInfo("continent")    // Int: the continent codes
)
// +------+-------+---------+
// |  city|country|continent|
// +------+-------+---------+
// | 12141|    229|        4|
// |107527|    229|        4|
// | 21233|    229|        4|
// | 94082|    229|        4|
// |  4773|    229|        4|
// +------+-------+---------+
```

It is hard to decipher what these codes mean, so Surgeon makes it easy by
providing a `label` method to map the codes to names: 


```scala 
hourly_df.select(
  geoInfo("city"),            // Int: the city codes
  geoInfo("city").label,      // String: the city names
  geoInfo("country"),         // Int: the country codes
  geoInfo("country").label,   // String: the country names
  geoInfo("continent"),       // Int: the continent codes
  geoInfo("continent").label  // String: the continent names
)
// +------+------------+-------+-------------+---------+--------------+
// |  city|   cityLabel|country| countryLabel|continent|continentLabel|
// +------+------------+-------+-------------+---------+--------------+
// | 12141|      Boston|    229|united states|        4| north america|
// |107527|West Chester|    229|united states|        4| north america|
// | 21233|    Columbus|    229|united states|        4| north america|
// | 94082|    Spanaway|    229|united states|        4| north america|
// |  4773|     Ashburn|    229|united states|        4| north america|
// +------+------------+-------+-------------+---------+--------------+
```

### Path construction

Surgeon makes constructing the paths to the data easier. Customers come with
Hourly, Daily or Monthly data. 

```scala 
import conviva.surgeon.Paths._
val path1 = PbSS.hourly(year = 2023, month = 12, days = 24, hours = 18)
val path2 = PbSS.hourly(12, 24, List(18, 19, 20))
val path3 = Cust(PbSS.hourly(12, 24, 18), ids = 1960180360)

// path1: conviva.surgeon.Paths.DataPath = /mnt/conviva-prod-archive-pbss-hourly/pbss/hourly/st=0/y=2023/m=12/d=24/dt=2023_12_24_18
// path2: conviva.surgeon.Paths.DataPath = /mnt/conviva-prod-archive-pbss-hourly/pbss/hourly/st=0/y=2024/m=12/d=24/dt=2024_12_24_{18,19,20}
// path3: String = /mnt/conviva-prod-archive-pbss-hourly/pbss/hourly/st=0/y=2024/m=12/d=24/dt=2024_12_24_18/cust={1960180360}

```

Can't remember the 9-10 digit Id of the customer? Then use the name, like this:

```scala 
val path = Cust(PbSS.hourly(month = 12, days = 24, hours = 18), names = "c3.RicksTV")
// path: String = /mnt/conviva-prod-archive-pbss-hourly/pbss/hourly/st=0/y=2024/m=12/d=24/dt=2024_12_24_18/cust={1960180000}
```

Only want to select any three customers for a given path, then do:

```scala 
val path = Cust(PbSS.hourly(2022, 12, 24, 18)), take = 3)
// path: String = /mnt/conviva-prod-archive-pbss-hourly/pbss/hourly/st=0/y=2024/m=12/d=24/dt=2024_12_24_18/cust={1960180001,1960180360,1960184999}
```

See the [Paths wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/1-Paths-to-datasets) for more details about this functionality.


### Customer methods

Surgeon provides some convenient methods for working with Customer data. You
can use these methods to read in a file with customer Ids and names, get names
from Ids, and get Ids from names. 

```scala  
import conviva.surgeon.Customer._
// Pulls the customer names from GeoUtils/c3ServiceConfig_30Jan2024.csv
customerNames() 
// res1: Map[Int,String] = Map(207488736 -> c3.MSNBC, 744085924 -> c3.PMNN, 1960180360 -> c3.TV2, 978960980 -> c3.BASC)
customerIdToName(207488736)
// res2: List[String] = List("c3.MSNBC")
customerIdToName(List(207488736, 744085924))
// res3: List[String] = List("c3.MSNBC", "c3.PMNN")
customerNameToId("TV2")
// res4: List[Int] = List(1960180360)
customerNameToId(List("c3.TV2", "c3.BASC"))
// res5: List[Int] = List(1960180360, 978960980)
```

See the [Customers wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/4-Customer-methods) for more details about this functionality.



<!-- Please see the wiki page for descriptions of surgeon's features. --> 

