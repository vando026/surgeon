<p align="center">
<img src="./media/surgeon-283.png" alt="" width="200" >
</p>

<h1 align="center"> conviva-surgeon</h1>

A Scala library with that makes it easy to read and query parquet Session Summary (PbSS) and RawLog (PbRl) data. For example, Surgeon reduces this mess (taken from a sample notebook on Databricks):

```scala
val hourly_df = sqlContext.read.parquet("/mnt/conviva-prod-archive-pbss-hourly/pbss/hourly/
  st=0/y=2023/m=02/d=07/dt=2023_02_07_{16,17,18,19}/cust={1960180360}")
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

```scala
val path = Path.pbss("2023-02-07T{16-20}").cust(1960180360)
val hourly_df = spark.read.parquet(path.toList:_*)
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


### Installation 

1. Download the latest JAR from the 
[target](https://github.com/Conviva-Internal/conviva-surgeon/tree/main/surgeon/target/scala-2.12)
folder of this repo (`surgeon_2_12_0_0_*.jar`) and upload it directly to your local JAR folder or to Databricks. 
2. Find it on Databricks at `/FileStore/avandormael/surgeon/surgeon_2_12_0_0_*.jar`. 
3. You can either compile the JAR yourself by cloning this repo and running build.sbt. 
 
### Features

Below is a brief vignette of Surgeon's many features. Please see the 
[Wiki home page](https://github.com/Conviva-Internal/conviva-surgeon/wiki/0-Installation) for installation instructions and more detailed demos. 


```scala
// setup code
import org.apache.spark.sql.{SparkSession}
import conviva.surgeon.Paths._
import conviva.surgeon.PbSS._
PathDB.geoUtilPath = PathDB.testPath
val spark = SparkSession.builder.master("local[*]").getOrCreate
// spark: SparkSession = org.apache.spark.sql.SparkSession@273578b7
val path = Path.pbss("2023-02-07T02").cust(1960180360).toList(0)
// path: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}"
val dat = spark.read.parquet(path).filter(sessionId === 89057425)
// dat: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [key: struct<sessId: struct<customerId: int, clientId: array<struct<element:int>> ... 1 more field>, type: tinyint ... 1 more field>, val: struct<type: tinyint, sessSummary: struct<intvStartTimeSec: int, joinTimeMs: int ... 119 more fields> ... 14 more fields>]
```

### Quick column selection

Surgeon makes it easy to select columns that are frequently used in analysis:

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
  joinTimeMs
).show 
// +----------+--------------------+---------+-------------+--------+---------+----------+-----------+---------+----------+
// |customerId|            clientId|sessionId|shouldProcess|hasEnded|justEnded|justJoined|endedStatus|joinState|joinTimeMs|
// +----------+--------------------+---------+-------------+--------+---------+----------+-----------+---------+----------+
// |1960180360|[476230728, 12930...| 89057425|         true|   false|    false|      true|          0|        1|      4978|
// +----------+--------------------+---------+-------------+--------+---------+----------+-----------+---------+----------+
//
``` 

It is also easy to query from structs, maps or arrays in PbSS and PbRl:

```scala
dat.select(
  sessSum("playerState"), 
  d3SessSum("lifePausedTimeMs"),
  joinSwitch("playingTimeMs"),
  lifeSwitch("sessionTimeMs"),
  intvSwitch("networkBufferingTimeMs"), 
  invTags("sessionCreationTimeMs"), 
  sumTags("c3.video.isAd")
).show
// +-----------+----------------+-------------+-------------+----------------------+---------------------+-------------+
// |playerState|lifePausedTimeMs|playingTimeMs|sessionTimeMs|networkBufferingTimeMs|sessionCreationTimeMs|c3_video_isAd|
// +-----------+----------------+-------------+-------------+----------------------+---------------------+-------------+
// |          3|               0|          [0]|    [1906885]|                [2375]|        1675765692087|            F|
// +-----------+----------------+-------------+-------------+----------------------+---------------------+-------------+
//
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
to do with Surgeon. The same applies for `sid6`, which appends `sessionCreationTimeMs`:


```scala
dat.select(
  sid5.concat,   
  sid5.concatToHex, 
  sid5.concatToUnsigned,
  sid6.concat, 
  sid6.concatToHex, 
  sid6.concatToUnsigned, 
).show
// +--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
// |                sid5|             sid5Hex|        sid5Unsigned|                sid6|             sid6Hex|        sid6Unsigned|
// +--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
// |476230728:1293028...|1c62b448:4d120d00...|476230728:1293028...|476230728:1293028...|1c62b448:4d120d00...|476230728:1293028...|
// +--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
//
```

You can select the customer column using `customerId` and customer names using `customerName`.

```scala
dat.select(
  customerId,  // Int: The customer Id
  customerName // String: Pulls the customer names from GeoUtils/c3ServiceConfig*.csv
)
// res4: org.apache.spark.sql.package.DataFrame = [customerId: int, customerName: string]
```

See the [PbSS wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/2-PbSS-selecting-columns) and 
[PbRl wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/3-PbRl-selecting-columns) for more details about this functionality.

#### Querying time columns

Surgeon makes it easy to work with Unix epoch time columns. You can select
these columns using the short name, which come with a `stamp` method to format
values as HH:mm:ss, a `toSec` method to convert values from milliseconds to
seconds since Unix epoch, and a `toMs` method. 

```scala
import org.apache.spark.sql.functions._
dat.select(
  lifeFirstRecvTime,                 // its original form, milliseconds since unix epoch
  lifeFirstRecvTime.toSec,           // converted to seconds since unix epoch
  lifeFirstRecvTime.stamp,           // as a timestamp (HH:mm:ss)
  dayofweek(lifeFirstRecvTime.stamp).alias("dow"), // get the day of the week (Spark method)
  hour(lifeFirstRecvTime.stamp).alias("hour")      // get hour of the time (Spark method)
).show
// +-------------------+--------------------+----------------------+---+----+
// |lifeFirstRecvTimeMs|lifeFirstRecvTimeSec|lifeFirstRecvTimeStamp|dow|hour|
// +-------------------+--------------------+----------------------+---+----+
// |      1675765693115|          1675765693|   2023-02-07 02:28:13|  3|   2|
// +-------------------+--------------------+----------------------+---+----+
//
```

### PbSS Core Library metrics
Surgeon makes it easy to select columns that are constructed from the PbSS Core Library, which cannot be found in the PbSS data:

```scala
dat.select(
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

Surgeon makes constructing the paths to the data easier. 

```scala
Path.pbss("2023-02").toList(0) // monthly
// res6: String = "./surgeon/src/test/data/conviva-prod-archive-pbss-monthly/pbss/monthly/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00" // monthly
Path.pbss("2023-{2-5}").toList(0) // monthly
// res7: String = "./surgeon/src/test/data/conviva-prod-archive-pbss-monthly/pbss/monthly/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00" // monthly
Path.pbss("2023-02-07").toList(0) // daily
// res8: String = "./surgeon/src/test/data/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=02/dt=d2023_02_07_08_00_to_2023_02_08_08_00" // daily
Path.pbss("2023-02-{7,9,14}").toList(0) // daily
// res9: String = "./surgeon/src/test/data/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=02/dt=d2023_02_07_08_00_to_2023_02_08_08_00" // daily
Path.pbss("2023-02-07T09").toList(0) // hourly
// res10: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_09" // hourly
Path.pbss("2023-02-07T{8,9}").toList(0) // hourly
// res11: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_08"
```

Can't remember the 9-10 digit Id of the customer? Then use the name, like this:

```scala
Path.pbss("2023-02-07T02").cust("c3.TopServe").toList
// res12: List[String] = List(
//   "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}"
// )
// To select by more than one customer name 
Path.pbss("2023-02-07T02").cust("c3.TopServe", "c3.PlayFoot").toList
// res13: List[String] = List(
//   "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={*}"
// )
```

Only want to select any three customers for a given path, then do:

```scala
Path.pbss("2023-02-07T02").cust(3).toList
// res14: List[String] = List(
//   "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960002004,1960180360,1960181845}"
// )
```

See the [Paths wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/1-Paths-to-datasets) for more details about this functionality.


### Customer methods

Surgeon provides some convenient methods for working with Customer data. You
can use these methods to read in a file with customer Ids and names, get names
from Ids, and get Ids from names. 

```scala
import conviva.surgeon.Customer._
import conviva.surgeon.GeoInfo._
// Pulls the customer names from GeoUtils/c3ServiceConfig_30Jan2024.csv
c3IdToName(1960180360)
// res15: List[String] = List("c3.TopServe")
c3IdToName(List(207488736, 744085924))
// res16: List[String] = List("Key_missing", "Key_missing")
c3NameToId("c3.FappleTV")
// res17: List[Int] = List(1960184661)
c3NameToId(List("c3.FappleTV", "c3.SATY"))
// res18: List[Int] = List(1960184661, 1960003321)
```

See the [Customers wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/4-Customer-methods) for more details about this functionality.



<!-- Please see the wiki page for descriptions of surgeon's features. --> 

