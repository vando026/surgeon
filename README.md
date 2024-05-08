<p align="center">
<img src="./media/surgeon-283.png" alt="" width="200" >
</p>

<h1 align="center"> conviva-surgeon</h1>

A Scala library with that makes it easy to read and query parquet Session Summary (PbSS) and RawLog (PbRl) data. For example, Surgeon reduces this mess (taken from a sample notebook on Databricks):

```scala 
val hourly_df = sqlContext.read.parquet("/mnt/conviva-prod-archive-pbss-hourly/pbss/hourly/
  st=0/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}")
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
import org.apache.spark.sql.{SparkSession, Column}
import org.apache.spark.sql.functions._
val spark = SparkSession.builder.master("local[*]").getOrCreate
// spark: SparkSession = org.apache.spark.sql.SparkSession@5ebbf219
import conviva.surgeon.GeoInfo._
import conviva.surgeon.Paths._
import conviva.surgeon.PbSS._
```

```scala
def pbss(date: String) = SurgeonPath(TestPbSS()).make(date)
def customerName() = CustomerName(TestPbSS().geoUtilPath).make(customerId)
```

```scala
val path = pbss("2023-02-07T02").c3id(1960180360)
// path: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}"
val hourly_df = spark.read.parquet(path)
// hourly_df: org.apache.spark.sql.package.DataFrame = [key: struct<sessId: struct<customerId: int, clientId: array<struct<element:int>> ... 1 more field>, type: tinyint ... 1 more field>, val: struct<type: tinyint, sessSummary: struct<intvStartTimeSec: int, joinTimeMs: int ... 119 more fields> ... 14 more fields>]
hourly_df.select(
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
  ).show(3, false)
// +----------+----------+--------------------------------------------+-------------------+----------------------+------------+-------------+-------------------+--------+----------+-----------------+-------------------+-----------+-------------+----------------+
// |customerId|sessionId |sid5Hex                                     |intvStartTimeStamp |lifeFirstRecvTimeStamp|c3_viewer_id|c3_video_isAd|lifeFirstRecvTimeMs|hasEnded|justJoined|lifePlayingTimeMs|lifeFirstRecvTimeMs|endedStatus|shouldProcess|intvStartTimeSec|
// +----------+----------+--------------------------------------------+-------------------+----------------------+------------+-------------+-------------------+--------+----------+-----------------+-------------------+-----------+-------------+----------------+
// |1960180360|1567276763|28ae4823:45055e0d:1c62c137:ac58c6fa:5d6abedb|2023-02-07 02:00:00|2023-02-07 02:37:40   |null        |F            |1675766260779      |true    |false     |0                |1675766260779      |2          |false        |1675764000      |
// |1960180360|1300229737|30d612b6:ef71f10d:6258098f:52670453:4d7fee69|2023-02-07 02:00:00|2023-02-07 02:10:21   |8201852     |F            |1675764621566      |true    |true      |607992           |1675764621566      |1          |true         |1675764000      |
// |1960180360|1780527017|6f13d44:93e04b25:e2261399:943b801:6a20afa9  |2023-02-07 02:00:00|2023-02-07 01:53:29   |9435756     |F            |1675763609283      |true    |false     |409205           |1675763609283      |2          |true         |1675764000      |
// +----------+----------+--------------------------------------------+-------------------+----------------------+------------+-------------+-------------------+--------+----------+-----------------+-------------------+-----------+-------------+----------------+
// only showing top 3 rows
//
``` 

### Installation 

1. Download the latest JAR from the 
[target](https://github.com/Conviva-Internal/conviva-surgeon/tree/main/surgeon/target/scala-2.12)
folder of this repo (`surgeon_2_12_0_1_*.jar`) and upload it directly to your local JAR folder or to Databricks. 
2. Find it on Databricks at `/FileStore/avandormael/surgeon/surgeon_2_12_0_1_*.jar`. 
3. You can either compile the JAR yourself by cloning this repo and running build.sbt. 
 
### Features

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
).show(3) 
// +----------+--------------------+----------+-------------+--------+---------+----------+-----------+---------+----------+
// |customerId|            clientId| sessionId|shouldProcess|hasEnded|justEnded|justJoined|endedStatus|joinState|joinTimeMs|
// +----------+--------------------+----------+-------------+--------+---------+----------+-----------+---------+----------+
// |1960180360|[682510371, 11579...|1567276763|        false|    true|     true|     false|          2|       -1|        -1|
// |1960180360|[819335862, -2777...|1300229737|         true|    true|     true|      true|          1|        1|      1524|
// |1960180360|[116473156, -1814...|1780527017|         true|    true|     true|     false|          2|        1|      1649|
// +----------+--------------------+----------+-------------+--------+---------+----------+-----------+---------+----------+
// only showing top 3 rows
//
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
  sumTags("c3.video.isAd")
).show(3)
// +-----------+----------------+-------------+-------------+----------------------+---------------------+-------------+
// |playerState|lifePausedTimeMs|playingTimeMs|sessionTimeMs|networkBufferingTimeMs|sessionCreationTimeMs|c3_video_isAd|
// +-----------+----------------+-------------+-------------+----------------------+---------------------+-------------+
// |        100|               0|          [0]|        [131]|                   [0]|        1675766259285|            F|
// |        100|               0|          [0]|     [620651]|                   [0]|        1675764620679|            F|
// |        100|               0|          [0]|     [516633]|                   [0]|        1675763608616|            F|
// +-----------+----------------+-------------+-------------+----------------------+---------------------+-------------+
// only showing top 3 rows
//
```

These `Column` classes come with their own methods. See for example `geoInfo` below; the `lifeSwitch`, `joinSwitch`, and `intvSwitch` are array columns which have several methods such as `first`, `last`, and
`distinct`, to name a few. See the 
[PbSS wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/2-PbSS-selecting-columns)
and 
[PbRl wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/3-PbRl-selecting-columns)
for more details about this functionality.

You can also use/mixin standard Scala API code with Surgeon syntax to select a column:

```scala
hourly_df.select(
  sessionId,
  col("val.sessSummary.intvFirstBufferLengthMs"),
  sessionState
).show(3, false)
// +----------+-----------------------+------------+
// |sessionId |intvFirstBufferLengthMs|sessionState|
// +----------+-----------------------+------------+
// |1567276763|-1                     |1           |
// |1300229737|0                      |1           |
// |1780527017|50865                  |1           |
// +----------+-----------------------+------------+
// only showing top 3 rows
//
```

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
hourly_df.select(
  sid5.concat,   
  sid5.concatToHex, 
  sid5.concatToUnsigned,
  sid6.concat, 
  sid6.concatToHex, 
  sid6.concatToUnsigned, 
).show(3, false)
// +-----------------------------------------------------+--------------------------------------------+-----------------------------------------------------+-------------------------------------------------------------------+-----------------------------------------------------+---------------------------------------------------------------+
// |sid5                                                 |sid5Hex                                     |sid5Unsigned                                         |sid6                                                               |sid6Hex                                              |sid6Unsigned                                                   |
// +-----------------------------------------------------+--------------------------------------------+-----------------------------------------------------+-------------------------------------------------------------------+-----------------------------------------------------+---------------------------------------------------------------+
// |682510371:1157979661:476234039:-1403468038:1567276763|28ae4823:45055e0d:1c62c137:ac58c6fa:5d6abedb|682510371:1157979661:476234039:2891499258:1567276763 |682510371:1157979661:476234039:-1403468038:1567276763:1675766259285|28ae4823:45055e0d:1c62c137:ac58c6fa:5d6abedb:2b73de55|682510371:1157979661:476234039:2891499258:1567276763:729013845 |
// |819335862:-277745395:1649936783:1382483027:1300229737|30d612b6:ef71f10d:6258098f:52670453:4d7fee69|819335862:4017221901:1649936783:1382483027:1300229737|819335862:-277745395:1649936783:1382483027:1300229737:1675764620679|30d612b6:ef71f10d:6258098f:52670453:4d7fee69:2b5add87|819335862:4017221901:1649936783:1382483027:1300229737:727375239|
// |116473156:-1814017243:-500821095:155432961:1780527017|6f13d44:93e04b25:e2261399:943b801:6a20afa9  |116473156:2480950053:3794146201:155432961:1780527017 |116473156:-1814017243:-500821095:155432961:1780527017:1675763608616|6f13d44:93e04b25:e2261399:943b801:6a20afa9:2b4b6c28  |116473156:2480950053:3794146201:155432961:1780527017:726363176 |
// +-----------------------------------------------------+--------------------------------------------+-----------------------------------------------------+-------------------------------------------------------------------+-----------------------------------------------------+---------------------------------------------------------------+
// only showing top 3 rows
//
```

You can select the customer column using `customerId` and customer names using `customerName`.

```scala doc
hourly_df.select(
  customerId,  // Int: The customer Id
  customerName // String: Pulls the customer names from GeoUtils/c3ServiceConfig*.csv
).show(1, false)
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
).show(3, false)
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
).show(3, false)
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
).show(3, false)
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
).show(false)
```

### Path construction

Surgeon makes constructing the paths to the data easier. 
The production paths on Databricks are shown below. 

```scala:reset:invisible 
import org.apache.spark.sql.{SparkSession, Column}
import org.apache.spark.sql.functions._
val spark = SparkSession.builder.master("local[*]").getOrCreate
import conviva.surgeon.Customer._
import conviva.surgeon.GeoInfo._
import conviva.surgeon.Paths._
import conviva.surgeon.PbSS._
def pbss(date: String) = SurgeonPath(ProdPbSS()).make(date)
```

```scala
// monthly
pbss("2023-02")
// res5: Builder = ./surgeon/src/test/data//y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00
pbss("2023-{2-5}")
// res6: Builder = ./surgeon/src/test/data//y=2023/m={02,03,04,05}/dt=c2023_{02,03,04,05}_01_08_00_to_2023_{03,04,05,06}_01_08_00

// daily
pbss("2023-02-07")
// res7: Builder = ./surgeon/src/test/data//y=2023/m=02/dt=d2023_02_07_08_00_to_2023_02_08_08_00
pbss("2023-02-{7,9,14}")
// res8: Builder = ./surgeon/src/test/data//y=2023/m=02/dt=d2023_02_{07,09,14}_08_00_to_2023_02_{08,10,15}_08_00

// hourly
pbss("2023-02-07T09")
// res9: Builder = ./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_09
pbss("2023-02-07T{8,9}")
// res10: Builder = ./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_{08,09}
```

```scala:reset:invisible 
import org.apache.spark.sql.{SparkSession, Column}
import org.apache.spark.sql.functions._
val spark = SparkSession.builder.master("local[*]").getOrCreate
import conviva.surgeon.Customer._
import conviva.surgeon.GeoInfo._
import conviva.surgeon.Paths._
import conviva.surgeon.PbSS._
def pbss(date: String) = SurgeonPath(TestPbSS()).make(date)
```

Can't remember the 9-10 digit Id of the customer? Then use the name, like this:

```scala
// demonstrate using paths to Surgeon test data
pbss("2023-02-07T02").c3name("c3.TopServe")
// res11: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}"
``` 

// To select by more than one customer name 
```scala
// demonstrate using paths to Surgeon test data
pbss("2023-02-07T02").c3name("c3.TopServe", "c3.PlayFoot")
// res12: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360,1960002004}"
```

Only want to select any three customers for a given path, then do:

```scala
// demonstrate using paths to Surgeon test data
pbss("2023-02-07T02").c3take(2)
// res13: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960002004,1960180360}"
```

See the [Paths wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/1-Paths-to-datasets) for more details about this functionality.


### Customer methods

Surgeon provides some convenient methods for working with Customer data. You
can use these methods to read in a file with customer Ids and names, get names
from Ids, and get Ids from names. 


```scala
// Pulls the customer names from GeoUtils/c3ServiceConfig_30Jan2024.csv
c3.idToName(1960180360)
// res14: Seq[String] = ArrayBuffer("c3.TopServe")
c3.idToName(1960184661, 1960003321)
// res15: Seq[String] = ArrayBuffer("c3.FappleTV", "c3.SATY")
c3.nameToId("c3.FappleTV")
// res16: Seq[Int] = ArrayBuffer(1960184661)
c3.nameToId("c3.FappleTV", "c3.SATY")
// res17: Seq[Int] = ArrayBuffer(1960184661, 1960003321)
```

See the [Customers wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/4-Customer-methods) for more details about this functionality.



<!-- Please see the wiki page for descriptions of surgeon's features. --> 

