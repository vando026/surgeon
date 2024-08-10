<p align="center">
<img src="./media/surgeon-283.png" alt="" width="200" >
</p>

<h1 align="center"> surgeon</h1>

A Scala library for Apache-Spark to query video streaming data. For example, Surgeon reduces this verbose SQL query


```scala
val path = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}"
// path: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}"
val sql = spark.read.parquet(path)
// sql: org.apache.spark.sql.package.DataFrame = [key: struct<sessId: struct<customerId: int, clientId: array<struct<element:int>> ... 1 more field>, type: tinyint ... 1 more field>, val: struct<type: tinyint, sessSummary: struct<intvStartTimeSec: int, joinTimeMs: int ... 119 more fields> ... 14 more fields>]
sql.createOrReplaceTempView("sql")
val sqlQuery = spark.sql(s"""
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
          , val.sessSummary.endedStatus endedStatus
          , val.sessSummary.shouldProcess
          , val.sessSummary.intvStartTimeSec as intvStartTimeSec
from sql
""")
// sqlQuery: org.apache.spark.sql.package.DataFrame = [customerId: int, clientId: array<struct<element:int>> ... 10 more fields]
```

to this query using the Scala API:


```scala
val path1 = pbss("2023-02-07T02").c3id(1960180360)
// path1: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}"
val df = spark.read.parquet(path1)
// df: org.apache.spark.sql.package.DataFrame = [key: struct<sessId: struct<customerId: int, clientId: array<struct<element:int>> ... 1 more field>, type: tinyint ... 1 more field>, val: struct<type: tinyint, sessSummary: struct<intvStartTimeSec: int, joinTimeMs: int ... 119 more fields> ... 14 more fields>]
df.select(
    customerId, 
    sessionId, 
    sid5.concatToHex, 
    intvStartTime.stamp,
    lifeFirstRecvTime.stamp, 
    sumTags("c3.viewer.id"),
    sumTags("c3.video.isAd"),
    lifeFirstRecvTime, 
    lifeFirstRecvTime, 
    endedStatus, 
    shouldProcess, 
    intvStartTime
  )
// res1: org.apache.spark.sql.package.DataFrame = [customerId: int, sessionId: int ... 10 more fields]
``` 


### Features

Below is a brief vignette of Surgeon's features to work with parquet raw log
(PbRl) and parquet session summary (PbSS) data. 

### Quick column selection

Surgeon makes it easy to select frequently used columns:

```scala
df.select(
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

It is also easy to query from structs, maps or arrays in raw event or session
data:

```scala
df.select(
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
[PbSS wiki](https://github.com/vando026/surgeon/blob/main/org-surgeon.wiki/target/mdoc/2-PbSS-selecting-columns.md)
and 
[PbRl wiki](https://github.com/vando026/surgeon/blob/main/org-surgeon.wiki/target/mdoc/3-PbRl-selecting-columns.md)
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
df.select(
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

```scala
df.select(
  customerId,  // Int: The customer Id
  customerName // String: Pulls the customer names from GeoUtils/c3ServiceConfig*.csv
).show(1, false)
// +----------+------------+
// |customerId|customerName|
// +----------+------------+
// |1960180360|c3.TopServe |
// +----------+------------+
// only showing top 1 row
//
```

See the 
[PbSS wiki](https://github.com/vando026/surgeon/blob/main/org-surgeon.wiki/target/mdoc/2-PbSS-selecting-columns.md)
and 
[PbRl wiki](https://github.com/vando026/surgeon/blob/main/org-surgeon.wiki/target/mdoc/3-PbRl-selecting-columns.md)
for more details about this functionality.



#### Querying time columns

Surgeon makes it easy to work with Unix epoch time columns. You can select
these columns using the short name, which come with a `stamp` method to format
values as HH:mm:ss, a `toSec` method to convert values from milliseconds to
seconds since Unix epoch, and a `toMs` method. 

```scala
df.select(
  lifeFirstRecvTime,                 // its original form, milliseconds since unix epoch
  lifeFirstRecvTime.toSec,           // converted to seconds since unix epoch
  lifeFirstRecvTime.stamp,           // as a timestamp (HH:mm:ss)
  dayofweek(lifeFirstRecvTime.stamp).alias("dow"), // get the day of the week (Spark method)
  hour(lifeFirstRecvTime.stamp).alias("hour")      // get hour of the time (Spark method)
).show(3, false)
// +-------------------+--------------------+----------------------+---+----+
// |lifeFirstRecvTimeMs|lifeFirstRecvTimeSec|lifeFirstRecvTimeStamp|dow|hour|
// +-------------------+--------------------+----------------------+---+----+
// |1675766260779      |1675766260          |2023-02-07 02:37:40   |3  |2   |
// |1675764621566      |1675764621          |2023-02-07 02:10:21   |3  |2   |
// |1675763609283      |1675763609          |2023-02-07 01:53:29   |3  |1   |
// +-------------------+--------------------+----------------------+---+----+
// only showing top 3 rows
//
```

#### GeoInfo class 

Surgeon makes it easy to work with the `geoInfo` struct.  You can select `geoInfo`
columns like so:

```scala
df.select(
  geoInfo("city"),        // Int: the city codes
  geoInfo("country"),     // Int: the country codes
  geoInfo("continent")    // Int: the continent codes
).orderBy("city").show(3, false)
// +-----+-------+---------+
// |city |country|continent|
// +-----+-------+---------+
// |0    |165    |3        |
// |49218|165    |3        |
// |59747|165    |3        |
// +-----+-------+---------+
// only showing top 3 rows
//
```

It is hard to decipher what these codes mean, so Surgeon makes it easy by
providing a `label` method to map the codes to names: 



```scala
df.select(
  geoInfo("city"),            // Int: the city codes
  geoInfo("city").label,      // String: the city names
  geoInfo("country"),         // Int: the country codes
  geoInfo("country").label,   // String: the country names
  geoInfo("continent"),       // Int: the continent codes
  geoInfo("continent").label  // String: the continent names
).orderBy("city").show(3, false)
// +-----+------------+-------+-------------+---------+--------------+
// |city |cityLabel   |country|countryLabel |continent|continentLabel|
// +-----+------------+-------+-------------+---------+--------------+
// |0    |Unknown     |165    |united states|3        |north america |
// |49218|Boston      |165    |united states|3        |north america |
// |59747|West Chester|165    |united states|3        |north america |
// +-----+------------+-------+-------------+---------+--------------+
// only showing top 3 rows
//
```

### Path construction

Surgeon makes constructing the paths to the data easier. 


```scala
// monthly
pbss("2023-02")
// res10: Builder = /mnt/org-prod-archive-pbss-monthly/pbss/monthly/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00
pbss("2023-{2-5}")
// res11: Builder = /mnt/org-prod-archive-pbss-monthly/pbss/monthly/y=2023/m={02,03,04,05}/dt=c2023_{02,03,04,05}_01_08_00_to_2023_{03,04,05,06}_01_08_00

// daily
pbss("2023-02-07")
// res12: Builder = /mnt/org-prod-archive-pbss-daily/pbss/daily/y=2023/m=02/dt=d2023_02_07_08_00_to_2023_02_08_08_00
pbss("2023-02-{7,9,14}")
// res13: Builder = /mnt/org-prod-archive-pbss-daily/pbss/daily/y=2023/m=02/dt=d2023_02_{07,09,14}_08_00_to_2023_02_{08,10,15}_08_00

// hourly
pbss("2023-02-07T09")
// res14: Builder = /mnt/org-prod-archive-pbss-hourly/pbss/hourly/st=0/y=2023/m=02/d=07/dt=2023_02_07_09
pbss("2023-02-07T{8,9}")
// res15: Builder = /mnt/org-prod-archive-pbss-hourly/pbss/hourly/st=0/y=2023/m=02/d=07/dt=2023_02_07_{08,09}
```


Can't remember the 9-10 digit Id of the customer? Then use the name, like this:

```scala
// demonstrate using paths to Surgeon test data
pbss("2023-02-07T02").c3name("c3.TopServe")
// res17: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}"
``` 

To select by more than one customer name 

```scala
// demonstrate using paths to Surgeon test data
pbss("2023-02-07T02").c3name("c3.TopServe", "c3.PlayFoot")
// res18: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360,1960002004}"
```

Only want to select any three customers for a given path, then do:

```scala
// demonstrate using paths to Surgeon test data
pbss("2023-02-07T02").c3take(2)
// res19: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960002004,1960180360}"
```

See the [Paths wiki](https://github.com/vando026/surgeon/blob/main/org-surgeon.wiki/target/mdoc/1-Paths-to-datasets.md) for more details about this functionality.


### Customer methods

Surgeon provides some convenient methods for working with Customer data. You
can use these methods to read in a file with customer Ids and names, get names
from Ids, and get Ids from names. 


```scala
// Pulls the customer names from GeoUtils/c3ServiceConfig_30Jan2024.csv
c3.idToName(1960180360)
// res20: Seq[String] = ArrayBuffer("c3.TopServe")
c3.idToName(1960184661, 1960003321)
// res21: Seq[String] = ArrayBuffer("c3.FappleTV", "c3.SATY")
c3.nameToId("c3.FappleTV")
// res22: Seq[Int] = ArrayBuffer(1960184661)
c3.nameToId("c3.FappleTV", "c3.SATY")
// res23: Seq[Int] = ArrayBuffer(1960184661, 1960003321)
```

See the [Customers wiki](https://github.com/vando026/surgeon/blob/main/org-surgeon.wiki/target/mdoc/4-Customer-methods.md) for more details about this functionality.



<!-- Please see the wiki page for descriptions of surgeon's features. --> 

