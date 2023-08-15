<p align="center">
<img src="./media/surgeon-283.png" alt="" width="200" >
</p>

<h1 align="center"> conviva-surgeon</h1>

A Scala library with tools to operate on parquet Session Summary (PbSS) and RawLog (PbRl) data. Surgeon is designed to reduce the verbose startup code needed to read the data and simplifies working with columns and column arrays. For example, surgeon reduces this mess (taken from a sample notebook on Databricks):

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
val path = Cust(Hourly(2022, 12, 24, List.range(16, 20)), ids = 1960184999)
val hourly_df = spark.read.parquet(path)
  .select(
    customerId, 
    sessionId, 
    sid5.hex, 
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

Below is a brief vignette of many of Surgeon's features. Please see the 
[Wiki home page](https://github.com/Conviva-Internal/conviva-surgeon/wiki/0-Installation) for installation instructions and more detailed demos. 

### Short hand columns

The are several short hand names that make the selection of frequently used columns as simple as
possible: 

```scala mdoc
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
``` 
and many more. See the [PbSS wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/2-PbSS-selecting-columns) and 
[PbRl wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/3-PbRl-selecting-columns) for the full list. 

### Containers

Surgeon makes it easier to select columns using containers. 

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

These containers may come with their own methods. See for example `geoInfo` below; the `lifeSwitch`, `joinSwitch`, and `intvSwitch` are array columns which have several methods such as `first`, `last`, and
`distinct`, to name a few. See the 
[PbSS wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/2-PbSS-selecting-columns)
and 
[PbRl wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/3-PbRl-selecting-columns)
for more details about this functionality.

### Column classes and methods

Surgeon provides some methods for the short hand named columns, which removes
verbose and repetitive code. 

#### ID class

Surgeon makes it easier to work with ID columns. Often, a `sid5` column is
constructed from the `clientId` and `sessionId` columns. Both columns are
inconsistently formatted across PbSS and PbRl datasets. Surgeon constructs a
`sid5` column for you with methods to format the values as is (`asis`), as
unsigned (`nosign`), or as hexadecimal (`hex`).

```scala 
hourly_df.select(
  sid5.asis,   
  sid5.hex, 
  sid5.nosign 
)
```

The same methods can be used with `clientId` or `sessionId` individually, and
you can even construct an `sid6` column with `sessionCreationTimeMs`:

```scala 
hourly_df.select(
  sid6.asis, 
  sid6.hex, 
  sid6.nosign, 
)
```

You can select the customer columns using `customerId` and customer names using `customerName`.

```scala 
hourly_df.select(
  customerId,  // Int: The customer Id
  customerName // String: Pulls the customer names from GeoUtils/c3ServiceConfig.xml
)
```

See the [PbSS wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/2-PbSS-selecting-columns) and 
[PbRl wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/3-PbRl-selecting-columns) for more details about this functionality.

#### Time class

Surgeon makes it easy to work with Unix epoch time columns. You can select
these columns using the short name, which come with a `stamp` method to format
values as HH:mm:ss, a `toSec` method to convert values from milliseconds to
seconds since Unix epoch, and a `toMs` method. 

```scala 
hourly_df.select(
  lifeFirstRecvTime,                 // its original form, milliseconds since unix epoch
  lifeFirstRecvTime.toSec,           // converted to seconds since unix epoch
  lifeFirstRecvTime.stamp,           // as a timestamp (HH:mm:ss)
  dayofweek(lifeFirstRecvTime.stamp),// get the day of the week (Spark method)
  hour(lifeFirstRecvTime.stamp)      // get hour of the time (Spark method)
)
```

#### GeoInfo class 

Surgeon makes it easy to work with `geoInf` columns (`val.invariant.geoInfo`).  You can select `geoInfo`
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

Surgeon makes constructing the paths to the data easier (of class `DataPath`). You can provide simple
arguments for the `Monthly`, `Daily` or `Hourly` classes. 

```scala 
import conviva.surgeon.Paths._
val path = Hourly(2022, month = 12, days = 24, hours = 18)
val path2 = Hourly(2022, 12, 24, List(18, 19, 20))
```

You can select monthly, daily, or hourly data by customer.

```scala 
val path = Cust(Hourly(2022, 12, 24, 18), ids = 1960180360)
```

Can't remember the 9-10 digit Id of the customer? Then use the name, like this:

```scala 
val path = Cust(Hourly(2022, month = 12, days = 24, hours = 18), names = "c3.CBSCom")
```

Only want to select any three customers for a given path, then do:

```scala 
val path = Cust(Hourly(2022, 12, 24, 18)), take = 3)
```

See the [Paths wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/1-Paths-to-datasets) for more details about this functionality.


### Customer methods

Surgeon provides some convenient methods for working with Customer data. You
can use these methods to read in a file with customer Ids and names, get names
from Ids, and get Ids from names. 

```scala  
import conviva.surgeon.Customer._
// Pulls the customer names from GeoUtils/c3ServiceConfig.xml
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

