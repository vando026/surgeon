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
      , hasJoined(val.sessSummary) joined
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
import conviva.surgeon.Paths._
import conviva.surgeon.PbSS._
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
    sessSum("lifePlayingTimeMs"), 
    lifeFirstRecvTime, 
    endedStatus, 
    shouldProcess, 
    intvStartTime
  )
```

### Path construction

Surgeon makes constructing the paths to the data easier, as shown in the demo
code above. Can't remember the 9-10 digit Id of the customer? Then use the name, like this:

```scala 
val path = Cust(Hourly(2022, month = 12, days = 24, hours = 18), names = "CBSCom")
```

Only want to select three customers for a given hour, then do:

```scala 
val path = Cust(Hourly(2022, 12, 24, 18)), take = 3)
```

See the [Paths wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/1-Paths-to-datasets) for more details about this functionality.

### Column methods

Surgeon provides methods to make it easier to select and work with columns.  For
example, the `val.sessSummary.d3SessSummary.lifeFirstRecvTimeMs` is a 
class `TimeMsCol` with `stamp` and `sec` methods. You can select it using its
simple name with or without a method:

```scala 
hourly_df.select(
  lifeFirstRecvTime // its original form, milliseconds since unix epoch
  lifeFirstRecvTime.sec, // converted to seconds since unix epoch
  lifeFirstRecvTime.stamp, // as a timestamp (HH:mm:ss)
)
```

Surgeon makes it easier to work with Ids. Often, a `sid5` column is constructed from the `clientId` and `sessionId` columns. 
 Both  columns are inconsistently formatted across session summary and rawlog datasets. Surgeon constructs a `sid5` column for you with methods to format the values as is (`asis`), as unsigned (`nosign`), or as hexadecimal (`hex`). For example, to
construct a sid5 column (`clientId:sessionId`) with either format, do:

```scala 
hourly_df.select(
  sid5.asis, 
  sid5.hex, 
  sid5.nosign, 
)
```

The same methods can be used with `clientId` or `sessionId` only (which are of
class `IdCol`), and you can even construct an `sid6` column with `sessionCreationTimeMs`:

```scala 
hourly_df.select(
  sid6.asis, 
  sid6.hex, 
  sid6.nosign, 
)
```

See the [PbSS wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/2-PbSS-selecting-columns) and 
[PbRl wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/3-PbRl-selecting-columns) for more details about this functionality.

### Customer methods

Surgeon provides some convenient methods for working with Customer data. An
example of this functionality was shown in the conversion of the customer name
to Id in the demo code above. You can use these methods to read in a file with
customer Ids and names, get names from Ids, and get Ids from names. 

```scala  
import conviva.surgeon.Customer._
val cdat = customerNames(path = "./surgeon/src/test/data/cust_dat.txt")
cdat.show()
// +----------+------------+
// |customerId|customerName|
// +----------+------------+
// | 207488736|       MSNBC|
// | 744085924|        PMNN|
// |1960180360|         TV2|
// | 978960980|        BASC|
// +----------+------------+
//
customerIdToName(207488736, cdat)
// res2: Array[String] = Array("MSNBC")
customerIdToName(List(207488736, 744085924), cdat)
// res3: Array[String] = Array("MSNBC", "PMNN")
customerNameToId("TV2", cdat)
// res4: Array[Int] = Array(1960180360)
customerNameToId(List("TV2", "BASC"), cdat)
// res5: Array[Int] = Array(1960180360, 978960980)
```


See the [Customers wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/4-Customer-methods) for more details about this functionality.


Please see the [Wiki home page](https://github.com/Conviva-Internal/conviva-surgeon/wiki/0-Installation) for installation instructions. 

<!-- Please see the wiki page for descriptions of surgeon's features. --> 

