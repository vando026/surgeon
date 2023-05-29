<p align="center">
<img src="./media/surgeon-283.png" alt="" width="200" >
</p>

<h1 align="center"> conviva-surgeon</h1>

A Scala library with tools to operate on Session Summary and RawLog data. Surgeon is designed to reduce the verbose startup code needed to read the data and simplifies working with columns and column arrays. For example, surgeon reduces this mess (taken from a sample notebook on Databricks):

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
      , intvPlayingTimeMs(val.sessSummary) playingTimeMs 
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
    sessSum("lifePlayingTimeMs"), 
    lifeFirstRecvTime, 
    endedStatus, 
    shouldProcess, 
    intvStartTime
  )
```

### Path construction

Surgeon makes constructing the paths to the data easier, as shown in the first
line of the code above. Can't remember the 9-10 digit Id of the customer? Then use the name, like this:

```scala 
val path = Cust(Hourly(2022, 12, 24, List.range(16, 20)), names = "CBSCom")
```

Only want to select three customers for a given hour, then do:

```scala 
val path = Cust(Hourly(2022, 12, 24, List.range(16, 20)), take = 3)
```

See the [Paths wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/1-Paths-to-datasets) for more details about this functionality.

### Column methods

Surgeon provides methods to make it easier to work with columns.  For
example, `val.sessSummary.d3SessSummary.lifeFirstRecvTimeMs` is a 
of class `TimeMsCol` with `stamp` and `sec` methods. 

```scala 
hourly_df.select(
  lifeFirstRecvTime // its original form, milliseconds since unix epoch
  lifeFirstRecvTime.sec, // converted to seconds since unix epoch
  lifeFirstRecvTime.stamp, // as a timestamp (HH:mm:ss)
)
```

Columns that represent Ids have asis (signed), nosign (unsigned), or  hex (hexadecimal) methods. For example, to
construct a sid5 column ("clientId:sessionId") with either format, do:

```scala 
hourly_df.select(
  sid5.hex, 
  sid5.asis, 
  sid5.nosign, 
)
```

See the [PbSS wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/2-Selecting-fields-with-methods) for more details about this functionality.


More documentation forthcoming. 

<!-- Please see the wiki page for descriptions of surgeon's features. --> 

