<p align="center">
<img src="./media/surgeon-283.png" alt="" width="200" >
</p>

<h1 align="center"> conviva-surgeon</h1>
A scala library with tools to operate on data generated from Conviva
Heartbeats. The library is aimed at data scientists or engineers who run their analysis scripts on Databricks. Surgeon is designed to reduce the verbose startup code needed to read the rawlog or session summary data. It also simplifies the basic but often tedious tasks of converting between timestamps, seconds, and milliseconds; manipulating arrays; constructing signed/unsigned session Ids; and cleaning or recoding frequently used fields; to name some of these tasks. 
For example, a primary goal of surgeon is to reduce this mess:

```
val hourly_df = sqlContext.read.parquet("/mnt/conviva-prod-archive-pbss-hourly/pbss/hourly/st=0/y=2022/m=12/d=25/dt=2022_12_25_{16,17,18,19}/cust={1960184999}")
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
val path = PbSSHourly(2022, 12, 24, List.range(16, 20)).custId(1960184999)
val hourly_df = spark.read.parquet(path)
  .select(
    customerId, sessionId, sid5.hex, 
    intvStartTime.stamp, lifeFirstRecvTime.stamp, 
    viewerId, c3VideoIsAd, lifeFirstRecvTime.ms, 
    hasEnded, hasJoined, justJoined, 
    playingTime.ms, lifeFirstRecvTime.ms, 
    endedStatus, shouldProcess, 
    intvStartTime.sec
)

```
Do you always struggle to remember what customer name the Id represents, or
vice versa? Then you can construct your path like so:

```scala 
val path = PbSSHourly(2022, 12, 24, List.range(16, 20)).custName("CBSCom")
val hourly_df = spark.read.parquet(path)
```

In the code above, some fields are objects with methods, so that you could get
various formats of, for example, `lifeFirstRecvTime`:

```scala 
hourly_df.select(
  lifeFirstRecvTime.stamp, // as a timestamp 
  lifeFirstRecvTime.ms, // milliseconds since unix epoch
  lifeFirstRecvTime.sec, // seconds since unix epoch
  lifeFirstRecvTime.asis // its original form, milliseconds since unix epoch
)
```

Similarly, we could get the client Id : session Id (sid5) as signed or unsigned
integers or hexadecimal. 

```scala 
hourly_df.select(
  sid5.hex, 
  sid5.signed, 
  sid5.unsigned, 
)
```

Please see the wiki page for descriptions of surgeon's features. 

