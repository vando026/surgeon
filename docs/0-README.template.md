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

```scala mdoc:invisible
import org.apache.spark.sql.{SparkSession, Column}
import org.apache.spark.sql.functions._
val spark = SparkSession.builder.master("local[*]").getOrCreate
import conviva.surgeon.GeoInfo._
import conviva.surgeon.Paths._
import conviva.surgeon.PbSS._
def pbss(date: String) = SurgeonPath(TestPbSS()).make(date)
def customerName() = CustomerName(TestPbSS().geoUtilPath).make(customerId)
def geoInfo(field: String) = GeoBuilder(TestPbSS().geoUtilPath).make(field)
```

```scala mdoc
val path = pbss("2023-02-07T02").c3id(1960180360)
val hourly_df = spark.read.parquet(path)
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
).show(3) 
``` 

It is also easy to query from structs, maps or arrays in PbSS and PbRl:

```scala mdoc
hourly_df.select(
  sessSum("playerState"), 
  d3SessSum("lifePausedTimeMs"),
  joinSwitch("playingTimeMs"),
  lifeSwitch("sessionTimeMs"),
  intvSwitch("networkBufferingTimeMs"), 
  invTags("sessionCreationTimeMs"), 
  sumTags("c3.video.isAd")
).show(3)
```

These `Column` classes come with their own methods. See for example `geoInfo` below; the `lifeSwitch`, `joinSwitch`, and `intvSwitch` are array columns which have several methods such as `first`, `last`, and
`distinct`, to name a few. See the 
[PbSS wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/2-PbSS-selecting-columns)
and 
[PbRl wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/3-PbRl-selecting-columns)
for more details about this functionality.

You can also use/mixin standard Scala API code with Surgeon syntax to select a column:

```scala mdoc
hourly_df.select(
  sessionId,
  col("val.sessSummary.intvFirstBufferLengthMs"),
  sessionState
).show(3, false)
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


```scala mdoc
hourly_df.select(
  sid5.concat,   
  sid5.concatToHex, 
  sid5.concatToUnsigned,
  sid6.concat, 
  sid6.concatToHex, 
  sid6.concatToUnsigned, 
).show(3, false)
```

You can select the customer column using `customerId` and customer names using `customerName`.

```scala mdoc
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

```scala mdoc
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

```scala mdoc
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

```scala mdoc
hourly_df.select(
  geoInfo("city"),        // Int: the city codes
  geoInfo("country"),     // Int: the country codes
  geoInfo("continent")    // Int: the continent codes
).show(3, false)
```

It is hard to decipher what these codes mean, so Surgeon makes it easy by
providing a `label` method to map the codes to names: 



```scala mdoc
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

```scala mdoc:reset:invisible
import org.apache.spark.sql.{SparkSession, Column}
import org.apache.spark.sql.functions._
val spark = SparkSession.builder.master("local[*]").getOrCreate
import conviva.surgeon.Customer._
import conviva.surgeon.GeoInfo._
import conviva.surgeon.Paths._
import conviva.surgeon.PbSS._
def pbss(date: String) = SurgeonPath(ProdPbSS()).make(date)
```

```scala mdoc
// monthly
pbss("2023-02")
pbss("2023-{2-5}")

// daily
pbss("2023-02-07")
pbss("2023-02-{7,9,14}")

// hourly
pbss("2023-02-07T09")
pbss("2023-02-07T{8,9}")
```

```scala mdoc:reset:invisible
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

```scala mdoc
// demonstrate using paths to Surgeon test data
pbss("2023-02-07T02").c3name("c3.TopServe")
``` 

// To select by more than one customer name 
```scala mdoc
// demonstrate using paths to Surgeon test data
pbss("2023-02-07T02").c3name("c3.TopServe", "c3.PlayFoot")
```

Only want to select any three customers for a given path, then do:

```scala mdoc
// demonstrate using paths to Surgeon test data
pbss("2023-02-07T02").c3take(2)
```

See the [Paths wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/1-Paths-to-datasets) for more details about this functionality.


### Customer methods

Surgeon provides some convenient methods for working with Customer data. You
can use these methods to read in a file with customer Ids and names, get names
from Ids, and get Ids from names. 

```scala mdoc:invisible
import conviva.surgeon.Customer._
val c3 = C3(TestPbSS())
```

```scala mdoc
// Pulls the customer names from GeoUtils/c3ServiceConfig_30Jan2024.csv
c3.idToName(1960180360)
c3.idToName(1960184661, 1960003321)
c3.nameToId("c3.FappleTV")
c3.nameToId("c3.FappleTV", "c3.SATY")
```

See the [Customers wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/4-Customer-methods) for more details about this functionality.



<!-- Please see the wiki page for descriptions of surgeon's features. --> 

