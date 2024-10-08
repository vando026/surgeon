<p align="center">
<img src="./media/surgeon-283.png" alt="" width="200" >
</p>

<h1 align="center"> surgeon</h1>

A Scala library for Apache-Spark to query video streaming data. For example, Surgeon reduces this verbose SQL query

```scala mdoc:invisible
import org.apache.spark.sql.{SparkSession, Column}
import org.apache.spark.sql.functions._
val spark = SparkSession.builder.master("local[*]").getOrCreate
import org.surgeon.GeoInfo._
import org.surgeon.Paths._
import org.surgeon.PbSS._
def pbss(date: String) = SurgeonPath(TestPbSS()).make(date)
def customerName() = CustomerName(TestPbSS().geoUtilPath).make(customerId)
def geoInfo(field: String) = GeoBuilder(TestPbSS().geoUtilPath).make(field)
```

```scala mdoc
val path = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}"
val sql = spark.read.parquet(path)
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
```

to this query using the Scala API:


```scala mdoc
val path1 = pbss("2023-02-07T02").c3id(1960180360)
val df = spark.read.parquet(path1)
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
``` 


### Features

Below is a brief vignette of Surgeon's features to work with parquet raw log
(PbRl) and parquet session summary (PbSS) data. 

### Quick column selection

Surgeon makes it easy to select frequently used columns:

```scala mdoc
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
``` 

It is also easy to query from structs, maps or arrays in raw event or session
data:

```scala mdoc
df.select(
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


```scala mdoc
df.select(
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
df.select(
  customerId,  // Int: The customer Id
  customerName // String: Pulls the customer names from GeoUtils/c3ServiceConfig*.csv
).show(1, false)
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

```scala mdoc
df.select(
  lifeFirstRecvTime,                 // its original form, milliseconds since unix epoch
  lifeFirstRecvTime.toSec,           // converted to seconds since unix epoch
  lifeFirstRecvTime.stamp,           // as a timestamp (HH:mm:ss)
  dayofweek(lifeFirstRecvTime.stamp).alias("dow"), // get the day of the week (Spark method)
  hour(lifeFirstRecvTime.stamp).alias("hour")      // get hour of the time (Spark method)
).show(3, false)
```

#### GeoInfo class 

Surgeon makes it easy to work with the `geoInfo` struct.  You can select `geoInfo`
columns like so:

```scala mdoc
df.select(
  geoInfo("city"),        // Int: the city codes
  geoInfo("country"),     // Int: the country codes
  geoInfo("continent")    // Int: the continent codes
).orderBy("city").show(3, false)
```

It is hard to decipher what these codes mean, so Surgeon makes it easy by
providing a `label` method to map the codes to names: 



```scala mdoc
df.select(
  geoInfo("city"),            // Int: the city codes
  geoInfo("city").label,      // String: the city names
  geoInfo("country"),         // Int: the country codes
  geoInfo("country").label,   // String: the country names
  geoInfo("continent"),       // Int: the continent codes
  geoInfo("continent").label  // String: the continent names
).orderBy("city").show(3, false)
```

### Path construction

Surgeon makes constructing the paths to the data easier. 

```scala mdoc:reset:invisible
import org.apache.spark.sql.{SparkSession, Column}
import org.apache.spark.sql.functions._
val spark = SparkSession.builder.master("local[*]").getOrCreate
import org.surgeon.Customer._
import org.surgeon.GeoInfo._
import org.surgeon.Paths._
import org.surgeon.PbSS._
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
import org.surgeon.Customer._
import org.surgeon.GeoInfo._
import org.surgeon.Paths._
import org.surgeon.PbSS._
def pbss(date: String) = SurgeonPath(TestPbSS()).make(date)
```

Can't remember the 9-10 digit Id of the customer? Then use the name, like this:

```scala mdoc
// demonstrate using paths to Surgeon test data
pbss("2023-02-07T02").c3name("c3.TopServe")
``` 

To select by more than one customer name 

```scala mdoc
// demonstrate using paths to Surgeon test data
pbss("2023-02-07T02").c3name("c3.TopServe", "c3.PlayFoot")
```

Only want to select any three customers for a given path, then do:

```scala mdoc
// demonstrate using paths to Surgeon test data
pbss("2023-02-07T02").c3take(2)
```

See the [Paths wiki](https://github.com/vando026/surgeon/blob/main/org-surgeon.wiki/target/mdoc/1-Paths-to-datasets.md) for more details about this functionality.


### Customer methods

Surgeon provides some convenient methods for working with Customer data. You
can use these methods to read in a file with customer Ids and names, get names
from Ids, and get Ids from names. 

```scala mdoc:invisible
import org.surgeon.Customer._
val c3 = C3(TestPbSS())
```

```scala mdoc
// Pulls the customer names from GeoUtils/c3ServiceConfig_30Jan2024.csv
c3.idToName(1960180360)
c3.idToName(1960184661, 1960003321)
c3.nameToId("c3.FappleTV")
c3.nameToId("c3.FappleTV", "c3.SATY")
```

See the [Customers wiki](https://github.com/vando026/surgeon/blob/main/org-surgeon.wiki/target/mdoc/4-Customer-methods.md) for more details about this functionality.



<!-- Please see the wiki page for descriptions of surgeon's features. --> 

