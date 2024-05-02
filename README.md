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
val path = Path.pbss("2023-02-07T02").c3id(1960180360)
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
  ).show(false)
// +----------+----------+--------------------------------------------+-------------------+----------------------+------------+-------------+-------------------+--------+----------+-----------------+-------------------+-----------+-------------+----------------+
// |customerId|sessionId |sid5Hex                                     |intvStartTimeStamp |lifeFirstRecvTimeStamp|c3_viewer_id|c3_video_isAd|lifeFirstRecvTimeMs|hasEnded|justJoined|lifePlayingTimeMs|lifeFirstRecvTimeMs|endedStatus|shouldProcess|intvStartTimeSec|
// +----------+----------+--------------------------------------------+-------------------+----------------------+------------+-------------+-------------------+--------+----------+-----------------+-------------------+-----------+-------------+----------------+
// |1960180360|1567276763|28ae4823:45055e0d:1c62c137:ac58c6fa:5d6abedb|2023-02-07 02:00:00|2023-02-07 02:37:40   |null        |F            |1675766260779      |true    |false     |0                |1675766260779      |2          |false        |1675764000      |
// |1960180360|1300229737|30d612b6:ef71f10d:6258098f:52670453:4d7fee69|2023-02-07 02:00:00|2023-02-07 02:10:21   |8201852     |F            |1675764621566      |true    |true      |607992           |1675764621566      |1          |true         |1675764000      |
// |1960180360|1780527017|6f13d44:93e04b25:e2261399:943b801:6a20afa9  |2023-02-07 02:00:00|2023-02-07 01:53:29   |9435756     |F            |1675763609283      |true    |false     |409205           |1675763609283      |2          |true         |1675764000      |
// |1960180360|316612812 |acb4a655:517784c6:c474ec24:b8c8f21b:12df20cc|2023-02-07 02:00:00|2023-02-07 02:15:23   |null        |F            |1675764923015      |true    |false     |0                |1675764923015      |2          |false        |1675764000      |
// |1960180360|34482349  |a211b3a0:daaa52f6:20f3f049:4bb6780d:20e28ad |2023-02-07 02:00:00|2023-02-07 01:55:47   |7422604     |F            |1675763747016      |true    |false     |3467796          |1675763747016      |2          |true         |1675764000      |
// |1960180360|112309555 |dbadb6f0:b731afe:fd2a8c3d:ee8a206e:6b1b533  |2023-02-07 02:00:00|2023-02-07 02:03:34   |2069477     |F            |1675764214162      |true    |false     |0                |1675764214162      |2          |false        |1675764000      |
// |1960180360|1987519696|ad64cc1e:63cc6f3b:c269b028:5246c332:767724d0|2023-02-07 02:00:00|2023-02-07 02:37:58   |3488038     |F            |1675766278274      |true    |true      |6066             |1675766278274      |1          |true         |1675764000      |
// |1960180360|2133524627|4090b86e:2f7c549:36b26252:596bf0d9:7f2b0093 |2023-02-07 02:00:00|2023-02-07 02:11:46   |null        |F            |1675764706675      |true    |false     |0                |1675764706675      |2          |false        |1675764000      |
// |1960180360|89057425  |1c62b448:4d120d00:a613f8d2:b9a1e9b4:54ee891 |2023-02-07 02:00:00|2023-02-07 02:28:13   |2640043     |F            |1675765693115      |false   |true      |1742812          |1675765693115      |0          |true         |1675764000      |
// |1960180360|1635131946|3a46e1ba:6b5cf6cc:845b88b8:1f38b971:6176222a|2023-02-07 02:00:00|2023-02-07 02:45:17   |8205915     |F            |1675766717049      |false   |true      |882687           |1675766717049      |0          |true         |1675764000      |
// +----------+----------+--------------------------------------------+-------------------+----------------------+------------+-------------+-------------------+--------+----------+-----------------+-------------------+-----------+-------------+----------------+
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
).show 
// +----------+--------------------+----------+-------------+--------+---------+----------+-----------+---------+----------+
// |customerId|            clientId| sessionId|shouldProcess|hasEnded|justEnded|justJoined|endedStatus|joinState|joinTimeMs|
// +----------+--------------------+----------+-------------+--------+---------+----------+-----------+---------+----------+
// |1960180360|[682510371, 11579...|1567276763|        false|    true|     true|     false|          2|       -1|        -1|
// |1960180360|[819335862, -2777...|1300229737|         true|    true|     true|      true|          1|        1|      1524|
// |1960180360|[116473156, -1814...|1780527017|         true|    true|     true|     false|          2|        1|      1649|
// |1960180360|[-1397447083, 136...| 316612812|        false|    true|     true|     false|          2|       -1|        -1|
// |1960180360|[-1575898208, -62...|  34482349|         true|    true|     true|     false|          2|        1|      2327|
// |1960180360|[-609372432, 1920...| 112309555|        false|    true|     true|     false|          2|       -1|        -1|
// |1960180360|[-1385903074, 167...|1987519696|         true|    true|     true|      true|          1|        1|      2829|
// |1960180360|[1083226222, 4979...|2133524627|        false|    true|     true|     false|          2|       -1|        -1|
// |1960180360|[476230728, 12930...|  89057425|         true|   false|    false|      true|          0|        1|      4978|
// |1960180360|[977723834, 18012...|1635131946|         true|   false|    false|      true|          0|        1|       264|
// +----------+--------------------+----------+-------------+--------+---------+----------+-----------+---------+----------+
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
).show
// +-----------+----------------+-------------+-------------+----------------------+---------------------+-------------+
// |playerState|lifePausedTimeMs|playingTimeMs|sessionTimeMs|networkBufferingTimeMs|sessionCreationTimeMs|c3_video_isAd|
// +-----------+----------------+-------------+-------------+----------------------+---------------------+-------------+
// |        100|               0|          [0]|        [131]|                   [0]|        1675766259285|            F|
// |        100|               0|          [0]|     [620651]|                   [0]|        1675764620679|            F|
// |        100|               0|          [0]|     [516633]|                   [0]|        1675763608616|            F|
// |        100|               0|          [0]|         [42]|                   [0]|        1675764922419|            F|
// |        100|           74794|          [0]|    [3760066]|                   [0]|        1675763746254|            F|
// |        100|               9|          [0]|          [9]|                   [0]|        1675720505875|            F|
// |        100|               0|          [0]|       [8896]|                   [0]|        1675766277687|            F|
// |        100|               0|          [0]|      [36011]|                   [0]|        1675764394660|            F|
// |          3|               0|          [0]|    [1906885]|                [2375]|        1675765692087|            F|
// |          3|               0|          [0]|     [882951]|                   [0]|        1675766716209|            F|
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
hourly_df.select(
  sid5.concat,   
  sid5.concatToHex, 
  sid5.concatToUnsigned,
  sid6.concat, 
  sid6.concatToHex, 
  sid6.concatToUnsigned, 
).show(false)
// +--------------------------------------------------------+--------------------------------------------+------------------------------------------------------+----------------------------------------------------------------------+-----------------------------------------------------+----------------------------------------------------------------+
// |sid5                                                    |sid5Hex                                     |sid5Unsigned                                          |sid6                                                                  |sid6Hex                                              |sid6Unsigned                                                    |
// +--------------------------------------------------------+--------------------------------------------+------------------------------------------------------+----------------------------------------------------------------------+-----------------------------------------------------+----------------------------------------------------------------+
// |682510371:1157979661:476234039:-1403468038:1567276763   |28ae4823:45055e0d:1c62c137:ac58c6fa:5d6abedb|682510371:1157979661:476234039:2891499258:1567276763  |682510371:1157979661:476234039:-1403468038:1567276763:1675766259285   |28ae4823:45055e0d:1c62c137:ac58c6fa:5d6abedb:2b73de55|682510371:1157979661:476234039:2891499258:1567276763:729013845  |
// |819335862:-277745395:1649936783:1382483027:1300229737   |30d612b6:ef71f10d:6258098f:52670453:4d7fee69|819335862:4017221901:1649936783:1382483027:1300229737 |819335862:-277745395:1649936783:1382483027:1300229737:1675764620679   |30d612b6:ef71f10d:6258098f:52670453:4d7fee69:2b5add87|819335862:4017221901:1649936783:1382483027:1300229737:727375239 |
// |116473156:-1814017243:-500821095:155432961:1780527017   |6f13d44:93e04b25:e2261399:943b801:6a20afa9  |116473156:2480950053:3794146201:155432961:1780527017  |116473156:-1814017243:-500821095:155432961:1780527017:1675763608616   |6f13d44:93e04b25:e2261399:943b801:6a20afa9:2b4b6c28  |116473156:2480950053:3794146201:155432961:1780527017:726363176  |
// |-1397447083:1366787270:-998970332:-1194790373:316612812 |acb4a655:517784c6:c474ec24:b8c8f21b:12df20cc|2897520213:1366787270:3295996964:3100176923:316612812 |-1397447083:1366787270:-998970332:-1194790373:316612812:1675764922419 |acb4a655:517784c6:c474ec24:b8c8f21b:12df20cc:2b5f7833|2897520213:1366787270:3295996964:3100176923:316612812:727676979 |
// |-1575898208:-626371850:552857673:1270249485:34482349    |a211b3a0:daaa52f6:20f3f049:4bb6780d:20e28ad |2719069088:3668595446:552857673:1270249485:34482349   |-1575898208:-626371850:552857673:1270249485:34482349:1675763746254    |a211b3a0:daaa52f6:20f3f049:4bb6780d:20e28ad:2b4d85ce |2719069088:3668595446:552857673:1270249485:34482349:726500814   |
// |-609372432:192092926:-47543235:-292937618:112309555     |dbadb6f0:b731afe:fd2a8c3d:ee8a206e:6b1b533  |3685594864:192092926:4247424061:4002029678:112309555  |-609372432:192092926:-47543235:-292937618:112309555:1675720505875     |dbadb6f0:b731afe:fd2a8c3d:ee8a206e:6b1b533:28b9ba13  |3685594864:192092926:4247424061:4002029678:112309555:683260435  |
// |-1385903074:1674342203:-1033261016:1380369202:1987519696|ad64cc1e:63cc6f3b:c269b028:5246c332:767724d0|2909064222:1674342203:3261706280:1380369202:1987519696|-1385903074:1674342203:-1033261016:1380369202:1987519696:1675766277687|ad64cc1e:63cc6f3b:c269b028:5246c332:767724d0:2b742637|2909064222:1674342203:3261706280:1380369202:1987519696:729032247|
// |1083226222:49792329:917660242:1500246233:2133524627     |4090b86e:2f7c549:36b26252:596bf0d9:7f2b0093 |1083226222:49792329:917660242:1500246233:2133524627   |1083226222:49792329:917660242:1500246233:2133524627:1675764394660     |4090b86e:2f7c549:36b26252:596bf0d9:7f2b0093:2b576aa4 |1083226222:49792329:917660242:1500246233:2133524627:727149220   |
// |476230728:1293028608:-1508640558:-1180571212:89057425   |1c62b448:4d120d00:a613f8d2:b9a1e9b4:54ee891 |476230728:1293028608:2786326738:3114396084:89057425   |476230728:1293028608:-1508640558:-1180571212:89057425:1675765692087   |1c62b448:4d120d00:a613f8d2:b9a1e9b4:54ee891:2b6b36b7 |476230728:1293028608:2786326738:3114396084:89057425:728446647   |
// |977723834:1801254604:-2074376008:523811185:1635131946   |3a46e1ba:6b5cf6cc:845b88b8:1f38b971:6176222a|977723834:1801254604:2220591288:523811185:1635131946  |977723834:1801254604:-2074376008:523811185:1635131946:1675766716209   |3a46e1ba:6b5cf6cc:845b88b8:1f38b971:6176222a:2b7ad731|977723834:1801254604:2220591288:523811185:1635131946:729470769  |
// +--------------------------------------------------------+--------------------------------------------+------------------------------------------------------+----------------------------------------------------------------------+-----------------------------------------------------+----------------------------------------------------------------+
//
```

You can select the customer column using `customerId` and customer names using `customerName`.

```scala
hourly_df.select(
  customerId,  // Int: The customer Id
  customerName // String: Pulls the customer names from GeoUtils/c3ServiceConfig*.csv
).show(false)
// +----------+------------+
// |customerId|customerName|
// +----------+------------+
// |1960180360|c3.TopServe |
// |1960180360|c3.TopServe |
// |1960180360|c3.TopServe |
// |1960180360|c3.TopServe |
// |1960180360|c3.TopServe |
// |1960180360|c3.TopServe |
// |1960180360|c3.TopServe |
// |1960180360|c3.TopServe |
// |1960180360|c3.TopServe |
// |1960180360|c3.TopServe |
// +----------+------------+
//
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
hourly_df.select(
  lifeFirstRecvTime,                 // its original form, milliseconds since unix epoch
  lifeFirstRecvTime.toSec,           // converted to seconds since unix epoch
  lifeFirstRecvTime.stamp,           // as a timestamp (HH:mm:ss)
  dayofweek(lifeFirstRecvTime.stamp).alias("dow"), // get the day of the week (Spark method)
  hour(lifeFirstRecvTime.stamp).alias("hour")      // get hour of the time (Spark method)
).show
// +-------------------+--------------------+----------------------+---+----+
// |lifeFirstRecvTimeMs|lifeFirstRecvTimeSec|lifeFirstRecvTimeStamp|dow|hour|
// +-------------------+--------------------+----------------------+---+----+
// |      1675766260779|          1675766260|   2023-02-07 02:37:40|  3|   2|
// |      1675764621566|          1675764621|   2023-02-07 02:10:21|  3|   2|
// |      1675763609283|          1675763609|   2023-02-07 01:53:29|  3|   1|
// |      1675764923015|          1675764923|   2023-02-07 02:15:23|  3|   2|
// |      1675763747016|          1675763747|   2023-02-07 01:55:47|  3|   1|
// |      1675764214162|          1675764214|   2023-02-07 02:03:34|  3|   2|
// |      1675766278274|          1675766278|   2023-02-07 02:37:58|  3|   2|
// |      1675764706675|          1675764706|   2023-02-07 02:11:46|  3|   2|
// |      1675765693115|          1675765693|   2023-02-07 02:28:13|  3|   2|
// |      1675766717049|          1675766717|   2023-02-07 02:45:17|  3|   2|
// +-------------------+--------------------+----------------------+---+----+
//
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
).show(false)
// +---------+---------+-----+------+-----+------+------+------------------+-----------------+-------------------+----------------+------------------+-------------------------------------+------------------+-------------------+-----------------+
// |isAttempt|hasJoined|isVSF|isVSFT|isVPF|isVPFT|isEBVS|lifeAvgBitrateKbps|firstHbTimeMs    |isSessDoneNotJoined|isSessJustJoined|isJoinTimeAccurate|justJoinedAndLifeJoinTimeMsIsAccurate|intvAvgBitrateKbps|intvBufferingTimeMs|intvPlayingTimeMs|
// +---------+---------+-----+------+-----+------+------+------------------+-----------------+-------------------+----------------+------------------+-------------------------------------+------------------+-------------------+-----------------+
// |true     |false    |false|false |false|false |true  |0.0               |1.675766260779E12|true               |false           |false             |false                                |0.0               |0.0                |0.0              |
// |true     |true     |false|false |false|false |false |6073.0            |1.675764621566E12|false              |true            |true              |true                                 |6073.0            |247.0              |607992.0         |
// |false    |true     |false|false |false|false |false |6328.0            |1.675763609283E12|false              |false           |true              |false                                |6339.0            |0.0                |20137.0          |
// |true     |false    |false|false |false|false |true  |0.0               |1.675764923015E12|true               |false           |false             |false                                |0.0               |0.0                |0.0              |
// |false    |true     |false|false |false|false |false |6425.0            |1.675763747016E12|false              |false           |true              |false                                |6425.0            |0.0                |3217141.0        |
// |true     |false    |false|false |false|false |true  |0.0               |1.675764214162E12|true               |false           |false             |false                                |0.0               |0.0                |0.0              |
// |true     |true     |false|false |false|false |false |3381.0            |1.675766278274E12|false              |true            |true              |true                                 |3380.0            |0.0                |6066.0           |
// |true     |false    |false|false |false|false |true  |0.0               |1.675764706675E12|true               |false           |false             |false                                |0.0               |0.0                |0.0              |
// |true     |true     |false|false |false|false |false |5807.0            |1.675765693115E12|false              |true            |true              |true                                 |5806.0            |2375.0             |1742812.0        |
// |true     |true     |false|false |false|false |false |7153.0            |1.675766717049E12|false              |true            |true              |true                                 |7152.0            |0.0                |882687.0         |
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
).show(false)
// +------+-------+---------+
// |city  |country|continent|
// +------+-------+---------+
// |0     |165    |3        |
// |49218 |165    |3        |
// |86743 |165    |3        |
// |287049|165    |3        |
// |287049|165    |3        |
// |59747 |165    |3        |
// |94673 |165    |3        |
// |71321 |165    |3        |
// |289024|165    |3        |
// |280361|66     |3        |
// +------+-------+---------+
//
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
// +------+---------+-------+------------+---------+--------------+
// |city  |cityLabel|country|countryLabel|continent|continentLabel|
// +------+---------+-------+------------+---------+--------------+
// |0     |Unknown  |165    |Norway      |3        |null          |
// |49218 |null     |165    |Norway      |3        |null          |
// |86743 |null     |165    |Norway      |3        |null          |
// |287049|null     |165    |Norway      |3        |null          |
// |287049|null     |165    |Norway      |3        |null          |
// |59747 |null     |165    |Norway      |3        |null          |
// |94673 |null     |165    |Norway      |3        |null          |
// |71321 |null     |165    |Norway      |3        |null          |
// |289024|NewYark  |165    |Norway      |3        |null          |
// |280361|null     |66     |null        |3        |null          |
// +------+---------+-------+------------+---------+--------------+
//
```

### Path construction

Surgeon makes constructing the paths to the data easier. 
The production paths on Databricks are shown below. 


```scala
// monthly
Path.pbss("2023-02")
// res11: SurgeonPath = /mnt/conviva-prod-archive-pbss-monthly/pbss/monthly/y=2023/m=02/dt=c2023_02_01_08_00_to_2023_03_01_08_00
Path.pbss("2023-{2-5}")
// res12: SurgeonPath = /mnt/conviva-prod-archive-pbss-monthly/pbss/monthly/y=2023/m={02,03,04,05}/dt=c2023_{02,03,04,05}_01_08_00_to_2023_{03,04,05,06}_01_08_00

// daily
Path.pbss("2023-02-07")
// res13: SurgeonPath = /mnt/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=02/dt=d2023_02_07_08_00_to_2023_02_08_08_00
Path.pbss("2023-02-{7,9,14}")
// res14: SurgeonPath = /mnt/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=02/dt=d2023_02_{07,09,14}_08_00_to_2023_02_{08,10,15}_08_00

// hourly
Path.pbss("2023-02-07T09")
// res15: SurgeonPath = /mnt/conviva-prod-archive-pbss-hourly/pbss/hourly/st=0/y=2023/m=02/d=07/dt=2023_02_07_09
Path.pbss("2023-02-07T{8,9}")
// res16: SurgeonPath = /mnt/conviva-prod-archive-pbss-hourly/pbss/hourly/st=0/y=2023/m=02/d=07/dt=2023_02_07_{08,09}
```

Can't remember the 9-10 digit Id of the customer? Then use the name, like this:


```scala
// demonstrate using paths to Surgeon test data
Path.pbss("2023-02-07T02").c3name("c3.TopServe")
// res20: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}"
``` 

// To select by more than one customer name 

Only want to select any three customers for a given path, then do:


See the [Paths wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/1-Paths-to-datasets) for more details about this functionality.


### Customer methods

Surgeon provides some convenient methods for working with Customer data. You
can use these methods to read in a file with customer Ids and names, get names
from Ids, and get Ids from names. 

```scala
import conviva.surgeon.Customer._
// Pulls the customer names from GeoUtils/c3ServiceConfig_30Jan2024.csv
c3IdToName(1960180360)
// res23: List[String] = List("c3.TopServe")
c3IdToName(List(1960184661, 1960003321))
// res24: List[String] = List("c3.FappleTV", "c3.SATY")
c3NameToId("c3.FappleTV")
// res25: List[Int] = List(1960184661)
c3NameToId(List("c3.FappleTV", "c3.SATY"))
// res26: List[Int] = List(1960184661, 1960003321)
```

See the [Customers wiki](https://github.com/Conviva-Internal/conviva-surgeon/wiki/4-Customer-methods) for more details about this functionality.



<!-- Please see the wiki page for descriptions of surgeon's features. --> 

