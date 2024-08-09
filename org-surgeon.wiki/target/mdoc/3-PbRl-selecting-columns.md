
## Parquet RawLog (PbRl)

Surgeon tries to simply the selection of columns when reading a PbSS or PbRl dataset for the
first time. To demonstrate,  first import surgeon's `PbRl`  object, 
set the file path, and read the data. 

```scala 
import org.surgeon.PbRl._
```


```scala
val path = pbrl("2023-05-01T09").c3id(1960181845)
// path: String = "./surgeon/src/test/data/pbrl/y=2023/m=05/d=01/dt=2023_05_01_09/cust={1960181845}"
val dat = spark.read.parquet(path).where(sessionId === 701891892) 
// dat: Dataset[Row] = [header: struct<packetLength: smallint, ipAddress: int ... 2 more fields>, payload: struct<heartbeat: struct<logFormatVersion: bigint, gaRandomIntOffset: smallint ... 50 more fields>>]
```

### Quick selection

Quick ways to select frequently used columns:

```scala
dat.select(
  customerId,
  sessionId,
  clientId,
  timeStamp,
  sessionAdId, 
  seqNumber,
  dftot,
  dfcnt,
  sessionTimeMs,
  ipv6
).show(false)
// +----------+---------+----------------------------------------------+----------------+-----------+---------+-----+-----+-------------+----------+
// |customerId|sessionId|clientId                                      |timeStampUs     |sessionAdId|seqNumber|dftot|dfcnt|sessionTimeMs|publicipv6|
// +----------+---------+----------------------------------------------+----------------+-----------+---------+-----+-----+-------------+----------+
// |1960181845|701891892|[2610775990, 381728387, 1100623162, 684873090]|1682959801433765|null       |152      |null |null |[8360681]    |null      |
// +----------+---------+----------------------------------------------+----------------+-----------+---------+-----+-----+-------------+----------+
//
```


Surgeon makes it easy to select data from arrays, maps, and other structs. This eliminates the
need for typing out long path names. The available container
methods with examples are:

```scala
dat.select(
  pbSdm("cwsSeekEvent"),             // root: payload.heartbeat.pbSdmEvents
  c3Tags("c3.client.osf"),           // root: payload.heartbeat.c3Tags
  clientTags("serviceName"),         // root: payload.heartbeat.clientTag
  cwsPlayerEvent("playerState"),     // root: payload.heartbeat.cwsPlayerMeasurementEvent
  cwsStateChangeNew("playingState")  // root: payload.heartbeat.cwsStateChangeEvent.newCwsState
  ).show(false)
// +------------+-------------+-----------+-----------+------------+
// |cwsSeekEvent|c3_client_osf|serviceName|playerState|playingState|
// +------------+-------------+-----------+-----------+------------+
// |[null]      |null         |SlingTV    |[3]        |[null]      |
// +------------+-------------+-----------+-----------+------------+
//
```


### Columns conversion and formatting

Surgeon provides methods for selecting, converting and formatting column
values. For example, the `clientId` column is an array of 4 values, which can
be easily concatenated into a single string using the `concat` method. Some
fields like `clientId`, `clientSessionId`, `publicIpv6` are  inconsistently
formatted as unsigned, signed, or hexadecimal across the PbSS and PbRl
datasets. Surgeon makes it easy to both concat and format these values as is
(`concat`), as unsigned (`concatToUnsigned`), or as hexadecimal
(`concatToHex`). Another example is `sid5`, a column concatenated from the
`clientId` and `sessionId` columns, which is easy to do with Surgeon.



```scala
dat.select(
  customerId,
  sessionId,
  sessionId.toUnsigned,
  sessionId.toHex,
  sessionAdId,
  sessionAdId.toUnsigned,
  sessionAdId.toHex,
  clientId,
  clientId.concat,
  clientId.concatToUnsigned,
  clientId.concatToHex,
  sid5.concat,
  sid5.concatToUnsigned,
  sid5.concatToHex,
  sid5Ad.concat,
  sid5Ad.concatToUnsigned,
  sid5Ad.concatToHex,
).show(false)
// +----------+---------+-----------------+------------+-----------+-------------------+--------------+----------------------------------------------+-----------------------------------------+-----------------------------------------+-----------------------------------+---------------------------------------------------+---------------------------------------------------+--------------------------------------------+-----------------------------------------+-----------------------------------------+-----------------------------------+
// |customerId|sessionId|sessionIdUnsigned|sessionIdHex|sessionAdId|sessionAdIdUnsigned|sessionAdIdHex|clientId                                      |clientId                                 |clientIdUnsigned                         |clientIdHex                        |sid5                                               |sid5Unsigned                                       |sid5Hex                                     |sid5Ad                                   |sid5AdUnsigned                           |sid5AdHex                          |
// +----------+---------+-----------------+------------+-----------+-------------------+--------------+----------------------------------------------+-----------------------------------------+-----------------------------------------+-----------------------------------+---------------------------------------------------+---------------------------------------------------+--------------------------------------------+-----------------------------------------+-----------------------------------------+-----------------------------------+
// |1960181845|701891892|701891892        |29d60534    |null       |null               |null          |[2610775990, 381728387, 1100623162, 684873090]|2610775990:381728387:1100623162:684873090|2610775990:381728387:1100623162:684873090|9b9d47b6:16c0b683:419a2d3a:28d25582|2610775990:381728387:1100623162:684873090:701891892|2610775990:381728387:1100623162:684873090:701891892|9b9d47b6:16c0b683:419a2d3a:28d25582:29d60534|2610775990:381728387:1100623162:684873090|2610775990:381728387:1100623162:684873090|9b9d47b6:16c0b683:419a2d3a:28d25582|
// +----------+---------+-----------------+------------+-----------+-------------------+--------------+----------------------------------------------+-----------------------------------------+-----------------------------------------+-----------------------------------+---------------------------------------------------+---------------------------------------------------+--------------------------------------------+-----------------------------------------+-----------------------------------------+-----------------------------------+
//
```

Another useful column is `customerName`, which returns the name of the `customerId`. 

```scala
dat.select(
  customerId, 
  customerName 
).show
// +----------+------------+
// |customerId|customerName|
// +----------+------------+
// |1960181845|    c3.DuoFC|
// +----------+------------+
//
```

The `ipv4` and `ipv6` columns have methods for formatting: 

```scala
dat.select(
  ipv4,            // Array asis, payload.heartbeat.publicIp
  ipv4.concat,      // concat values
  ipv6,            // Array asis, payload.heartbeat.publicipv6
  ipv6.concat,     // concat values 
).show
// +-----------------+------------+----------+----+
// |         publicIp|        ipv4|publicipv6|ipv6|
// +-----------------+------------+----------+----+
// |[64, 98, 93, 119]|64:98:93:119|      null|null|
// +-----------------+------------+----------+----+
//
```
### Time columns

Surgeon provides `TimeSecCol`, `TimeMsCol`, and `TimeUsCol` classes with
methods to work with Unix Epoch time columns. These classes extends the base
class of a column to add `toMs()`, `toSec()` or `stamp()` methods to existing
column methods (i.e., `alias`, `when`, etc). The list below shows the available
`Time*Col` columns with methods.

```scala
dat.select(
  timeStamp,                  // as is, microseconds (us) since unix epoch
  timeStamp.toMs,             // converts to ms since unix epoch
  timeStamp.toSec,            // converts to seconds since unix epoch
  timeStamp.stamp,            // converts to timestamp (HH:mm:ss)
  sessionCreationTime,
  sessionCreationTime.toSec,
  sessionCreationTime.stamp,
).show(false)
// +----------------+-------------+------------+-------------------+---------------------+----------------------+------------------------+
// |timeStampUs     |timeStampMs  |timeStampSec|timeStamp          |sessionCreationTimeMs|sessionCreationTimeSec|sessionCreationTimeStamp|
// +----------------+-------------+------------+-------------------+---------------------+----------------------+------------------------+
// |1682959801433765|1682959801433|1682959801  |2023-05-01 09:50:01|1682951440681        |1682951440            |2023-05-01 07:30:40     |
// +----------------+-------------+------------+-------------------+---------------------+----------------------+------------------------+
//
```


### GeoInfo columns
Surgeon has a method to extract geoInfo data and provides a convenient
method called `label` to assign labels to the numeric coded geo fields. 

```scala 
dat.select(
  geoInfo("city"),    
  geoInfo("country"),
  geoInfo("city").label,
  geoInfo("country").label
).show
```


> Compiled using version 0.1.7. 
