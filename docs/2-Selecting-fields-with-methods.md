Surgeon tries to simply the selection of fields when reading a dataset for the
first time. One way is to shorten the long string paths to a given field. An
example using a PbSS dataset is given below:

``` scala 
spark.read.parquet("path")
  .select(
    col("key.sessId.clientId")),
    col("val.sessSummary.d3SessSummary."),
    col("val.sessSummary.joinSwitchInfos.playingTime"),

  )
```

The `PbSS` object has several methods to shorten these paths. 
Using Surgeon, first import the `PbSS` object 

```scala mdoc 
import conviva.surgeon.PbSS._
```

Then do:

``` scala 
spark.read.parquet("path")
  .select(
    clientId,
    d3SS(""),
    joinSwitch("playingTimeMs")
  )
```
