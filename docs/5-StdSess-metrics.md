```scala mdoc
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
val spark = SparkSession.builder.master("local[*]").getOrCreate
```

## StdSess (Druid) metrics

Surgeon makes several `StdSess` metrics available; these metrics are usually
shown in Druid, such as `VSF`, `VSFT`, `VPF`, `EBVS`, and so on. 

```scala mdoc:invisible 
import conviva.surgeon.PbSS._
import conviva.surgeon.Paths._
PathDB = TestProfile()

```

```scala mdoc
import conviva.surgeon.PbSS._
// First point to the customer data in this test env
val path = pbss("2023-02-07T02").c3id(1960180360)
// Read in the test data
val dat0 = spark.read.parquet(path).cache
// Select only one client session Id
val dat = dat0.where(sessionId === 89057425)
```

```scala mdoc
    val metrics = dat.select(
        isEBVS, 
        isAttempt, 
        isJoinTimeAccurate, 
        isVPF, 
        isVPFT, 
        isVSF, 
        isVSFT, 
        hasJoined, 
        lifeAvgBitrateKbps, 
        firstHbTimeMs,
        intvAvgBitrateKbps, 
        intvBufferingTimeMs, 
        intvPlayingTimeMs, 
        justJoinedAndLifeJoinTimeMsIsAccurate, 
        isSessDoneNotJoined,
        isSessJustJoined
        )
    metrics.show
```


> Compiled using version @VERSION@. 
