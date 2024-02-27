```scala mdoc
import org.apache.spark.sql.{SparkSession}
val spark = SparkSession.builder
  .master("local[*]")
  .getOrCreate
```

### Customer names data

The `c3IdMap` method reads in a
`/FileStore/Geo_Utils/c3ServiceConfig*.csv` file from Databricks, and converts
it to a `Map[Int, String]` with the customer Ids and names, respectively. 

For this demonstration, I use toy customer data. 

```scala mdoc
import conviva.surgeon.Customer._
import conviva.surgeon.Paths._

// Reads in the file from Databricks
// c3IdMap()
// For this demo, I used the toy data below
val cdat: Map[Int, String] = Map(
  207488736 -> "c3.MSNBC",
  744085924 -> "c3.PMNN",
  1960180360 -> "c3.TV2",
  978960980 -> "c3.BASC"
)
```

### Convert customer Id to name

To quickly convert a customer Id to name, you can use the `customerIdToName`
method. The method takes an Int or List[Int] as the first argument and the
customer data as the second. 

```scala mdoc 
// can be Int or List[Int]
c3IdToName(207488736, cdat)
c3IdToName(List(207488736, 744085924), cdat)
```
### Convert customer name to Id

Conversely, you can get a customer name from an Id from the customer data. 

```scala mdoc 
// can be String or List[String]
c3NameToId("TV2", cdat)
c3NameToId(List("TV2", "BASC"), cdat)
```

### Customer Ids and names on Databricks path

Another useful method is to get all the customer Ids from a data path on
Databricks. You can do so using the `customerIds` method, which takes a path
argument.  For example, you want to know all the customer Ids for a given hour
for PbSS data. 

```scala
val path = PbSS.prodHourly(month = 5, day = 22, hour = 18)
c3IdOnPath(path.toString)
```

You can go one step further and convert the IDs to names, like so (using PbRl
data):

```scala
val path = PbRl.prodHourly(month = 7, day = 4, hour = 16)
c3IdToName(c3IdOnPath(path.toString))
// path: conviva.surgeon.Paths.HourlyRaw[Int] = /mnt/conviva-prod-archive-pbrl/3d/rawlogs/pbrl/lt_1/y=2023/m=07/d=04/dt=2023_07_04_16
// res4: List[String] = List(c3.Turner-MML, c3.Echostar-SlingTV, c3.Movistarplus, c3.BBCK-PerformGroup, c3.Turner-NCAA, 
// c3.Turner-TBS, c3.Atresmedia, c3.HearstTV, c3.Univision-OTT-Streaming, c3.SportsNet-SNY, c3.TELUS, c3.LGE, c3.OSNtv)
```

### Shared customer Ids on PbSS and PbRl paths

Sometimes, you wish to work with both PbRl and PbSS data for a given Hour or
Day. But not all customers in PbSS appear in PbRl. You can use the
`c3IdInBothPaths` method to return the intersection of customers in both
paths. 


```scala
val pbss = PbSS.prodHourly(month = 5, day = 22, hour = 18)
val pbrl = PbSS.prodHourly(month = 5, day = 22, hour = 18)
c3IdInBothPaths(pbss.toString, pbrl.toString)
```



