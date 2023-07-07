```scala mdoc
import org.apache.spark.sql.{SparkSession}
val spark = SparkSession.builder
  .master("local[*]")
  .getOrCreate
```

### Customer names data

The `customerNames` method reads in a
`/FileStore/Geo_Utils/c3ServiceConfig*.xml` file from Databricks, and converts
it to a `Map[Int, String]` with the customer Ids and names, respectively. 

For this demonstration, I use toy customer data. 

```scala mdoc
import conviva.surgeon.Customer._
// Reads in the file from Databricks
// customerNames()
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
customerIdToName(207488736, cdat)
customerIdToName(List(207488736, 744085924), cdat)
```
### Convert customer name to Id

Conversely, you can get a customer name from an Id from the customer data. 

```scala mdoc 
// can be String or List[String]
customerNameToId("TV2", cdat)
customerNameToId(List("TV2", "BASC"), cdat)
```

### Customer Ids on Databricks path

Another useful method is to get all the customer Ids from a data path on
Databricks. You can do so using the `customerIds` method, which takes a path
argument.  For example, you want to know all the customer Ids for a given hour
for PbSS data. 

```scala
val path = Hourly(month = 5, days = 22, hours = 18)
customerIds(path.toString)
```

### Shared customer Ids on PbSS and PbRl paths

Sometimes, you wish to work with both PbRl and PbSS data for a given Hour or
Day. But not all customers in PbSS appear in PbRl. You can use the
`customerInBothPaths` method to return the intersection of customers in both
paths. 


```scala
val pbss = Hourly(month = 5, days = 22, hours = 18)
val pbrl = HourlyRaw(month = 5, days = 22, hours = 18)
customerInBothPaths(pbss.toString, pbrl.toString)
```


