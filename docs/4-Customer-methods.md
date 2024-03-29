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

For this demonstration, I use toy customer data from the Test folder. 

```scala mdoc
import conviva.surgeon.Customer._
import conviva.surgeon.Paths._
// Point to the fake customer data
PathDB.geoUtilPath = PathDB.testPath
```

### Convert customer Id to name

To quickly convert a customer Id to name, you can use the `customerIdToName`
method. The method takes an Int or List[Int] as the first argument and the
customer data as the second. 

```scala mdoc 
// can be Int or List[Int]
c3IdToName(207488736)
c3IdToName(List(207488736, 744085924))
```
### Convert customer name to Id

Conversely, you can get a customer name from an Id from the customer data. 

```scala mdoc 
// can be String or List[String]
c3NameToId("TV2")
c3NameToId(List("TV2", "BASC"))
```

### Customer Ids and names on Databricks path

Another useful method is to get all the customer Ids from a data path on
Databricks. You can do so using the `customerIds` method, which takes a path
argument.  For example, you want to know all the customer Ids for a given hour
for PbSS data. 

```scala mdoc
val path = Path.pbss("2023-02-07T02").toList(0)
c3IdOnPath(path)
```

You can go one step further and convert the IDs to names, like so (using PbRl
data):

```scala mdoc
val path2 = Path.pbrl("2023-05-01T09").toList(0)
c3IdToName(c3IdOnPath(path2))
```




