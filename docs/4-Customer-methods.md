```scala mdoc
import org.apache.spark.sql.{SparkSession}
val spark = SparkSession.builder
  .master("local[*]")
  .getOrCreate
import conviva.surgeon.Paths._
PathDB = TestProfile()
```


The customer names and ids are read from the
`/FileStore/Geo_Utils/c3ServiceConfig*.csv` file from Databricks, which is
converted to a `Map[Int, String]`. For this demonstration, I use fake customer
data from the Test folder.

```scala mdoc
import conviva.surgeon.Customer._
import conviva.surgeon.Paths._
```

### Convert customer Id to name

To quickly convert a customer Id to name, you can use the `customerIdToName`
method. The method takes an Int or List[Int] as the first argument and the
customer data as the second. 

```scala mdoc 
// can be Int or List[Int]
c3IdToName(1960181845)
c3IdToName(List(1960181845, 1960002004))
```
### Convert customer name to Id

Conversely, you can get a customer name from an Id from the customer data. 

```scala mdoc 
// can be String or List[String]
c3NameToId("TV2")
c3NameToId(List("TV2", "BASC"))
```

### Customer Ids and names on Databricks path

Another useful method is to get all the customer ids or names from a path on
Databricks. You can do so using the `customerIds` method, which takes a path
argument.  For example, you want to know all the customer ids for a given hour
for PbSS data. 

```scala mdoc
val path = Path.pbss("2023-02-07T02")
c3IdOnPath(path)
```

You can go one step further and convert the IDs to names, like so.

```scala mdoc
c3IdToName(c3IdOnPath(path))
```




