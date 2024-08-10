```scala mdoc:invisible
import org.apache.spark.sql.{SparkSession}
val spark = SparkSession.builder
  .master("local[*]")
  .getOrCreate
import org.surgeon.Paths._
import org.surgeon.PbSS._ 
```


The customer names and ids are read from the
`/FileStore/Geo_Utils/c3ServiceConfig*.csv` file from Databricks, which is
converted to a `Map[Int, String]`. For this demonstration, I use fake customer
data from the Test folder.

```scala mdoc
import org.surgeon.Customer._
```

```scala mdoc:invisible
val c3 = C3(TestPbSS())
def pbss(date: String) = SurgeonPath(TestPbSS()).make(date)
```

### Convert customer Id to name

To quickly convert a customer Id to name, you can use the `customerIdToName`
method. The method takes an Int or List[Int] as the first argument and the
customer data as the second. 

```scala mdoc 
// must be Int 
c3.idToName(1960181845)
c3.idToName(1960181845, 1960002004)
```
### Convert customer name to Id

Conversely, you can get a customer name from an Id from the customer data. 

```scala mdoc 
// can be String 
c3.nameToId("TV2")
c3.nameToId("TV2", "BASC")
```

### Customer Ids and names on Databricks path

Another useful method is to get all the customer ids or names from a path. You can do so using the `customerIds` method, which takes a path
argument.  For example, you want to know all the customer ids for a given hour
for PbSS data. 

```scala mdoc
val path = pbss("2023-02-07T02") 
c3.idOnPath(path.toString)
```

You can go one step further and convert the IDs to names, like so.

```scala mdoc
c3.idToName(c3.idOnPath(path.toString):_*)
```




