

The customer names and ids are read from the
`/FileStore/Geo_Utils/c3ServiceConfig*.csv` file from Databricks, which is
converted to a `Map[Int, String]`. For this demonstration, I use fake customer
data from the Test folder.

```scala
import org.surgeon.Customer._
```


### Convert customer Id to name

To quickly convert a customer Id to name, you can use the `customerIdToName`
method. The method takes an Int or List[Int] as the first argument and the
customer data as the second. 

```scala 
// must be Int 
c3.idToName(1960181845)
// res0: Seq[String] = ArrayBuffer("c3.DuoFC")
c3.idToName(1960181845, 1960002004)
// res1: Seq[String] = ArrayBuffer("c3.DuoFC", "c3.PlayFoot")
```
### Convert customer name to Id

Conversely, you can get a customer name from an Id from the customer data. 

```scala 
// can be String 
c3.nameToId("TV2")
// res2: Seq[Int] = ArrayBuffer()
c3.nameToId("TV2", "BASC")
// res3: Seq[Int] = ArrayBuffer()
```

### Customer Ids and names on Databricks path

Another useful method is to get all the customer ids or names from a path. You can do so using the `customerIds` method, which takes a path
argument.  For example, you want to know all the customer ids for a given hour
for PbSS data. 

```scala
val path = pbss("2023-02-07T02") 
// path: Builder = ./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02 
c3.idOnPath(path.toString)
// res4: List[Int] = List(1960002004, 1960180360, 1960181845)
```

You can go one step further and convert the IDs to names, like so.

```scala
c3.idToName(c3.idOnPath(path.toString):_*)
// res5: Seq[String] = List("c3.PlayFoot", "c3.TopServe", "c3.DuoFC")
```




