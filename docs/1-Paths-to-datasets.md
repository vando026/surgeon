# Setting paths to Databricks datasets

Surgeon provides classes for constructing Databricks paths to the rawlog and
session summary datasets (in parquet format). For now, these classes are
limited to paths on the `/mnt`  Databricks directory.

```scala mdoc
import org.apache.spark.sql.{SparkSession}
val spark = SparkSession.builder
  .master("local[*]")
  .getOrCreate
```

### Import
First import the `Paths` object that contains the classes:

```scala mdoc 
import conviva.surgeon.Paths._
```

On Databricks, a customer will have hourly, daily, or monthly data. The prupose
of the `Path` class is to contruct paths to these datasets. 

There are currently three classes each with a `path` method that prints out the
path as a string. These classes are: `Monthly`, `Daily`, and `Hourly`.

To construct the path to the parquet monthly session summary data, using
February 2023 as an example, use the `Monthly` class: 

```scala mdoc
val monthly = Monthly(year = 2023, month = 2)
monthly.toString
```

For the parquet daily session summary data, use the `Daily` class.

```scala mdoc 
val daily = Daily(year = 2023, month = 2, day = 16)
daily.toString
```

The year defaults to the current year, so you can omit it as long as the
parameters are in month then day order. 

```scala mdoc
val daily2 = Daily(2, 16)
daily2.toString
```

For the parquet hourly session summary data, use the `Hourly` class. 

```scala mdoc 
val hourly = Hourly(year = 2023, month = 2, day = 14, hours = List(2))
hourly.toString
val hourly2 = Hourly(month = 2, day = 14, hours = List.range(2, 10))
hourly2.toString
```
Again, the year argument defaults to the current year, which you can omit so
long as the parameters are in the month, day, then hours order. The hours parameter
must be a List so that you can select multiple hours in a day. 

For the parquet rawlog data, you need to change the root path.  This is because
the default path to `Hourly` is `PathDB.hourly`, which has the root `/mnt/databricks-user-share/pbss-hourly`.

```scala mdoc 
val pbraw = Hourly(year = 2023, month = 2, day = 14, hours = List(2), root = PathDB.rawlog())
pbraw.toString
val pbraw2 = Hourly(month = 2, day = 14, hours = List.range(2, 10), root = PathDB.rawlog())
pbraw2.toString
```

You can use standard scala code to extend the functionality. For example, with
the `Daily` class, you can only select 1 day at a time. To select 2 or more
days, you can do:

```scala mdoc
List(2, 5, 7).map(d => Daily(month = 2, day = d).toString)
```


### Paths object

The classes above use the `PathDB` object, which stores the root paths to
the respective datasets.

```scala mdoc 
PathDB.prodArchive
PathDB.hourly()
PathDB.daily
PathDB.monthly
PathDB.rawlog()
```
The `hourly` and `rawlog` root paths default to `st=1`, so you can set the `st`
flag using the relevant interger, provided it exists:


```scala mdoc 
val ss = Hourly(year = 2023, month = 2, day = 14, hours = List(2), root = PathDB.hourly(st=2))
ss.toString
```

### Customer methods

Surgeon provides methods for selecting customer ids or customer names through
the `Cust` class. These methods work as follows (using the `Daily`
class for demonstration).

To construct the path for all customers.

```scala mdoc 
val c1 = Cust(Daily(12, 28))
c1.toString
```
To construct the path for one customer using the Id. 

```scala mdoc
val c2 = Cust(Daily(12, 28), ids = List(1960184999))
c2.toString
```

Using several customer Ids.

```scala mdoc
val c3 = Cust(Daily(12, 28), ids = List(1960184999, 1960180360))
c3.toString
``` 
Take the first n customer Ids

```scala 
val c4 = Cust(Daily(12, 28), take  = 3)
c4.toString
// res: String = "/mnt/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=12/dt=d2023_12_28_08_00_to_2023_12_29_08_00/cust={1960180360,1960180361,1960180388}"
```

To select by customer name.

```scala 
val c5 = Cust(Daily(12, 28), names = List("Yahoo"))
c5.toString
// res: String = "/mnt/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=12/dt=d2023_12_28_08_00_to_2023_12_29_08_00/cust={450695772}"

``` 
To select by customer names.
```scala 
val c6 = Cust(Daily(12, 28), names = List("Yahoo", "MLB"))
c6.toString
// res: String = "/mnt/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=12/dt=d2023_12_28_08_00_to_2023_12_29_08_00/cust={450695772,1960180361}"
``` 
