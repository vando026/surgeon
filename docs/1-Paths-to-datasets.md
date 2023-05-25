# Setting paths to Databricks datasets

Surgeon provides classes for constructing Databricks paths to the Rawlog and
Session Summary datasets (in parquet format). For now, these classes are
limited to constructing paths for data on the `/mnt`  Databricks directory.

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

On Databricks, a customer will have hourly, daily, or monthly data. The purpose
of the `Path` class is to contruct paths to these datasets. 

There are currently three classes: `Monthly`, `Daily`, and `Hourly`, each with
a `toString` method.

To construct the path to the parquet monthly session summary data, using
February 2023 as an example, use the `Monthly` class: 

```scala mdoc
val monthly = Monthly(year = 2023, month = 2)
monthly.toString
```

For the parquet daily session summary data, use the `Daily` class.

```scala mdoc 
val daily = Daily(year = 2023, month = 2, days = 16)
daily.toString
val daily2 = Daily(year = 2023, month = 2, days = List(16, 17))
daily2.toString
```

The year defaults to the current year, so you can omit it as long as the
parameters are in month then day order. 

```scala mdoc
val daily3 = Daily(2, 16)
daily3.toString
```

For the parquet hourly session summary data, use the `Hourly` class. 

```scala mdoc 
val hourly = Hourly(year = 2023, month = 2, days = 14, hours = 2)
hourly.toString
val hourly2 = Hourly(month = 2, days = 14, hours = List.range(2, 10))
hourly2.toString
val hourly3 = Hourly(month = 2, days = List(14, 15), hours = 2)
hourly3.toString
```
Again, the year argument defaults to the current year, which you can omit so
long as the parameters are in order of month, day(s), and hour(s). The day and hour parameters
can be an Int or List[Int] so that you can select multiple days or hours. 

For the parquet hourly rawlog data, you can do:

```scala mdoc 
val pbraw = HourlyRaw(year = 2023, month = 2, days = 14, hours = List.range(2, 8))
pbraw.toString
```

You can also change the root path to the data.  For example, instead of using
the shorter `HourlyRaw`, we could use the `PathDB.rawlog` path. See below for
more details on `PathDB`.

```scala mdoc 
val pbraw2 = Hourly(year = 2023, month = 2, days = 14, hours = List(2, 3), root = PathDB.rawlog())
pbraw2.toString
```

### Paths object

The classes above use the `PathDB` object, which stores the root paths to
the various datasets.

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
val ss = Hourly(year = 2023, month = 2, days = 14, hours = List(2), root = PathDB.hourly(st=2))
ss.toString
```

### Customer methods

Surgeon provides methods for selecting customer ids or customer names through
the `Cust` class. These methods work as follows (using the `Daily`
class for demonstration).

To construct the path for all customers.

```scala mdoc 
val c1 = Cust(Daily(month = 12, days = 28))
```
To construct the path for one customer using the Id. 

```scala mdoc
val c2 = Cust(Daily(12, 28), ids = 1960184999)
```

Using several customer Ids.

```scala mdoc
val c3 = Cust(Daily(12, 28), ids = List(1960184999, 1960180360))
``` 
Take the first n customer Ids

```scala 
val c4 = Cust(Daily(12, 28), take  = 3)
// res: String = "/mnt/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=12/dt=d2023_12_28_08_00_to_2023_12_29_08_00/cust={1960180360,1960180361,1960180388}"
```

To select by customer name.

```scala 
val c5 = Cust(Daily(12, 28), names = "Yahoo")
// res: String = "/mnt/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=12/dt=d2023_12_28_08_00_to_2023_12_29_08_00/cust={450695772}"

``` 
To select by customer names.
```scala 
val c6 = Cust(Daily(12, 28), names = List("Yahoo", "MLB"))
// res: String = "/mnt/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=12/dt=d2023_12_28_08_00_to_2023_12_29_08_00/cust={450695772,1960180361}"
``` 
