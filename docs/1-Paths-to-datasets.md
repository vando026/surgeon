## Introduction and import

Surgeon provides classes for constructing Databricks paths to the parquet Rawlog (PbRl) and
Session Summary (PbSS) datasets. For now, I demonstrate path construction to data on  the `/mnt`  directory of Databricks.

```scala mdoc
import org.apache.spark.sql.{SparkSession}
val spark = SparkSession.builder
  .master("local[*]")
  .getOrCreate
```

Import the `Paths` object. 

```scala mdoc 
import conviva.surgeon.Paths._
```

## DataPath class

On Databricks, a customer will have hourly, daily, or monthly production data.
To construct paths to the data, the `Path` object provides three clasess,
called `Monthly`, `Daily`, and `Hourly`, which extend the `DataPath` trait. Each class comes with a `toString` and a `toList` method.

### Monthly 
For monthly PbSS production data use the `PbSS.prodMonthly` class which have year and month parameters.   So for February 2023:

```scala mdoc
val monthly = PbSS.prodMonthly(year = 2023, month = 2)
monthly.toString
```

### Daily
For the daily PbSS production data, use the `PbSS.prodDaily` class. 

```scala mdoc 
val daily = PbSS.prodDaily(year = 2023, month = 2, day = 16)
daily.toString
val daily2 = PbSS.prodDaily(year = 2023, month = 2, day = List(16, 17))
daily2.toString
daily2.toList
```

The first example is for February 16, 2023; the second example is for the 16th
and 17th day of that month.  Therefore, the day parameter can take an Int or
List[Int]. The year defaults to the current year, so you can omit it as long as
the parameters are in the order of month then day.

```scala mdoc
val daily3 = PbSS.prodDaily(2, List(16, 17))
daily3.toString
```

### Hourly

For the PbSS hourly production data, use the `PbSS.prodHourly` class:

```scala mdoc 
val hourly = PbSS.prodHourly(year = 2023, month = 2, day = 14, hours = 2)
hourly.toString
val hourly2 = PbSS.prodHourly(month = 2, day = 14, hours = List.range(2, 10))
hourly2.toString
val hourly3 = PbSS.prodHourly(month = 2, day = List(14, 15), hours = 2)
hourly3.toString
hourly3.toList
```
Again, the year argument defaults to the current year, which you can omit so
long as the parameters are in order of month, day(s), and hour(s). The day and hour parameters
can be an Int or List[Int] so that you can select multiple days or hours. 

###  RawLog
For the PbRl data, you can do:

```scala mdoc 
val pbraw = PbRl.prodHourly(year = 2023, month = 2, day = 14, hours = List.range(2, 8))
pbraw.toString
```

## File paths

The classes above use the `PathDB` object to construct paths to the various datasets.

```scala mdoc 
PathDB.prodArchive
PathDB.pbssProd1h()  // production 1 hour
PathDB.pbssProd1d    // production 1 day   
PathDB.pbssProd1M    // production 1 month
PathDB.pbrlProd()    // rawlog production
```
The `pbssProd1h` and `pbrlProd` root paths default to `st=1`, so you can set the `st`
flag using the relevant interger, provided it exists:


```scala mdoc 
val ss = PbSS.prodHourly(year = 2023, month = 2, day = 14, hours = List(2), root = PathDB.pbssProd1h(2))
ss.toString
```

## Customer methods

Surgeon provides methods for selecting customer ids or customer names through
the `Cust` class. This class also comes with a convenient `take` method. These methods work as follows (using the `Daily` class for demonstration).

To construct the path for all customers.

```scala mdoc 
val c1 = Cust(PbSS.prodDaily(month = 12, day = 28))
```
To construct the path for one customer using the customer Id. 

```scala mdoc
val c2 = Cust(PbSS.prodDaily(12, 28), ids = 1960184999)
```

Using several customer Ids.

```scala mdoc
val c3 = Cust(PbSS.prodDaily(12, 28), ids = List(1960184999, 1960180360))
``` 
Take the first n customer Ids

```scala 
val c4 = Cust(PbSS.prodDaily(12, 28), take  = 3)
// res: String = "/mnt/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=12/dt=d2023_12_28_08_00_to_2023_12_29_08_00/cust={1960180360,1960180361,1960180388}"
```

To select by customer name.

```scala 
val c5 = Cust(PbSS.prodDaily(12, 28), names = "c3.Yahoo")
// res: String = "/mnt/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=12/dt=d2023_12_28_08_00_to_2023_12_29_08_00/cust={450695772}"

``` 
To select by customer names.
```scala 
val c6 = Cust(PbSS.prodDaily(12, 28), names = List("c3.Yahoo", "c3.MLB"))
// res: String = "/mnt/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=12/dt=d2023_12_28_08_00_to_2023_12_29_08_00/cust={450695772,1960180361}"
``` 
