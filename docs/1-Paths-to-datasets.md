<p align="center">
<img src="./media/surgeon-283.png" alt="" width="200" >
</p>

<h1 align="center"> conviva-surgeon</h1>


```scala mdoc
// setup code
import org.apache.spark.sql.{SparkSession}
val spark = SparkSession.builder
  .master("local[*]")
  .getOrCreate
```


## Introduction and import

Surgeon provides classes for constructing Databricks paths to the parquet Rawlog (PbRl) and
Session Summary (PbSS) datasets. In the next section, I demonstrate path construction to data using the default path settings for  the `/mnt` (production)  directory of Databricks.

First import the `Paths` object. 

```scala mdoc 
import conviva.surgeon.Paths._
```

## DataPath class

On Databricks, the PbSS and PbRL data is in hourly, daily, or monthly intervals. These methods return a `DataPath` object which has a `.toString` and `.toList` methods. 

### Monthly 
For monthly PbSS production data use `pbssMonth`, which has a year and month parameter. So for February 2023:

```scala mdoc
val monthly = pbssMonth(year = 2023, month = 2)
monthly.toString
```

### Daily

For the daily PbSS production data use the `pbssDay`, which has year, month and
day parameter. The first example below is for February 16, 2023; the second
example is for the 16th and 17th day of that month.  Therefore, the day
parameter can take an Int or List[Int].

```scala mdoc 
val daily = pbssDay(year = 2023, month = 2, day = 16)
daily.toString
val daily2 = pbssDay(year = 2023, month = 2, day = List(16, 17))
daily2.toString
daily2.toList
```

The year defaults to the current year, so you can omit it as long as
the parameters are in the order of month then day.

```scala mdoc
val daily3 = pbssDay(2, List(16, 17))
daily3.toString
```

### Hourly

For the PbSS hourly production data, use `pbssHour`, which has hour parameter.

```scala mdoc 
val hourly = pbssHour(year = 2023, month = 2, day = 14, hour = 2)
hourly.toString
val hourly2 = pbssHour(month = 2, day = 14, hour = List.range(2, 10))
hourly2.toString
val hourly3 = pbssHour(month = 2, day = List(14, 15), hour = 2)
hourly3.toString
hourly3.toList
```
Again, the year argument defaults to the current year, which you can omit so
long as the parameters are in order of month, day(s), and hour(s). The day and hour parameters
can be an Int or List[Int] so that you can select multiple days or hours. 

###  RawLog
For the PbRl production data, use `pbrlHour`:

```scala mdoc 
val pbraw = pbrlHour(year = 2023, month = 2, day = 14, hour = List.range(2, 8))
pbraw.toString
```

## File paths

The methods above use the `PathDB` object to construct the root paths to various production datasets.

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
val ss = pbssHour(year = 2023, month = 2, day = 14, hour = List(2), root = PathDB.pbssProd1h(2))
ss.toString
```

If you wish to construct a path to a different folder, for example surgeon's test
data, then you can change the root path like so:

```scala modc
val path = pbssHour(year=2023, month=2, day=7, hour=2, root = PathDB.testPath + "pbss")
```

## Customer methods

Surgeon provides a way to select data for a month, day, or hour for one or more
customers. This is done using the `Cust` class, which has a `path` as a first parameter.
For this demonstration, use fake customerIds from surgeon's test data
folder. We therefore have to point to this test data first. 

```scala mdoc
import conviva.surgeon.GeoInfo._
// read the customer id Map
val custMap = getGeoData("customer", PathDB.testPath)
// construct the path to the test data
val path = pbssHour(year = 2023, month = 2, day = 7, 
    hour = 2, root = PathDB.testPath + "pbss")
``` 

To construct the path for all customers.

```scala mdoc
Cust(path)
```

To construct the path for one customer using the customer Id. 

```scala mdoc
Cust(path, id = 1960184999)
```

Using more than one customer Id, must be a `List`.

```scala mdoc
Cust(path, id = List(1960184999, 1960180360))
``` 
Take the first n customer Ids

```scala mdoc
Cust(path, take  = 3)
```

To select by customer name on the DataBricks production environment do:

```scala 
Cust(path, name = "c3.Yahoo")
``` 

However, because we are running this on the test environment, we need to
provide the fake customer data as an argument to `Cust`:

```scala mdoc
Cust(path, name = "c3.Yahoo", custMap)
``` 

To select by more than one customer name on the DataBricks production
environment, do:

```scala 
Cust(path, name = List("c3.Yahoo", "c3.MLB"), custMap)
``` 

For the test environment, it is:

```scala mdoc
Cust(path, name = List("c3.Yahoo", "c3.MLB"), custMap)
``` 


** Compiled using version @VERSION@. 
