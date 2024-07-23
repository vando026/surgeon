```scala mdoc:invisible
// setup code
import org.apache.spark.sql.{SparkSession}
import conviva.surgeon.Paths._
val spark = SparkSession.builder
  .master("local[*]")
  .getOrCreate
import conviva.surgeon.PbSS._ 
def pbss(date: String) = SurgeonPath(ProdPbSS()).make(date)
```

## Introduction and import

Surgeon provides classes for constructing Databricks paths to the parquet Rawlog (PbRl) and
Session Summary (PbSS) datasets. In the next section, I demonstrate path construction to data using the default path settings for  the `/mnt` (production)  directory of Databricks.


On Databricks, the PbSS and PbRL data is in hourly, daily, or monthly intervals. 

### Monthly 
For monthly PbSS production data use `pbss`, which takes a string of format `yyyy-MM`, for example "2024-02". You can also specify a list or range of months like so: "2023-{2,4,5}" or "2024-{3-8}" (do not include spaces).


```scala mdoc
pbss("2024-02")
pbss("2023-{2,4,5}")
pbss("2023-{3-8}")
```

### Daily

For the daily PbSS production data, provide a string argument of format
`yyyy-MM-dd`. Again, you can specify a list of range of days. You cannot
specify both a list or days and months. 

```scala mdoc
pbss("2024-02-01")
pbss("2023-12-{2,4,5}")
pbss("2023-12-{3-8}")
```

### Hourly

For the PbSS hourly production data, provide a string argument of format
`yyyy-MM-ddTHH`. You can specify a list or range of hours, but not a range of hours, days,
and/or months. 

```scala mdoc
pbss("2024-02-01T09")
pbss("2023-12-10T{2,4,5}")
pbss("2023-12-10T{3-8}")
```
For the hourly PbRl production data for import the `PbRl` library, and use `pbrl`

```scala mdoc
import conviva.surgeon.PbRl._
pbrl("2023-12-10T09")
```

## Customer methods

Surgeon provides a way to select data for a month, day, or hour for one or more
customers. For this demonstration, we use fake customerIds from surgeon's test data folder (see the next section on how to change to the test folder path).

```scala mdoc:invisible:reset
import org.apache.spark.sql.{SparkSession}
import conviva.surgeon.Paths._
import conviva.surgeon.GeoInfo._
import conviva.surgeon.Customer._
val spark = SparkSession.builder
  .master("local[*]")
  .getOrCreate
// Set path to fake data in Test folder
def pbss(date: String) = SurgeonPath(TestPbSS()).make(date)
val c3 = C3(TestPbSS())
```

 To construct the path for all customers on this date:
```scala mdoc
pbss("2023-02-07T02").toString 
// or 
pbss("2024-02-07T02").c3all
```

To construct the path for one customer using the customer Id. 
```scala mdoc
pbss("2023-02-07T02").c3id(1960184999)
```
Or more than one. 
```scala mdoc
pbss("2023-02-07T02").c3id(1960184999, 1960180360)
```
Take the first n customer Ids
```scala mdoc
pbss("2023-02-07T02").c3take(3)
```
To select by customer name:
```scala mdoc
pbss("2023-02-07T02").c3name("c3.TopServe")
```
Or more than one. 
```scala mdoc
pbss("2023-02-07T02").c3name("c3.TopServe", "c3.PlayFoot")
``` 

## File paths

Surgeon sets the file paths to the production PbSS and PbRl datasets. Behind
the hood, it does this:

```scala 
def pbss(date: String) = SurgeonPath(ProdPbSS()).make(date)
```

by calling the `ProdPbSS` object. 

To change to the test environment paths, you can do:

```scala 
def pbss(date: String) = SurgeonPath(TestPbSS()).make(date)
```
> Compiled using version @VERSION@. 
