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

On Databricks, the PbSS and PbRL data is in hourly, daily, or monthly intervals. These methods return a `SurgeonPath` object which has several methods described below. 

### Monthly 
For monthly PbSS production data use `Path.pbss`, which takes a string of format `yyyy-MM`, for example "2024-02". You can also specify a list or range of months like so: "2023-{2,4,5}" or "2024-{3-8}" (do not include spaces).

```scala mdoc
Path.pbss("2024-02")
Path.pbss("2023-{2,4,5}")
Path.pbss("2023-{3-8}")
```

### Daily

For the daily PbSS production data, provide a string argument of format
`yyyy-MM-dd`. Again, you can specify a list of range of days. You cannot
specify both a list or days and months. 

```scala mdoc 
Path.pbss("2024-02-01")
Path.pbss("2023-12-{2,4,5}")
Path.pbss("2023-12-{3-8}")
```

### Hourly

For the PbSS hourly production data, provide a string argument of format
`yyyy-MM-ddTHH`. You can specify a list or range of hours, but not a range of hours, days,
and/or months. 

```scala mdoc 
Path.pbss("2024-02-01T09")
Path.pbss("2023-12-10T{2,4,5}")
Path.pbss("2023-12-10T{3-8}")
```
For the hourly PbRl production data, use `Path.pbrl`

```scala mdoc 
Path.pbrl("2023-12-10T09")
```

## File paths

The methods above use the mutable `PathDB` object to modify path components to various production datasets.

```scala mdoc 
// root path
PathDB.root

// production 1 hour
PathDB.pbssHourly    

// production 1 day   
PathDB.pbssDaily     

// production 1 month
PathDB.pbssMonthly   
// rawlog production
PathDB.pbrlHourly    

// surgeon test path
PathDB.testPath      
```

I typically mutate these paths to the surgeon test path, for example:

```scala mdoc
PathDB.root = PathDB.testPath 
PathDB.pbssHourly = "pbss"    

// print the new paths
PathDB.root                   

// print the new paths
PathDB.pbssHourly             
```

The `pbssHourly` and `pbrlHourly`  paths default to `st_0` and `lt_1`, so you can set the `st`
flag using the relevant value:

```scala mdoc 
PathDB.st = 2
PathDB.lt = 17
```

You can get the default paths to Surgeon test files using the shorthand:

```scala mdoc
PathDB = TestProfile()
```

## Customer methods

Surgeon provides a way to select data for a month, day, or hour for one or more
customers. This is done using the `cust` method. For this demonstration, we use
fake customerIds from surgeon's test data folder, which we have to point to.

```scala mdoc
import conviva.surgeon.GeoInfo._
import conviva.surgeon.Paths._
// Set path to fake data in Test folder
PathDB = TestProfile()
```

 To construct the path for all customers on this date:
```scala mdoc
Path.pbss("2023-02-07T02").toString 
// or 
Path.pbss("2024-02-07T02").c3all
```

To construct the path for one customer using the customer Id. 
```scala mdoc
Path.pbss("2023-02-07T02").c3id(1960184999)
```
Or more than one. 
```scala mdoc
Path.pbss("2023-02-07T02").c3id(1960184999, 1960180360)
```
Take the first n customer Ids
```scala mdoc
Path.pbss("2023-02-07T02").c3take(3)
```
To select by customer name:
```scala mdoc
Path.pbss("2023-02-07T02").c3name("c3.TopServe")
```
Or more than one. 
```scala mdoc
Path.pbss("2023-02-07T02").c3name("c3.TopServe", "c3.PlayFoot")
``` 

> Compiled using version @VERSION@. 
