# Setting datasets paths

Surgeon provides case classes for constructing Databricks paths to the rawlog
and session summary datasets. For now, these classes are limited to paths on the
`/mnt` directory on Databricks.


### Import
First import the `Paths` object that contains the classes:

```scala mdoc 
import conviva.surgeon.Paths._
```

### Classes

There are currently four classes each with a path method that prints out the
path as a string. These are: `PbSSMonthly`, `PbSSDaily`, `PbSSHourly`, and
`PbRawLog`.

To construct the path to the parquet monthly session summary data, using
February 2023 as an example, use the `PbSSMonthly` class: 

```scala mdoc
val monthly = PbSSMonthly(year = 2023, month = 2)
monthly.path
```

For the parquet daily session summary data, use the `PbSSDaily` class.

```scala mdoc 
val daily = PbSSDaily(year = 2023, month = 2, day = 16)
daily.path
```

The year defaults to the current year, so you can omit it as long as the
parameters are in month then day order. 

```scala mdoc
val daily2 = PbSSDaily(2, 16)
daily2.path
```

For the parquet hourly session summary data, use the `PbSSHourly` class. 

```scala mdoc 
val hourly = PbSSHourly(year = 2023, month = 2, day = 14, hours = List(2))
hourly.path
val hourly2 = PbSSHourly(month = 2, day = 14, hours = List.range(2, 10))
hourly2.path
```
Again, the year argument defaults to the current year, which you can omit so
long as the parameters are in the month, day, then hours order. The hours parameter
must be a List so that you can select multiple hours in a day. 

For the parquet rawlog data, use the `PbRawLog` class. 

```scala mdoc 
val pbraw = PbRawLog(year = 2023, month = 2, day = 14, hours = List(2))
pbraw.path
val pbraw2 = PbRawLog(month = 2, day = 14, hours = List.range(2, 10))
pbraw2.path
```

You can use standard scala code to extend the functionality. For example, with
the `PbSSDaily` class, you can only select 1 day at a time. To select 2 or more
days, you can do:

```scala mdoc
List(2, 5, 7).map(d => PbSSDaily(month = 2, day = d).path)
```


### Paths object

The classes above use the `PrArchPaths` object, which stores the root paths to
the respective datasets.

```scala mdoc 
PrArchPaths.root
PrArchPaths.hourly
PrArchPaths.daily
PrArchPaths.monthly
PrArchPaths.rawlog
```

### Customer methods

Surgeon provides methods for selecting customer Ids or customer names through
the `CustomerPath` class. These methods work as follows (using the `PbSSDaily`
class for demonstration).

To construct the path for all customers.

```scala mdoc 
PbSSDaily(12, 28).custAll
```
or 

```scala mdoc
PbSSDaily(12, 28).path
```

To construct the path for one customer Id. 

```scala mdoc
PbSSDaily(12, 28).custId(1960184999)
```

Using several customer Ids.

```scala mdoc
PbSSDaily(12, 28).custIds(List(1960184999, 1960180360))
``` 
Take first n customer Ids

```scala 
PbSSDaily(12, 28).custTake(n = 3)
// res: String = "/mnt/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=12/dt=d2023_12_28_08_00_to_2023_12_29_08_00/cust={1960180360,1960180361,1960180388}"
```

To select by customer name.

```scala 
PbSSDaily(12, 28).custName("Yahoo")
// res: String = "/mnt/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=12/dt=d2023_12_28_08_00_to_2023_12_29_08_00/cust={450695772}"

``` 
To select by customer names.
```scala 
PbSSDaily(12, 28).custName(List("Yahoo", "MLB"))
// res: String = "/mnt/conviva-prod-archive-pbss-daily/pbss/daily/y=2023/m=12/dt=d2023_12_28_08_00_to_2023_12_29_08_00/cust={450695772,1960180361}"
``` 
