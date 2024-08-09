
## Introduction and import

Surgeon provides classes for constructing Databricks paths to the parquet Rawlog (PbRl) and
Session Summary (PbSS) datasets. In the next section, I demonstrate path construction to data using the default path settings for  the `/mnt` (production)  directory of Databricks.


On Databricks, the PbSS and PbRL data is in hourly, daily, or monthly intervals. 

### Monthly 
For monthly PbSS production data use `pbss`, which takes a string of format `yyyy-MM`, for example "2024-02". You can also specify a list or range of months like so: "2023-{2,4,5}" or "2024-{3-8}" (do not include spaces).


```scala
pbss("2024-02")
// res0: Builder = /mnt/org-prod-archive-pbss-monthly/pbss/monthly/y=2024/m=02/dt=c2024_02_01_08_00_to_2024_03_01_08_00
pbss("2023-{2,4,5}")
// res1: Builder = /mnt/org-prod-archive-pbss-monthly/pbss/monthly/y=2023/m={02,04,05}/dt=c2023_{02,04,05}_01_08_00_to_2023_{03,05,06}_01_08_00
pbss("2023-{3-8}")
// res2: Builder = /mnt/org-prod-archive-pbss-monthly/pbss/monthly/y=2023/m={03,04,05,06,07,08}/dt=c2023_{03,04,05,06,07,08}_01_08_00_to_2023_{04,05,06,07,08,09}_01_08_00
```

### Daily

For the daily PbSS production data, provide a string argument of format
`yyyy-MM-dd`. Again, you can specify a list of range of days. You cannot
specify both a list or days and months. 

```scala
pbss("2024-02-01")
// res3: Builder = /mnt/org-prod-archive-pbss-daily/pbss/daily/y=2024/m=02/dt=d2024_02_01_08_00_to_2024_02_02_08_00
pbss("2023-12-{2,4,5}")
// res4: Builder = /mnt/org-prod-archive-pbss-daily/pbss/daily/y=2023/m=12/dt=d2023_12_{02,04,05}_08_00_to_2023_12_{03,05,06}_08_00
pbss("2023-12-{3-8}")
// res5: Builder = /mnt/org-prod-archive-pbss-daily/pbss/daily/y=2023/m=12/dt=d2023_12_{03,04,05,06,07,08}_08_00_to_2023_12_{04,05,06,07,08,09}_08_00
```

### Hourly

For the PbSS hourly production data, provide a string argument of format
`yyyy-MM-ddTHH`. You can specify a list or range of hours, but not a range of hours, days,
and/or months. 

```scala
pbss("2024-02-01T09")
// res6: Builder = /mnt/org-prod-archive-pbss-hourly/pbss/hourly/st=0/y=2024/m=02/d=01/dt=2024_02_01_09
pbss("2023-12-10T{2,4,5}")
// res7: Builder = /mnt/org-prod-archive-pbss-hourly/pbss/hourly/st=0/y=2023/m=12/d=10/dt=2023_12_10_{02,04,05}
pbss("2023-12-10T{3-8}")
// res8: Builder = /mnt/org-prod-archive-pbss-hourly/pbss/hourly/st=0/y=2023/m=12/d=10/dt=2023_12_10_{03,04,05,06,07,08}
```
For the hourly PbRl production data for import the `PbRl` library, and use `pbrl`

```scala
import org.surgeon.PbRl._
pbrl("2023-12-10T09")
// res9: Builder = /mnt/org-prod-archive-pbrl/3d/rawlogs/pbrl/lt_1/y=2023/m=12/d=10/dt=2023_12_10_09
```

## Customer methods

Surgeon provides a way to select data for a month, day, or hour for one or more
customers. For this demonstration, we use fake customerIds from surgeon's test data folder (see the next section on how to change to the test folder path).


 To construct the path for all customers on this date:
```scala
pbss("2023-02-07T02").toString 
// res11: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02" 
// or 
pbss("2024-02-07T02").c3all
// res12: String = "./surgeon/src/test/data/pbss/y=2024/m=02/d=07/dt=2024_02_07_02/cust={*}"
```

To construct the path for one customer using the customer Id. 
```scala
pbss("2023-02-07T02").c3id(1960184999)
// res13: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960184999}"
```
Or more than one. 
```scala
pbss("2023-02-07T02").c3id(1960184999, 1960180360)
// res14: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960184999,1960180360}"
```
Take the first n customer Ids
```scala
pbss("2023-02-07T02").c3take(3)
// res15: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960002004,1960180360,1960181845}"
```
To select by customer name:
```scala
pbss("2023-02-07T02").c3name("c3.TopServe")
// res16: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360}"
```
Or more than one. 
```scala
pbss("2023-02-07T02").c3name("c3.TopServe", "c3.PlayFoot")
// res17: String = "./surgeon/src/test/data/pbss/y=2023/m=02/d=07/dt=2023_02_07_02/cust={1960180360,1960002004}"
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
> Compiled using version 0.1.7. 
