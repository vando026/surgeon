package conviva.surgeon

import conviva.surgeon.Paths

object {
  def getCustData(): DataFrame = {
    spark.read
      .option("delimiter", "|")
      .option("inferSchema", "true")
      .csv(Root.geoUtilsCustomer)
      .toDF("customerId", "name")
  }

}
