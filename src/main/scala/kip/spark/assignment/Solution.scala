package kip.spark.assignment

/**
  * Copyright Knoldus, Inc. 2015. All rights reserved.
  */
object Solution extends KipLogger with App {
  info("Starting Spark Context")
  val sparkContext = new SparkClient().getSparkContext(Config.sparkMaster, Config.sparkAppName)

  info("Parsing data")
  val parser = new Parser()
  val customer = parser.parseCustomer(sparkContext.textFile("src/main/resources/customers.txt"))
  val sale = parser.parseSale(sparkContext.textFile("src/main/resources/sales.txt"))

  info("Joining data")
  val joiner = new Joiner()
  val year = joiner.year(customer, sale)
  val month = joiner.month(customer, sale)
  val day = joiner.day(customer, sale)

  // prepare result
  val result = (year ++ month ++ day)
    .map(_.split("[@#&*?$+-]+"))
    .map(arr => (arr(0), arr))
    .groupByKey
    .flatMapValues { it =>
      it.map { arr =>
        if(arr.size == 3) {
          s"${arr(0)}#${arr(1)}###${arr(2)}"
        } else if(arr.size == 4) {
          s"${arr(0)}#${arr(1)}#${arr(2)}##${arr(3)}"
        } else {
          s"${arr(0)}#${arr(1)}#${arr(2)}#${arr(3)}#${arr(4)}"
        }
      }
    }.map(_._2).collect

  println("Result is: ")
  result.toList.map(println(_))
}
