package kip.spark.assignment

/**
  * Copyright Knoldus, Inc. 2015. All rights reserved.
  */
object Config {
  val sparkMaster = System.getenv("SPARK_MASTER")
  val sparkAppName = System.getenv("SPARK_APP_NAME")
}
