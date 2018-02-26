package kip.spark.assignment

import org.apache.spark.SparkContext

/**
  * Copyright Knoldus, Inc. 2015. All rights reserved.
  */
class SparkClient {
  def getSparkContext(sparkMaster: String, appName: String): SparkContext =
    new SparkContext(sparkMaster, appName)
}
