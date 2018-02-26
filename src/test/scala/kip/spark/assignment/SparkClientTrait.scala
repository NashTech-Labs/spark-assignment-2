package kip.spark.assignment

import org.apache.spark.SparkContext

/**
  * Copyright Knoldus, Inc. 2015. All rights reserved.
  */
trait SparkClientTrait {
  val sc = new SparkContext("local", this.getClass.getName)
}
