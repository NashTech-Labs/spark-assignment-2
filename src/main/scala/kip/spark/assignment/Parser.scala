package kip.spark.assignment

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD

/**
  * Copyright Knoldus, Inc. 2015. All rights reserved.
  */
class Parser {
  def parseCustomer(line: RDD[String]): RDD[(Int, Customer)] = {
    line
      .map(_.split("[@#&*.?$+-]+"))
      .map { arr =>
        val customerId = arr(0).toInt
        val state = arr(2) takeRight (2)

        (customerId, Customer(customerId, state))
      }
  }

  def parseSale(line: RDD[String]): RDD[(Int, Sale)] = {
    line
      .map(_.split("[@#&*.?$+-]+"))
      .map { arr =>
        val timestamp = arr(0).toLong * 1000L
        val year: Int = new SimpleDateFormat("yyyy").format(timestamp).toInt
        val month: Int = new SimpleDateFormat("MM").format(timestamp).toInt
        val day: Int = new SimpleDateFormat("dd").format(timestamp).toInt
        val customer_id = arr(1).toInt
        val price = arr(2).toDouble

        (customer_id, Sale(timestamp, year, month, day, customer_id, price))
      }
  }
}
