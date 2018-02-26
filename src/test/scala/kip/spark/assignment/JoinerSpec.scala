package kip.spark.assignment

import org.scalatest.{BeforeAndAfterAll, FlatSpec}

/**
  * Copyright nDimensional, Inc. 2015. All rights reserved.
  */
class JoinerSpec extends FlatSpec with SparkClientTrait with BeforeAndAfterAll {
  val joiner = new Joiner()

  it should "join by year" in {
    val customer = sc.parallelize(List((123, Customer(123, "CA"))))
    val sale = sc.parallelize(List((123, Sale(1470049200000L, 2016, 8, 1, 123, 123459.0))))
    val result = joiner.year(customer, sale)

    assert(result.collect().toList === List("CA#2016###123459.0"))
  }

  it should "join by year and month" in {
    val customer = sc.parallelize(List((123, Customer(123, "CA"))))
    val sale = sc.parallelize(List((123, Sale(1470049200000L, 2016, 8, 1, 123, 123459.0))))
    val result = joiner.month(customer, sale)

    assert(result.collect().toList === List("CA#2016#8##123459.0"))
  }

  it should "join by year, month, and day" in {
    val customer = sc.parallelize(List((123, Customer(123, "CA"))))
    val sale = sc.parallelize(List((123, Sale(1470049200000L, 2016, 8, 1, 123, 123459.0))))
    val result = joiner.day(customer, sale)

    assert(result.collect().toList === List("CA#2016#8#1#123459.0"))
  }

  override protected def afterAll(): Unit =
    sc.stop()
}
