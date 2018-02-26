package kip.spark.assignment

import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class ParserSpec extends FlatSpec with SparkClientTrait with BeforeAndAfterAll {
  val parser = new Parser()

  it should "parse customer" in {
    val result = parser.parseCustomer(sc.parallelize(List("123#AAA Inc#1 First Ave Mountain View CA#94040")))
    assert(result.collect().toList === List((123, Customer(123, "CA"))))
  }

  it should "parse sale" in {
    val result = parser.parseSale(sc.parallelize(List("1470049200#789#123459")))
    assert(result.collect().toList === List((789, Sale(1470049200000L, 2016, 8, 1, 789, 123459.0))))
  }

  override protected def afterAll(): Unit =
    sc.stop
}
