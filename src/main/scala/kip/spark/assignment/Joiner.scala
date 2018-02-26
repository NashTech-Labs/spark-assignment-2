package kip.spark.assignment

import org.apache.spark.rdd.RDD

/**
  * Copyright Knoldus, Inc. 2015. All rights reserved.
  */
class Joiner {
  // By Year
  def year(customer: RDD[(Int, Customer)], sale: RDD[(Int, Sale)]): RDD[String] = {
    customer
      .fullOuterJoin(sale)
      .groupBy { case(_, value) => (value._1.map(_.state).getOrElse(""), value._2.map(_.year).getOrElse(0)) }
      .map { case(key, value) => (key, value.map(_._2._2.map(_.price).getOrElse(0D))) }
      .filter { case(key, _) => key._1 != "" && key._2 != 0 }
      .map { case(key, value) => s"${key._1}#${key._2}###${value.sum}" }
  }

  // By Year and Month
  def month(customer: RDD[(Int, Customer)], sale: RDD[(Int, Sale)]): RDD[String] = {
    customer
      .fullOuterJoin(sale)
      .groupBy { case(_, value) => (value._1.map(_.state).getOrElse(""), value._2.map(_.year).getOrElse(0), value._2.map(_.month).getOrElse(0)) }
      .map { case(key, value) => (key, value.map(_._2._2.map(_.price).getOrElse(0D))) }
      .filter { case(key, _) => key._1 != "" && key._2 != 0 && key._3 != 0 }
      .map { case(key, value) => s"${key._1}#${key._2}#${key._3}##${value.sum}" }
  }

  // By Year, Month, and Day
  def day(customer: RDD[(Int, Customer)], sale: RDD[(Int, Sale)]): RDD[String] = {
    customer
      .fullOuterJoin(sale)
      .groupBy { case(_, value) => (value._1.map(_.state).getOrElse(""), value._2.map(_.year).getOrElse(0), value._2.map(_.month).getOrElse(0), value._2.map(_.day).getOrElse(0)) }
      .map { case(key, value) => (key, value.map(_._2._2.map(_.price).getOrElse(0D))) }
      .filter { case(key, _) => key._1 != "" && key._2 != 0 && key._3 != 0 && key._4 != 0 }
      .map { case(key, value) => s"${key._1}#${key._2}#${key._3}#${key._4}#${value.sum}" }
  }
}
