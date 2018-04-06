package com.mayurb

import com.mayurb.model.{Class, Student}
import com.mayurb.spark.sql.dsl._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}

/**
  *
  * @author mbadgujar
  */
class DSLTest extends FlatSpec with Matchers {

  val testSData = Seq(
    Student("Sherlock", "Holmes", 5, 10),
    Student("John", "Watson", 5, 10)
  )

  val testCData = Seq(
    Class(10, "Tenth"),
    Class(9, "Ninth"),
    Class(8, "Ninth")
  )

  "DSL transformations" should "generate the transformations correctly" in {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DSLTest")
    val sc = new SparkContext(sparkConf)
    val sqlc = new SQLContext(sc)
    import sqlc.implicits._
    val studentDF = testSData.toDF()
    val classDF = testCData.toDF()
    val rename = Rename("first_name" -> "firstname", "last_name" -> "lastname")
    val transform = Transform("firstname" -> upper($"firstname"), "lastname" -> upper($"lastname"))
    val join = Join(None, "cls") <> classDF
    val transformed = rename + transform + join + Sequence(0l, "id") --> studentDF
    println("transformed DF output =>")
    transformed.show(false)
    transformed.count should be(4)
  }

}
