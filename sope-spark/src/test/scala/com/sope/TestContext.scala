package com.sope

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Context for Unit test cases
  *
  * @author mbadgujar
  */
//noinspection ScalaDeprecation
object TestContext {

  private val sparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("UnitTest")
    .set("spark.driver.allowMultipleContexts", "true"))

  private val sqlContext = new SQLContext(sparkContext)
  def getSQlContext: SQLContext = sqlContext

}
