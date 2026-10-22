package com.sope

import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Context for Unit test cases
  *
  * @author mbadgujar
  */
//noinspection ScalaDeprecation
object TestContext {

  private val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("UnitTest")
    .config("spark.driver.allowMultipleContexts", "true")
    .create()

  private val sqlContext = sparkSession.sqlContext
  def getSQlContext: SQLContext = sqlContext

}
