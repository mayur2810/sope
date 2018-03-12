package com.mayurb.dwp

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Context for Unit test cases
  *
  * @author mbadgujar
  */
object TestContext {

  private val sparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("DQTest")
    .set("spark.driver.allowMultipleContexts", "true"))
  private val sqlContext = new SQLContext(sparkContext)

  def getSQlContext: SQLContext = sqlContext

}
