package com.sope.etl

import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Context for Unit test cases
  *
  * @author mbadgujar
  */
object TestContext {

  def getSQlContext: SQLContext = {
    System.setProperty(UDFRegistrationClassProperty, "com.sope.etl.custom.CustomUDF")
    System.setProperty(TransformationRegistrationClassProperty, "com.sope.etl.custom.CustomTransformation")
    SparkSession.builder()
      .master("local[*]")
      .appName("SopeUnitTest")
      .getOrCreate().sqlContext
  }
}
