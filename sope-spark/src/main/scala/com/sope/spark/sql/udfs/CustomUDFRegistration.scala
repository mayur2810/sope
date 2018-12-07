package com.sope.spark.sql.udfs

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.UserDefinedFunction


/**
  * Trait to use for Custom UDF registration
  *
  * @author mbadgujar
  */
trait CustomUDFRegistration {
  protected def registerUDFs: Map[String, UserDefinedFunction]

  def performRegistration(sqlContext: SQLContext): Unit = {
    registerUDFs.foreach { case (name, udf) => sqlContext.udf.register(name, udf) }
  }
}
