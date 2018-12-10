package com.sope.etl.register

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Trait to use for Custom UDF registration
  *
  * @author mbadgujar
  */
trait UDFRegistration {

  /**
    * Returns the list of UDFs to Register
    *
    * @return Map of Registration name and UDF
    */
  protected def registerUDFs: Map[String, UserDefinedFunction]

  /**
    * Registers the UDF in Spark's Registry
    */
  def performRegistration(sqlContext: SQLContext): Unit = {
    registerUDFs.foreach { case (name, udf) => sqlContext.udf.register(name, udf) }
  }


}
