package com.sope.etl.custom

import com.sope.etl.register.UDFRegistration
import org.apache.spark.sql.UserDefinedFunction
import org.apache.spark.sql.functions._

object CustomUDF extends UDFRegistration {

  private def upperCase(s: String): String = s.toUpperCase()

  override protected def registerUDFs: Map[String, UserDefinedFunction] =
    Map("custom_upper" -> udf(upperCase _))
}
