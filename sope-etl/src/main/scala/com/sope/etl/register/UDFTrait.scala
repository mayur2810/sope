package com.sope.etl.register

import org.apache.spark.sql.expressions.UserDefinedFunction

trait UDFTrait {
  def getUDF: UserDefinedFunction
}
