package com.sope.etl.utils

import org.apache.spark.sql.expressions.UserDefinedFunction

trait UDFTrait {
  def udff: UserDefinedFunction
}