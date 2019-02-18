package com.sope.etl

import com.sope.etl.utils.ScalaScriptEngine

object UDFTest {

  def main(args: Array[String]): Unit = {
    val udf = ScalaScriptEngine.eval("test", "(i: Int) => i + 2")
    println(udf.dataType)
  }
}
