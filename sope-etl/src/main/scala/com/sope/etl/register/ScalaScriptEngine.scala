package com.sope.etl.register

import com.sope.utils.Logging

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain
import scala.util.{Failure, Success, Try}
object ScalaScriptEngine extends Logging {

  val DefaultClassLocation = "/tmp/sope/dynamic"

  private def objectString(clazz: String, code: String) =
    s"""
       | package com.sope.etl.dynamic
       |
       | import org.apache.spark.sql.functions._
       | import org.apache.spark.sql.expressions.UserDefinedFunction
       | import com.sope.etl.register.UDFTrait
       |
       | object $clazz extends UDFTrait {
       |   override def getUDF : UserDefinedFunction = udf($code)
       | }
       |
    """.stripMargin

  def eval(UDFName: String, code: String): Unit = {
    val settings = new Settings
    settings.Yreploutdir.value = DefaultClassLocation
    settings.usejavacp.value = true
    val eval = new IMain(settings)
    val objectCode = objectString(UDFName, code)
    logDebug(s"UDF code to be compiled: $objectCode")
    Try {
      eval.compileString(objectCode)
    } match {
      case Success(_) =>
      case Failure(exception) =>
        logError("Failed to compile UDF code")
        throw exception
    }
    eval.close()
  }

}
