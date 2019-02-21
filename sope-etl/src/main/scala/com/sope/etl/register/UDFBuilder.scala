package com.sope.etl.register

import com.sope.utils.Logging

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain
import scala.util.{Failure, Success, Try}
import com.sope.etl.getObjectInstance
import com.sope.etl.utils.CreateJar
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.expressions.UserDefinedFunction

object UDFBuilder extends Logging {

  val DefaultClassLocation = "/tmp/sope/dynamic"
  val DefaultJarLocation = "/tmp/sope/sope-dynamic-udf.jar"


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

  private def evalUDF(UDFName: String, code: String): UserDefinedFunction = {
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
    val udf = getObjectInstance[UDFTrait](eval.classLoader, "com.sope.etl.dynamic." + UDFName).get.getUDF
    eval.close()
    udf
  }

  def buildDynamicUDFs(udfCodeMap: Map[String, String]): Map[String, UserDefinedFunction] = {
    val file = new java.io.File(UDFBuilder.DefaultClassLocation)
    FileUtils.deleteDirectory(file)
    file.mkdirs()
    val udfMap = udfCodeMap.map { case (udfName, udfCode) => udfName -> UDFBuilder.evalUDF(udfName, udfCode) }
    CreateJar.build(DefaultClassLocation, DefaultJarLocation)
    udfMap
  }

}
