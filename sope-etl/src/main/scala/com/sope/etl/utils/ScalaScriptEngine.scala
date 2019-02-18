package com.sope.etl.utils

import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain

object ScalaScriptEngine {

  private def classStr(clazz: String, code: String) =
    s"""
       | import com.sope.etl.utils.UDFTrait
       | import org.apache.spark.sql.functions._
       | class $clazz extends UDFTrait {
       |    println(this.getClass.getClassLoader.toString)
       |    def udff  = udf($code)
       | }
    """.stripMargin

  def eval(UDFName: String, code: String): UserDefinedFunction = {
    println("EVALUATING CODE")
    val settings = new Settings
    settings.usejavacp.value = true
    settings.verbose.value = true
    //settings.deprecation.value = true
    settings.embeddedDefaults(this.getClass.getClassLoader)
    val eval = new IMain(settings)
    val gcode = classStr(UDFName, code)
    println(gcode)
    eval.compileString(gcode)
    val classA = eval.classLoader.loadClass(UDFName)
    val inst = classA.newInstance().asInstanceOf[UDFTrait]
    eval.close()
    println(this.getClass.getClassLoader.toString)
    val d = inst.udff
    d
  }

}
