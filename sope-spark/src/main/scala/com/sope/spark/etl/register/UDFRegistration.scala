package com.sope.spark.etl.register

import com.sope.spark.etl.{SopeETLConfig, getClassInstance}
import com.sope.utils.Logging
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

object UDFRegistration extends Logging {

  /*
   Registers custom udf from provided class
  */
  def registerCustomUDFs(sqlContext: SQLContext): Unit = {
    SopeETLConfig.UDFRegistrationConfig match {
      case Some(classStr) =>
        logInfo(s"Registering custom UDFs from $classStr")
        getClassInstance[UDFRegistration](classStr) match {
          case Some(udfClass) =>
            udfClass.performRegistration(sqlContext)
            logInfo("Successfully registered custom UDFs")
          case _ => logError(s"UDF Registration failed")
        }
      case None => logInfo("No class defined for registering Custom udfs")
    }
  }
}
