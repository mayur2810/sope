package com.sope.etl.yaml

import com.fasterxml.jackson.databind.JsonMappingException
import com.sope.etl._
import com.sope.etl.register._
import com.sope.etl.yaml.YamlParserUtil._
import com.sope.spark.sql.udfs._
import com.sope.utils.Logging
import org.apache.spark.sql.SQLContext

import scala.util.{Failure, Success, Try}

/**
  * A wrapper class with utilities around Yaml file
  *
  * @author mbadgujar
  */
abstract class YamlFile[T](yamlPath: String, substitutions: Option[Seq[Any]] = None, modelClass: Class[T])
  extends Logging {

  protected val model: T = serialize

  /*
     Updates Placeholders with values provided for Substitution
   */
  private def updatePlaceHolders(): String = {
    substitutions.get
      .zipWithIndex
      .map { case (value, index) => "$" + (index + 1) -> convertToYaml(value) }
      .foldLeft(readYamlFile(yamlPath)) { case (yamlStr, (key, value)) => yamlStr.replace(key, value) }
  }

  /*
    Gets Parse error message
   */
  private def getParseErrorMessage(errorLine: Int, errorColumn: Int): String = {
    val lines = getText.split("\\R+").zipWithIndex
    val errorLocation = lines.filter(_._2 == errorLine - 1)
    s"Encountered issue while parsing Yaml File : $getYamlFileName. Error Line No. : $errorLine:$errorColumn\n" +
      errorLocation(0)._1 + s"\n${(1 until errorColumn).map(_ => " ").mkString("")}^"
  }

  /*
     Registers custom udf from provided class
  */
  private def registerCustomUDFs(sqlContext: SQLContext): Unit = {
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

  /*
    Registers Custom Transformation from provided class
  */
  private def registerTransformations(): Unit = {
    SopeETLConfig.TransformationRegistrationConfig match {
      case Some(classStr) =>
        logInfo(s"Registering custom Transformations from $classStr")
        getClassInstance[TransformationRegistration](classStr) match {
          case Some(transformClass) =>
            transformClass.performRegistration()
            logInfo("Successfully registered Custom Transformations")
          case _ => logError(s"Transformation Registration failed")
        }
      case None => logInfo("No class defined for registering Custom Transformations")
    }
  }

  /**
    * Performs Custom UDF & Transformation registrations
    *
    * @param sqlContext Spark's SQL Context
    */
  protected def performRegistrations(sqlContext: SQLContext): Unit = {
    registerUDFs(sqlContext) // Register sope utility udfs
    registerCustomUDFs(sqlContext) // Register custom udfs if provided
    registerTransformations() // Register custom transformations
  }


  /**
    * Get Yaml File Name
    *
    * @return [[String]] file name
    */
  def getYamlFileName: String = yamlPath.split("[\\\\/]").last

  /**
    * Get Yaml Text. Substitutes values if they are provided
    *
    * @return [[String]]
    */
  def getText: String = substitutions.fold(readYamlFile(yamlPath))(_ => updatePlaceHolders())

  /**
    * Serialize the YAML to provided Type
    *
    * @return T
    */
  def serialize: T = Try {
    parseYAML(getText, modelClass)
  } match {
    case Success(t) => t
    case Failure(e) => e match {
      case e: JsonMappingException =>
        Option(e.getLocation) match {
          case Some(location) =>
            val errorMessage = getParseErrorMessage(location.getLineNr, location.getColumnNr)
            logError(errorMessage + s"\n${e.getMessage}")
          case None =>
        }
        throw e
      case _ => throw e
    }
  }

}
