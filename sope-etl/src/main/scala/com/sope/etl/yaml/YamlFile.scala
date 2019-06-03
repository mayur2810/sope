package com.sope.etl.yaml

import com.fasterxml.jackson.databind.JsonMappingException
import com.sope.etl.yaml.YamlParserUtil._
import com.sope.utils.Logging

import scala.util.{Failure, Success, Try}

/**
  * A wrapper class with utilities around Yaml file
  *
  * @author mbadgujar
  */
abstract class YamlFile[T](yamlPath: String, substitutions: Option[Map[String, Any]] = None, modelClass: Class[T])
  extends Logging {

  protected val model: T = deserialize

  /*
     Updates Placeholders with values provided for Substitution
   */
  private def updatePlaceHolders(): String = {
    substitutions.get
      .map { case (placeholder, substitution) => "\"*\\s*\\$\\{" + placeholder.trim + "\\}\\s*\"*" -> convertToYaml(substitution).trim }
      .foldLeft(readYamlFile(yamlPath)) { case (yamlStr, (key, value)) => yamlStr.replaceAll(key, s" $value")
      }
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
    * Deserialize the YAML to provided Type
    *
    * @return T
    */
  def deserialize: T = Try {
    val yamlStr = getText
    logInfo(s"Parsing $getYamlFileName YAML file :-\n $yamlStr")
    parseYAML(yamlStr, modelClass)
  } match {
    case Success(t) => logInfo(s"Successfully parsed $getYamlFileName YAML File"); t
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
