package com.sope.common.yaml

import com.fasterxml.jackson.databind.{JsonMappingException, Module}
import com.sope.common.transform.exception.TransformException
import com.sope.common.transform.model.Failed
import com.sope.common.utils.{Logging, RedactUtil}
import com.sope.common.yaml.YamlParserUtil._

import scala.util.{Failure, Success, Try}

/**
  * A wrapper class with utilities around Yaml file
  *
  * @author mbadgujar
  */
abstract class YamlFile[T](yamlPath: String, substitutions: Option[Map[String, Any]] = None, modelClass: Class[T])
  extends Logging {

  private val text: String = getText
  protected val redactedText: String = RedactUtil.redact(text)
  protected val model: T = deserialize

  /**
   * Get the module to register extensions that are to be used during deserialization
   * @return Optional Module
   */
  def getModule: Option[Module] = None

  /*
     Updates Placeholders with values provided for Substitution
   */
  private def updatePlaceHolders(): String = {
    substitutions.get
      .map { case (placeholder, substitution) => "\\$\\{" + placeholder.trim + "\\}" -> convertToYaml(substitution).trim }
      .foldLeft(readYamlFile(yamlPath)) { case (yamlStr, (key, value)) => yamlStr.replaceAll(key, value)
      }
  }

  /*
    Gets Parse error message
   */
  private def getParseErrorMessage(errorLine: Int, errorColumn: Int): String = {
    val lines = redactedText.split("\\R+").zipWithIndex
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
  private def getText: String = substitutions.fold(readYamlFile(yamlPath))(_ => updatePlaceHolders())

  private def logFailures(exception: Throwable): Throwable = {
    exception match {
      case e: JsonMappingException =>
        Option(e.getLocation) match {
          case Some(location) =>
            val errorMessage = getParseErrorMessage(location.getLineNr, location.getColumnNr)
            logError(errorMessage + s"\n${e.getMessage}")
          case None => e.getCause match {
            case TransformException(_, failures) =>
              failures.foreach {
                case Failed(msg, line, index) =>
                  val errorMessage = getParseErrorMessage(line, index)
                  logError(errorMessage + s"\n$msg")
              }
          }
        }
      case TransformException(_, failures) =>
        failures.foreach {
          case Failed(msg, line, index) =>
            val errorMessage = getParseErrorMessage(line, index)
            logError(errorMessage + s"\n$msg")
        }
      case _ =>
    }
    exception
  }

  /**
   * Deserialize the YAML to provided Type
   *
   * @return T
   */
  def deserialize: T = Try {
    val yamlStr = text
    logInfo(s"Parsing $getYamlFileName YAML file :-\n $redactedText")
    getModule.fold(parseYAML(yamlStr, modelClass)) {
      module => parseYAML(yamlStr, modelClass, module)
    }
  } match {
    case Success(obj) =>
      logInfo(s"Successfully parsed $getYamlFileName YAML File")
      obj
    case Failure(e) => throw logFailures(e)
  }

}
