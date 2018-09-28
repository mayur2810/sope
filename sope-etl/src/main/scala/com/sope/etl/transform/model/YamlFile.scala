package com.sope.etl.transform.model

import com.sope.etl.transform.YamlParserUtil._

/**
  * A wrapper class with utilities around Yaml file
  *
  * @author mbadgujar
  */
case class YamlFile(yamlPath: String, substitutions: Option[Seq[Any]] = None) {

  private def updatePlaceHolders(): String = {
    substitutions.get
      .zipWithIndex
      .map { case (value, index) => "$" + (index + 1) -> convertToYaml(value) }
      .foldLeft(readYamlFile(yamlPath)) { case (yamlStr, (key, value)) => yamlStr.replace(key, value) }
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
    * Serialize the YAML to provided file
    *
    * @param clazz Class type
    * @tparam T Type T
    * @return T
    */
  def serialize[T](clazz: Class[T]): T = parseYAML(getText, clazz)

}
