package com.sope.common.yaml

import com.fasterxml.jackson.databind.{Module, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.dataformat.yaml.snakeyaml.{DumperOptions, Yaml}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.sope.common.transform.exception.TransformException

import scala.collection.JavaConverters._
import scala.io.Source

/**
  * Parsing Utility for YAMl file
  *
  * @author mbadgujar
  */
object YamlParserUtil {

  private val mapper = new ObjectMapper(new YAMLFactory())
  private val yamlOptions = new DumperOptions
  yamlOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW)
  private val yaml = new Yaml(yamlOptions)

  /**
    * Parses the yaml string to provided class T
    *
    * @param yamlStr Yaml String
    * @param clazz   class to serialize to
    * @tparam T Class type
    * @return object of class T
    */
  def parseYAML[T](yamlStr: String, clazz: Class[T]): T = {
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(yamlStr, clazz)
  }

  /**
   * Parses the yaml string to provided class T. Uses the provided module for Parsing
   *
   * @param yamlStr Yaml String
   * @param clazz  class to serialize to
   * @param moduleToRegister Jackson module
   * @tparam T Class type
   * @return object of class T
   */
  def parseYAML[T](yamlStr: String, clazz: Class[T], moduleToRegister: Module): T = {
    mapper.registerModule(moduleToRegister)
    parseYAML(yamlStr, clazz)
  }

  /**
    * Reads the Yaml file to String
    *
    * @param yamlFile Yaml file that should be in Classpath
    * @return String
    */
  def readYamlFile(yamlFile: String): String = {
    val fileURL = this.getClass.getClassLoader.getResource(s"./$yamlFile")
    val yamlFilePath = Option(fileURL)
      .fold(throw new TransformException(s"Yaml File $yamlFile not found in driver classpath")) {
        url => url.getPath
      }
    val file = Source.fromFile(yamlFilePath)
    try {
      file.getLines.mkString("\n")
    } finally {
      file.close()
    }
  }

  /*
      Converts Scala collection types to Java type
   */
  private def mapVal(any: Any): Any = any match {
    case map: Map[_, _] => map.mapValues(mapVal).asJava
    case seq: Seq[_] => seq.map(mapVal).asJava
    case _ => any
  }

  /**
    * Convert provided object to YAML using SnakeYaml dumper
    *
    * @param obj object
    * @return YAML string
    */
  def convertToYaml(obj: Any): String = {
    obj match {
      case str: String => str.trim
      case _ => yaml.dump(mapVal(obj))
    }
  }

  /**
    * Convert provided object to YAML using Jackson Mapper
    *
    * @param obj object
    * @return Yaml String
    */
  def convertToYaml2(obj: Any): String = mapper.writeValueAsString(obj)

}
