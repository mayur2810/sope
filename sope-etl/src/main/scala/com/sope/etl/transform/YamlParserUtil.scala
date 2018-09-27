package com.sope.etl.transform

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.dataformat.yaml.snakeyaml.Yaml
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.JavaConverters._
import scala.io.Source

/**
  * Parsing Utility for YAMl file
  *
  * @author mbadgujar
  */
object YamlParserUtil {

  private val mapper = new ObjectMapper(new YAMLFactory())
  private val yaml = new Yaml()

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
    * Reads the Yaml file to String
    *
    * @param yamlFile Yaml file that should be in Classpath
    * @return String
    */
  def readYamlFile(yamlFile: String): String = {
    val yamlFilePath = this.getClass.getClassLoader.getResource(s"./$yamlFile").getPath
    Source.fromFile(yamlFilePath).getLines.mkString("\n")
  }

  /**
    * Convert provided object to YAML
    *
    * @param obj object
    * @return YAML string
    */
  def convertToYaml(obj: Any): String = {
    obj match {
      case _ :: _ => yaml.dump(obj.asInstanceOf[List[_]].asJava)
      case _: Map[_, _] => yaml.dump(obj.asInstanceOf[Map[_, _]].asJava)
      case _ => yaml.dump(obj)
    }
  }

}
