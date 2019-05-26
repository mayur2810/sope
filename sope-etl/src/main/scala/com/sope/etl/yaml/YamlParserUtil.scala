package com.sope.etl.yaml

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.dataformat.yaml.snakeyaml.{DumperOptions, Yaml}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.sope.etl.transform.exception.YamlDataTransformException

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
    * Reads the Yaml file to String
    *
    * @param yamlFile Yaml file that should be in Classpath
    * @return String
    */
  def readYamlFile(yamlFile: String): String = {
    val fileURL = this.getClass.getClassLoader.getResource(s"./$yamlFile")
    val yamlFilePath = Option(fileURL)
      .fold(throw new YamlDataTransformException(s"Yaml File $yamlFile not found in driver classpath")) {
        url => url.getPath
      }
    val file = Source.fromFile(yamlFilePath)
    try {
      file.getLines.mkString("\n")
    } finally {
      file.close()
    }
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
      case str: String => "\"" + yaml.dump(str).trim + "\""
      case _ => yaml.dump(obj)
    }
  }

  def convertToYaml2(obj: Any): String = {
    mapper.writeValueAsString(obj)
  }

}
