package com.sope.etl.yaml

import com.sope.utils.Logging
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
  * Yaml file representing Spark SQL Schema
  *
  * @author mbadgujar
  */
case class SchemaYaml(yamlPath: String) extends YamlFile(yamlPath, None, classOf[mutable.LinkedHashMap[String, String]]) with Logging {

  def getSparkSchema: StructType = StructType.fromDDL {
    logInfo(s"Generating Spark Schema from provided configuration: $model")
    model.map { case (k, v) => k + " " + v }.mkString(",")
  }
}