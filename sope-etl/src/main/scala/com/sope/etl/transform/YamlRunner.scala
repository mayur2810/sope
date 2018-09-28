package com.sope.etl.transform

import com.sope.etl.transform.model.YamlFile
import com.sope.utils.Logging
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Helper for Running YAML Transformation
  *
  * @author mbadgujar
  */
object YamlRunner extends Logging {

  private val YamlPathOpt = "yaml_file"
  private val options = new Options().addOption(YamlPathOpt, true, "yaml file path")

  def main(args: Array[String]): Unit = {
    val cmdLine = new BasicParser().parse(options, args, true)
    val optionMap = cmdLine.getOptions.map(option => option.getOpt.trim -> option.getValue.trim).toMap
    val yamlPath =  optionMap(YamlPathOpt)
    val yamlTransformer = new YamlDataTransform(YamlFile(yamlPath))
    logInfo("Successfully parsed YAML File, executing the Flow")
    val sparkConf = new SparkConf().setAppName("Spark: YAML Transformer")
    val session = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    yamlTransformer.performTransformations(session.sqlContext)
  }

}
