package com.mayurb.dwp.transform

import com.mayurb.utils.Logging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Helper for Running YAML Transformation
  *
  * @author mbadgujar
  */
object YamlRunner extends Logging {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new Exception("Yaml file path required")
    }
    val yamlPath = args(0)
    val yamlTransformer = new YamlDataTransform(yamlPath)
    logInfo("Successfully parsed YAML File, executing the Flow")
    val sparkConf = new SparkConf().setAppName("YAML Transformer")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sparkContext)
    yamlTransformer.performTransformations(sqlContext)
  }

}
