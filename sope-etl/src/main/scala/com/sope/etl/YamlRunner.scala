package com.sope.etl

import com.sope.etl.yaml.YamlFile.End2EndYaml
import com.sope.etl.yaml.YamlParserUtil.parseYAML
import com.sope.utils.Logging
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Helper for Running YAML Transformation
  *
  * @author mbadgujar
  */
object YamlRunner extends Logging {

  private val options = new Options()
    .addOption(MainYamlFileOption, true, "Main yaml file name")
    .addOption(buildOptionalCmdLineOption(MainYamlFileSubstitutionsOption))

  def main(args: Array[String]): Unit = {
    val cmdLine = new BasicParser().parse(options, args, true)
    val optionMap = cmdLine.getOptions.map(option => option.getOpt.trim -> option.getValue.trim).toMap
    val mainYamlFile = optionMap(MainYamlFileOption)
    logInfo(s"Parsing Main Yaml File: $mainYamlFile")
    val substitutions = optionMap.get(MainYamlFileSubstitutionsOption).map(parseYAML(_, classOf[Seq[Any]]))
    logInfo(s"Substitutions provided : ${substitutions.mkString(",")}")
    val end2endYaml = End2EndYaml(mainYamlFile, substitutions)
    logInfo("Successfully parsed YAML File")
    logInfo("Initializing Spark context & executing the flow..")
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    end2endYaml.performTransformations(sqlContext)
  }

}
