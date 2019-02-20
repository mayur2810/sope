package com.sope.etl

import com.sope.etl.register.ScalaScriptEngine
import com.sope.etl.yaml.End2EndYaml
import com.sope.etl.yaml.YamlParserUtil.parseYAML
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
    val sparkConf = if (end2endYaml.registerTempClassPath) {
      logInfo(s"Setting driver classpath for dynamic udfs: ${ScalaScriptEngine.DefaultClassLocation}")
      new SparkConf()
        .set("spark.driver.extraClassPath", ScalaScriptEngine.DefaultClassLocation)
        .set("spark.files", s"${ScalaScriptEngine.DefaultClassLocation}/*")
    } else new SparkConf()
    logInfo("Successfully parsed YAML File")
    logDebug(s"Parsed YAML file :-\n${end2endYaml.getText}")
    logInfo("Initializing Spark context & executing the flow..")
    val session = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    end2endYaml.performTransformations(session.sqlContext)
  }

}
