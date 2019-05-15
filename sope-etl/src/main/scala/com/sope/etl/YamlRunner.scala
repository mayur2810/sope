package com.sope.etl

import com.sope.etl.register.UDFBuilder
import com.sope.etl.yaml.YamlParserUtil.parseYAML
import com.sope.etl.yaml.{End2EndYaml, MapYaml}
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
    .addOption(buildOptionalCmdLineOption(MainYamlFileSubstitutionFilesOption))

  def main(args: Array[String]): Unit = {
    val cmdLine = new BasicParser().parse(options, args, true)
    val optionMap = cmdLine.getOptions.map(option => option.getOpt.trim -> option.getValue.trim).toMap
    val mainYamlFile = optionMap(MainYamlFileOption)
    logInfo(s"Parsing Main Yaml File: $mainYamlFile")
    // Get Substitutions from cmd line
    val substitutionsFromCmdLine = optionMap.get(MainYamlFileSubstitutionsOption)
      .map(parseYAML(_, classOf[Map[String, Any]]))
    // Get Substitutions from files
    val substitutionsFromFiles = optionMap.get(MainYamlFileSubstitutionFilesOption)
      .fold(Map[String, Any]()) { _.split(",").map(new MapYaml[String, Any](_).getMap).reduce(_ ++ _).toMap}
    // Merge Substitutions
    val substitutions = substitutionsFromCmdLine.getOrElse(Map.empty) ++ substitutionsFromFiles
    logInfo(s"Substitutions provided :- \n${substitutions.mkString("\n")}")
    val end2endYaml = End2EndYaml(mainYamlFile, Option(substitutions))
    val sparkConf = if (end2endYaml.dynamicUDFDefined) {
      val sparkConf = new SparkConf()
      val updatedJarList = sparkConf.get("spark.jars", "")
        .split(",")
        .filterNot(_.isEmpty)
        .toSeq :+ UDFBuilder.DefaultJarLocation
      sparkConf.set("spark.jars", updatedJarList.mkString(","))
    } else new SparkConf()
    logInfo("Initializing Spark context & executing the flow..")
    val session = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    end2endYaml.performTransformations(session.sqlContext)
  }

}
