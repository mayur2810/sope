package com.sope.etl

import com.sope.etl.register.UDFBuilder
import com.sope.etl.yaml.YamlParserUtil.parseYAML
import com.sope.etl.yaml.{End2EndYaml, MapYaml}
import com.sope.utils.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Helper for Running YAML Transformation
  *
  * @author mbadgujar
  */
object YamlRunner extends Logging {

  private case class RunnerConfig(mainYamlFile: String = "",
                          substitutionsFromCmdLine: Map[String, Any] = Map.empty,
                          substitutionsFromFiles: Map[String, Any] = Map.empty)

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[RunnerConfig]("Sope-ETL Yaml Runner") {
      head("Sope-ETL")

      opt[String](MainYamlFileOption)
        .required()
        .action((value, config) => config.copy(mainYamlFile = value))
        .text("Entry point yaml file")

      opt[String](SubstitutionsOption)
        .optional()
        .action((value, config) => config.copy(substitutionsFromCmdLine = parseYAML(value, classOf[Map[String, Any]])))
        .text("Substitutions")

      opt[String](SubstitutionFilesOption)
        .optional()
        .action((value, config) => config.copy(substitutionsFromFiles =
          value.split(",").map(file => new MapYaml[String, Any](file.trim).getMap).reduce(_ ++ _)))
        .text("Substitutions from Files")

      help("help").text("help menu")

      override def showUsageOnError: Boolean = true
    }

    parser.parse(args, RunnerConfig()) match {
      case Some(config) =>
        val mainYamlFile = config.mainYamlFile
        val substitutions = config.substitutionsFromFiles ++ config.substitutionsFromCmdLine
        logInfo(s"Substitutions provided :- \n${substitutions.mkString("\n")}")
        logInfo(s"Parsing Main Yaml File: $mainYamlFile")
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
      case None =>
        throw new Exception("Invalid options provided")
    }
  }
}
