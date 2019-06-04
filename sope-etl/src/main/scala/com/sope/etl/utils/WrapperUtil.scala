package com.sope.etl.utils

import java.io.File

import com.sope.etl.MainYamlFileOption
import com.sope.etl.buildRequiredCmdLineOption
import org.apache.commons.cli.{BasicParser, Options}

/**
  * @author mbadgujar
  */
object WrapperUtil {

  val YamlFolder = "yaml_folder"
  val SparkProperties = "spark_property_file"
  val ClusterMode = "cluster_mode"

  def getFiles(directory: String): List[File] = {
    val dir = new File(directory)
    if (dir.exists && dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toList
    } else
      Nil
  }

  def main(args: Array[String]): Unit = {

    val options = new Options()
      .addOption(buildRequiredCmdLineOption(YamlFolder))
      .addOption(buildRequiredCmdLineOption(MainYamlFileOption))
      .addOption(buildRequiredCmdLineOption(SparkProperties))
      .addOption(buildRequiredCmdLineOption(ClusterMode))

    val cmdLine = new BasicParser().parse(options, args, true)
    val optionMap = cmdLine.getOptions.map(option => option.getOpt.trim -> option.getValue.trim).toMap
    val yamlFolder = optionMap(YamlFolder)
    val yamlFiles = getFiles(yamlFolder).map(_.getAbsoluteFile)
    val isClusterMode = optionMap(ClusterMode).toBoolean

    val sparkProps = if (isClusterMode) {
      "--deploy-mode cluster --files" + s""""${yamlFiles.mkString(",")}""""
    } else {
      "--deploy-mode client  --driver-class-path=" + s""""${optionMap(YamlFolder)}""""
    }
    val sopeProps = s"--$MainYamlFileOption ${optionMap(MainYamlFileOption)}"
    println(s"$sparkProps\n$sopeProps")
  }

}
