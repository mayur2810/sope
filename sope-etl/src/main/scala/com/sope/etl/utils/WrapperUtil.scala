package com.sope.etl.utils

import java.io.File

import com.sope.etl.MainYamlFileOption
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.launcher.SparkLauncher

/**
  * @author mbadgujar
  */
object WrapperUtil {

  val YamlFolder = "yaml_folder"
  val SparkProperties = "spark_properties"

  def getFiles(directory: String): List[File] = {
    val dir = new File(directory)
    if (dir.exists && dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toList
    } else
      Nil
  }

  def main(args: Array[String]): Unit = {

    val options = new Options()
      .addOption(YamlFolder, true, "Folder containing Sope YAML files")
      .addOption(MainYamlFileOption, true, "Main yaml file name")
      .addOption(SparkProperties, true, "Spark Properties")

    val cmdLine = new BasicParser().parse(options, args, false)
    val optionMap = cmdLine.getOptions.map(option => option.getOpt.trim -> option.getValue.trim).toMap
    println(optionMap)
    val yamlFolder = optionMap(YamlFolder)
    val yamlFiles = getFiles(yamlFolder).map(_.getAbsoluteFile)
    optionMap.filter { case (k, _) => k == YamlFolder } + ("files" -> yamlFiles.mkString(","))
  }

}
