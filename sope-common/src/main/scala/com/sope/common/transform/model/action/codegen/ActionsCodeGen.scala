package com.sope.common.transform.model.action.codegen

import java.io.{BufferedWriter, File, FileWriter}

import com.sope.common.yaml.YamlFile


/**
 * @author mbadgujar
 */
object ActionsCodeGen {

  case class ActionDef(actionFile: String) extends YamlFile(actionFile, None, classOf[ActionsDefinitions]) {
    def getDefinitions: ActionsDefinitions = model
  }

  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      throw new IllegalArgumentException("Error: Please provide an argument which provides the source directory " +
        "folder path.")
    }
    val sourceDir = args(0)

    val moduleDefinition: ModuleDefinition = new YamlFile("module_def.yaml", None, classOf[ModuleDefinition]) {
      def moduleDefinition: ModuleDefinition = model
    }.moduleDefinition

    val actionConfig = moduleDefinition.actionConfig
    actionConfig.foreach {
      config =>
        val code = ActionDef(config.configFile).getDefinitions.getCode(moduleDefinition, config.name)
        val file = new File(config.getFilePath(moduleDefinition.getPackage))
        val bw = new BufferedWriter(new FileWriter(sourceDir + "/" + file))
        bw.write(code)
        bw.close()
    }
  }
}
