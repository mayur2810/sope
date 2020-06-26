package com.sope.common.transform.model.action.codegen

import java.io.{BufferedWriter, File, FileWriter}

import com.sope.common.yaml.YamlFile


/**
 * @author mbadgujar
 */
object CoreActionsCodeGen {


  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      throw new IllegalArgumentException("Error: Please provide an argument which provides the source directory " +
        "folder path.")
    }
    val sourceDir = args(0)

    object ActionDef extends YamlFile("action_def.yaml", None, classOf[ActionsDefinitions]) {
      def getDefinitions: ActionsDefinitions = model
    }

    object ModuleDef extends YamlFile("module_def.yaml", None, classOf[ModuleDefinition]) {
      def moduleDefinition: ModuleDefinition = model
    }

    val code = ActionDef.getDefinitions.getCode(ModuleDef.moduleDefinition)
    val file = new File(ModuleDef.moduleDefinition.getFilePath)
    val bw = new BufferedWriter(new FileWriter(sourceDir + "/" + file))
    bw.write(code)
    bw.close()
  }
}
