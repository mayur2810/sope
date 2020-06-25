package com.sope.common.utils

import java.io.File

import com.sope.etl._

import scala.collection.mutable

/**
  * Sope Spark Wrapper Utility. The purpose of the utility is to generate the appropriate Spark-submit
  * settings for adding Yaml files to Driver classpath. THe Utility accepts the yaml folder containing the
  * yaml file and based on the cluster mode, it creates the respective spark settings.
  *
  * @author mbadgujar
  */
object WrapperUtil {

  private val YamlFolders = "yaml_folders"
  private val ClusterMode = "cluster_mode"
  private val CustomUDFSClass = "custom_udfs_class"
  private val CustomTransformationsClass = "custom_transformations_class"
  private val AutoPersistMode = "auto_persist"
  private val TestingMode = "testing_mode"
  private val TestingDataFraction = "testing_data_fraction"


  // Configuration Model
  case class WrapperConfig(yamlFolders: Seq[String] = Nil,
                           mainYamlFile: String = "",
                           isClusterMode: Boolean = false,
                           substitutionsFromCmdLine: String = "",
                           substitutionsFromFiles: String = "",
                           customUDFsClass: String = "",
                           customTransformationsClass: String = "",
                           autoPersistMode: Boolean = true,
                           testingMode: Boolean = false,
                           testingDataFraction: Double = 0.0)

  // Gets files from the given directory
  // TODO : Handle prefixes. HDFS, files etc
  private def getFiles(directory: String): List[File] = {
    val dir = new File(directory)
    if (dir.exists && dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toList
    } else throw new Exception(s"Yaml Folder $directory not found")
  }

  /*
     Custom Option Parser. The parser get the unknown options and instead of printing warnings, collects them
     internally.
   */
  class WrapperConfigParser extends scopt.OptionParser[WrapperConfig]("Sope-ETL Spark Wrapper") {
    val UnknownOptionMsg = "Unknown option"
    val UnknownArgMsg = "Unknown argument"
    private val unknownOptions = mutable.MutableList[String]()

    head("Sope-ETL")

    opt[String](ClusterMode)
      .required()
      .action((value, config) => config.copy(isClusterMode = value.toBoolean))
      .text("Whether the job is to deployed in Cluster mode (true/false). " +
        "\n Note: You do not need to provide the 'deploy-mode' spark option, it is provided internally by the wrapper")

    opt[String](YamlFolders)
      .required()
      .action((value, config) => config.copy(yamlFolders = value.split(",").map(_.trim)))
      .text("Comma separated list of folder paths containing all the Sope Yaml Files")

    opt[String](MainYamlFileOption)
      .required()
      .action((value, config) => config.copy(mainYamlFile = value))
      .text("Entry point yaml file")

    opt[String](SubstitutionsOption)
      .optional()
      .action((value, config) => config.copy(substitutionsFromCmdLine = s"--$SubstitutionsOption=$value"))
      .text("(Optional) Yaml Map containing key, value pairs for substitutions")

    opt[String](SubstitutionFilesOption)
      .optional()
      .action((value, config) => config.copy(substitutionsFromFiles = s"--$SubstitutionFilesOption=$value"))
      .text("(Optional) Comma separated list of Substitution files")

    opt[String](CustomUDFSClass)
      .optional()
      .action((value, config) => config.copy(customUDFsClass = value))
      .text("(Optional) Fully qualified class name containing custom UDFs")

    opt[String](CustomTransformationsClass)
      .optional()
      .action((value, config) => config.copy(customTransformationsClass = value))
      .text("(Optional) Fully qualified class name containing custom Transformations")

    opt[String](AutoPersistMode)
      .optional()
      .action((value, config) => config.copy(autoPersistMode = value.toBoolean))
      .text("(Optional) Enable/Disable Auto Persist Mode (true/false)")

    opt[String](TestingMode)
      .optional()
      .action((value, config) => config.copy(testingMode = value.toBoolean))
      .text("(Optional) Enable/Disable Testing Mode (true/false)")

    opt[String](TestingDataFraction)
      .optional()
      .action((value, config) => config.copy(testingDataFraction = value.toDouble))
      .text("(Optional) Testing data fraction, if Testing mode is enabled")


    help("help").text("help menu")

    override def showUsageOnError: Boolean = true

    override def errorOnUnknownArgument: Boolean = false

    //TODO This is bad hack for now. Need to check alternative approach
    override def reportWarning(msg: String): Unit = {
      msg.trim match {
        case option if option.startsWith(UnknownOptionMsg) =>
          unknownOptions += option.replace(UnknownOptionMsg, "").trim
        case arg if arg.startsWith(UnknownArgMsg) =>
          unknownOptions += arg.replace(UnknownArgMsg, "").replace("'", "").trim
        case other => unknownOptions += other
      }
    }

    def getUnknownOptions: Seq[String] = unknownOptions
  }

  def main(args: Array[String]): Unit = {

    val delimiter = "|"
    val parser = new WrapperConfigParser()

    // Gets the passed options and prints the output to console, which is processed by Bash script.
    parser.parse(args, WrapperConfig()) match {
      case Some(config) =>
        val yamlFiles = config.yamlFolders.flatMap(getFiles).map(_.getAbsolutePath)

        val sopeJavaOptions =
          (if (config.customUDFsClass.isEmpty) Nil else Nil :+ s"-D$UDFRegistrationClassProperty=${config.customUDFsClass}") ++
            (if (config.customTransformationsClass.isEmpty) Nil else
              Nil :+ s"-D$TransformationRegistrationClassProperty=${config.customTransformationsClass}") ++
            (if (!config.testingMode) Nil else Nil :+ s"-D$TestingModeProperty=${config.testingMode}") ++
            (if (config.testingDataFraction == 0.0) Nil else Nil :+ s"-D$TestingDataFractionProperty=${config.testingDataFraction}") :+
            s"-D$AutoPersistProperty=${config.autoPersistMode}"

        // Spark Configurations. Also contains sope configurations passed as java options to driver
        val sparkProps = {
          if (config.isClusterMode)
            Seq("--deploy-mode=cluster", s"--files=${yamlFiles.mkString(",")}")
          else
            Seq("--deploy-mode=client", s"--driver-class-path=${config.yamlFolders.mkString(File.pathSeparator)}")
        } ++ parser.getUnknownOptions :+ s"--driver-java-options=${sopeJavaOptions.mkString(" ")}"

        // Sope Configurations passed as command line options
        val sopeProps = Seq(s"--$MainYamlFileOption=${config.mainYamlFile}", config.substitutionsFromCmdLine,
          config.substitutionsFromFiles).filterNot(_.isEmpty)

        println(s"${sparkProps.mkString(delimiter)}\n${sopeProps.mkString(delimiter)}")
      case None =>
        throw new Exception("Invalid options provided")
    }
  }
}
