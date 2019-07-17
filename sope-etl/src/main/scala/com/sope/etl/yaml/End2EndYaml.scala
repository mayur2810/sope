package com.sope.etl.yaml

import java.util.Calendar

import com.sope.etl.register.UDFBuilder
import com.sope.etl.transform.Transformer
import com.sope.etl.transform.model.TransformModelWithSourceTarget
import com.sope.etl.{SopeETLConfig, _}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.{Failure, Success, Try}

/**
  * End-to-End model YAML. It should contain source and target information
  *
  * @param yamlPath      yaml file path
  * @param substitutions Substitutions if any
  * @author mbadgujar
  */
case class End2EndYaml(yamlPath: String, substitutions: Option[Map[String, Any]] = None)
  extends YamlFile(yamlPath, substitutions, classOf[TransformModelWithSourceTarget]) {

  /* Add the provided configurations to Spark context */
  private def addConfigurations(sqlContext: SQLContext): Unit = {
    model.configs
      .getOrElse(Map())
      .foreach { case (k, v) => sqlContext.setConf(k, v) }
  }

  // Get the UDF definitions
  private val udfs = model.udfs.getOrElse(Map.empty)
  private val udfFiles = model.udfFiles.getOrElse(Nil)

  /**
    * Flag indicating Dynamic UDFs are provided in the Yaml file
    *
    * @return Boolean
    */
  def dynamicUDFDefined: Boolean = udfs.nonEmpty || udfFiles.nonEmpty

  private val udfMap = if (dynamicUDFDefined) {
    val udfMap = {
      udfFiles
        .map(new MapYaml(_).getMap)
        .fold(Map.empty)(_ ++ _)
    } ++ udfs
    UDFBuilder.buildDynamicUDFs(udfMap)
  } else Map.empty

  /*
     Registers the UDF provided in YAML file
   */
  private def registerDynamicUDFs(sqlContext: SQLContext): Unit = udfMap.foreach {
    case (udfName, udfInst) =>
      logInfo(s"Registering Dynamic UDF :- $udfName")
      sqlContext.udf.register(udfName, udfInst)
  }

  /**
    * Get transformed Dataframe references
    *
    * @param sqlContext Spark's SQL Context
    * @return Map of Alias and Transformed Dataframe
    */
  def getTransformedDFs(sqlContext: SQLContext): Map[String, DataFrame] = {
    addConfigurations(sqlContext)
    performRegistrations(sqlContext)
    registerDynamicUDFs(sqlContext)
    val testingMode = SopeETLConfig.TestingModeConfig
    if (testingMode) logWarning("TESTING MODE IS ENABLED!!")
    val sourceDFMap = model.sources.data
      .map(source => {
        val sourceAlias = source.getSourceName
        val sourceDF = Try {
          source.apply(sqlContext)
        } match {
          case Success(df) => df
          case Failure(exception) =>
            logError(s"Failed to create dataframe from source: ${source.getSourceName}")
            throw exception
        }
        if (testingMode) {
          val fraction = SopeETLConfig.TestingDataFraction
          logWarning(s"Sampling ${fraction * 100} percent data from source: $source")
          sourceAlias -> {
            val sampledDF = sourceDF
              .sample(withReplacement = true, SopeETLConfig.TestingDataFraction)
            sampledDF.createOrReplaceTempView(sourceAlias)
            sampledDF.alias(sourceAlias)
          }
        }
        else
          sourceAlias -> {
            sourceDF.createOrReplaceTempView(sourceAlias)
            sourceDF.alias(sourceAlias)
          }
      }).toMap

    // Apply transformations
    new Transformer(getYamlFileName, sourceDFMap, model).transform.toMap
  }

  /**
    * Performs end to end transformations - Reading sources and writing transformation result to provided targets
    * The source yaml file should contains source and target information.
    *
    * @param sqlContext Spark [[SQLContext]]
    */
  def performTransformations(sqlContext: SQLContext): Unit = {
    val transformations = getTransformedDFs(sqlContext)
    // Write transformed dataframes to output targets
    model.targets.data.foreach(target => {
      logInfo(s"Outputting transformation: ${target.getInput} to target: ${target.getId}")
      logInfo(s"Start time: ${Calendar.getInstance().getTime}")
      target(transformations(target.getInput))
      logInfo(s"End time: ${Calendar.getInstance().getTime}")
    })
  }
}