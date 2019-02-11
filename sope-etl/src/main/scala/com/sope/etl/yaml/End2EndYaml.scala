package com.sope.etl.yaml

import java.util.Calendar

import com.sope.etl.{SopeETLConfig, _}
import com.sope.etl.transform.Transformer
import com.sope.etl.transform.model.TransformModelWithSourceTarget
import org.apache.spark.sql.SQLContext

import scala.util.{Failure, Success, Try}

/**
  * End-to-End model YAML. It should contain source and target information
  *
  * @param yamlPath      yaml file path
  * @param substitutions Substitutions if any
  * @author mbadgujar
  */
case class End2EndYaml(yamlPath: String, substitutions: Option[Seq[Any]] = None)
  extends YamlFile(yamlPath, substitutions, classOf[TransformModelWithSourceTarget]) {

  /* Add the provided configurations to Spark context */
  private def addConfigurations(sqlContext: SQLContext): Unit = {
    model.configs
      .getOrElse(Map())
      .foreach { case (k, v) => sqlContext.setConf(k, v) }
  }

  /**
    * Performs end to end transformations - Reading sources and writing transformation result to provided targets
    * The source yaml file should contains source and target information.
    *
    * @param sqlContext Spark [[SQLContext]]
    */
  def performTransformations(sqlContext: SQLContext): Unit = {
    addConfigurations(sqlContext)
    performRegistrations(sqlContext)
    val testingMode = SopeETLConfig.TestingModeConfig
    if (testingMode) logWarning("TESTING MODE IS ENABLED!!")
    val sourceDFMap = model.sources
      .map(source => {
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
          source.getSourceName -> sourceDF
            .sample(withReplacement = true, SopeETLConfig.TestingDataFraction)
            .alias(source.getSourceName)
        }
        else
          source.getSourceName -> {
            val sourceAlias = source.getSourceName
            sourceDF.createOrReplaceTempView(sourceAlias)
            sourceDF.alias(sourceAlias)
          }
      }).toMap

    // Apply transformations
    val transformationResult = new Transformer(getYamlFileName, sourceDFMap, model).transform.toMap

    // Write transformed dataframes to output targets
    model.targets.foreach(target => {
      logInfo(s"Outputting transformation: ${target.getInput} to target: ${target.getId}")
      logInfo(s"Start time: ${Calendar.getInstance().getTime}")
      target(transformationResult(target.getInput))
      logInfo(s"End time: ${Calendar.getInstance().getTime}")
    })
  }
}