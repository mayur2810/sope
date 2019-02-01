package com.sope.etl.yaml

import com.sope.etl.SopeETLConfig
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
            logError(s"Failed to read data from source: ${source.getSourceName}")
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
          source.getSourceName -> sourceDF.alias(source.getSourceName)
      }).toMap
    val transformationResult = new Transformer(getYamlFileName, sourceDFMap, model).transform.toMap
    model.targets.foreach(target => target(transformationResult(target.getInput)))
  }
}