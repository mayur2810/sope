package com.sope.spark.yaml

import com.fasterxml.jackson.databind.Module
import com.sope.common.transform.exception.TransformException
import com.sope.common.transform.model.{TransformModelWithoutTarget, TransformationTypeRegistration}
import com.sope.common.yaml.YamlFile
import com.sope.spark.sql.Transformer
import com.sope.spark.etl._
import org.apache.spark.sql.DataFrame

/**
  * Intermediate Mode YAML. The sources are logical aliases which are passed dynamically.
  * Also, It does not contain any output targets
  *
  * @param yamlPath      yaml file path
  * @param substitutions Substitutions if any
  * @author mbadgujar
  */
case class IntermediateYaml(yamlPath: String, substitutions: Option[Map[String, Any]] = None)
  extends YamlFile(yamlPath, substitutions, classOf[TransformModelWithoutTarget[DataFrame]]) {


  /**
   * Get the module to register extensions that are to be used during deserialization
   *
   * @return Optional Module
   */
  override def getModule: Option[Module] = TransformationTypeRegistration.getModule

  /**
    * Perform transformation on provided dataframes.
    * The sources provided in YAML file should be equal and in-order to the provided dataframes
    *
    * @return Transformed [[DataFrame]]
    */
  def getTransformedDFs(dataFrames: DataFrame*): Seq[(String, DataFrame)] = {
    val sources = model.sources.data
    if (sources.size != dataFrames.size)
      throw new TransformException("Invalid Dataframes provided or incorrect yaml config")
    val sqlContext = dataFrames.headOption.getOrElse {
      throw new TransformException("Empty Dataframe List")
    }.sqlContext
    performRegistrations(sqlContext)
    val sourceDFMap = sources.zip(dataFrames).map {
      case (source, df) => (source, {
        df.createOrReplaceTempView(source)
        df.alias(source)
      })
    }
    new Transformer(getYamlFileName, sourceDFMap.toMap, model).transform
  }
}
