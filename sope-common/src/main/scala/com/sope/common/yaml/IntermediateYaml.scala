package com.sope.common.yaml

import com.sope.etl._
import com.sope.common.transform.Transformer
import com.sope.common.transform.exception.TransformException
import com.sope.common.transform.model.TransformModelWithoutSourceTarget
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
  extends YamlFile(yamlPath, substitutions, classOf[TransformModelWithoutSourceTarget]) {

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