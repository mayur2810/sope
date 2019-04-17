package com.sope.etl.yaml

import com.sope.etl._
import com.sope.etl.transform.Transformer
import com.sope.etl.transform.exception.YamlDataTransformException
import com.sope.etl.transform.model.TransformModelWithoutSourceTarget
import org.apache.spark.sql.DataFrame

/**
  * Intermediate Mode YAML. The sources are logical aliases which are passed dynamically.
  * Also, It does not contain any output targets
  *
  * @param yamlPath      yaml file path
  * @param substitutions Substitutions if any
  * @author mbadgujar
  */
case class IntermediateYaml(yamlPath: String, substitutions: Option[Seq[Any]] = None)
  extends YamlFile(yamlPath, substitutions, classOf[TransformModelWithoutSourceTarget]) {

  /**
    * Perform transformation on provided dataframes.
    * The sources provided in YAML file should be equal and in-order to the provided dataframes
    *
    * @return Transformed [[DataFrame]]
    */
  def getTransformedDFs(dataFrames: DataFrame*): Seq[(String, DataFrame)] = {
    if (model.sources.size != dataFrames.size)
      throw new YamlDataTransformException("Invalid Dataframes provided or incorrect yaml config")
    performRegistrations(dataFrames.head.sqlContext)
    val sourceDFMap = model.sources.zip(dataFrames).map {
      case (source, df) => (source, {
        df.createOrReplaceTempView(source)
        df.alias(source)
      })
    }
    new Transformer(getYamlFileName, sourceDFMap.toMap, model).transform
  }
}