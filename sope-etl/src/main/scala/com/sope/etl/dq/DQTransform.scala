package com.sope.etl.dq

import java.io.FileReader

import com.sope.etl.dq.model.DataQuality
import com.sope.spark.sql.dsl._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.DataFrame

/**
  * Performs Data Quality Check
  *
  * @author mbadgujar
  */
class DQTransform(yamlFilePath: String, sourceDF: DataFrame) {

  // Instantiate object mapper object
  private val mapper = new ObjectMapper(new YAMLFactory())
  mapper.registerModule(DefaultScalaModule)

  private def parseYAML: DataQuality = {
    mapper.readValue(new FileReader(yamlFilePath), classOf[DataQuality])
  }

  /**
    * Apply DQ Checks
    *
    * @return DataFrame with DQ status column and error reason column generated for each column
    *         on which DQ check is applied
    */
  def performChecks: DataFrame = {
    val dqModel = parseYAML
    val dqTransform = dqModel.dqChecks.foldLeft(NoOp()) { case (result, dqCheck) => result + dqCheck.apply }
    dqTransform --> sourceDF
  }
}
