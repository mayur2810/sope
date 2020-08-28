package com.sope.spark.transform.model

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.sope.common.transform.model.MListDeserializer._
import com.sope.common.transform.model.io.input.SourceTypeRoot
import com.sope.common.transform.model.io.output.TargetTypeRoot
import com.sope.common.transform.model.{MList, TransformModel, Transformation}
import com.sope.spark.utils.SQLChecker
import org.apache.spark.sql.{DataFrame, SQLContext}

case class SparkTransformModelWithSourceTarget(@JsonDeserialize(using = classOf[InputDeserializer[SQLContext, DataFrame]])
                               @JsonProperty(required = true, value = "inputs")
                               sources: MList[SourceTypeRoot[SQLContext, DataFrame]],
                                               @JsonDeserialize(using = classOf[TransformationDeserializer[DataFrame]])
                               @JsonProperty(required = true)
                               transformations: MList[Transformation[DataFrame]],
                                               @JsonDeserialize(using = classOf[TargetDeserializer[DataFrame]])
                               @JsonProperty(required = true, value = "outputs")
                               targets: MList[TargetTypeRoot[DataFrame]],
                                               configs: Option[Map[String, String]],
                                               udfs: Option[Map[String, String]],
                                               @JsonProperty(value = "udf_files") udfFiles: Option[Seq[String]])
  extends TransformModel[DataFrame] {

  override def sqlValidationFn(sqlExpr: Any, isSQL: Boolean): Option[String] = SQLChecker.validateSQLExpression(sqlExpr, isSQL)

  checkFailure()
}