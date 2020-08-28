package com.sope.spark.transform.model

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.sope.common.transform.model.MListDeserializer._
import com.sope.common.transform.model.io.input.SourceTypeRoot
import com.sope.common.transform.model.io.output.TargetTypeRoot
import com.sope.common.transform.model.{MList, TransformModel, Transformation}
import com.sope.spark.utils.SQLChecker
import org.apache.spark.sql.{DataFrame, SQLContext}

case class SparkTransformModelWithoutTarget(@JsonProperty(required = true, value = "inputs") vSources: Seq[String],
                                            @JsonDeserialize(using = classOf[TransformationDeserializer[DataFrame]])
                                            @JsonProperty(required = true) transformations: MList[Transformation[DataFrame]])
  extends TransformModel[DataFrame] {

  override def sources: MList[String] = MList(vSources)

  override def targets: MList[TargetTypeRoot[DataFrame]] = MList(Nil)

  checkFailure()

  override def sqlValidationFn(sqlExpr: Any, isSQL: Boolean): Option[String] =
    SQLChecker.validateSQLExpression(sqlExpr, isSQL)
}