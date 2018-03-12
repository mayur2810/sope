package com.mayurb.dwp.dq

import com.mayurb.spark.sql.dsl._
import com.mayurb.spark.sql.udfs._
import org.apache.spark.sql.functions._
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.annotation.JsonSubTypes.Type

/**
  * Model Classes for Data Quality Check Functionality
  *
  * @author mbadgujar
  */
package object model {

  case class DataQuality(@JsonProperty(value = "dq-checks", required = true) dqChecks: Seq[Check])

  @JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
  @JsonSubTypes(Array(
    new Type(value = classOf[NullCheck], name = "null-check"),
    new Type(value = classOf[EmptyCheck], name = "empty-check"),
    new Type(value = classOf[DateFormatCheck], name = "date-check"),
    new Type(value = classOf[CustomCheck], name = "custom-check")
  ))
  abstract class Check(@JsonProperty(value = "type", required = true) id: String, columns: Seq[String]) {
    def getUDF: ColFunc

    def apply: DFFunc = Transform(columns.flatMap(column => {
      Seq(s"${column}_${id}_dq" -> getUDF(col(column)),
        s"${column}_${id}_dq_reason" ->
          when(col(s"${column}_${id}_dq") === true,
            concat_ws("", lit(s"failed for value:"), col(column))).otherwise(lit(null)))
    }): _*)
  }

  case class NullCheck(@JsonProperty(required = true) columns: Seq[String]) extends Check("null-check", columns) {
    override def getUDF: ColFunc = isnull
  }

  case class EmptyCheck(@JsonProperty(required = true) columns: Seq[String]) extends Check("empty-check", columns) {
    override def getUDF: ColFunc = checkEmptyUDF(_)
  }

  case class DateFormatCheck(@JsonProperty(required = true) format: String,
                             @JsonProperty(required = true) columns: Seq[String]) extends Check("date-check", columns) {
    override def getUDF: ColFunc = dateFormatCheckUdf(_, lit(format))
  }

  // TODO: currently supports single argument. Support multiple arguments?
  case class CustomCheck(@JsonProperty(required = true) name: String,
                         @JsonProperty(required = true) udf: String,
                         @JsonProperty(required = true) columns: Seq[String]) extends Check(name + "custom-check", columns) {
    override def getUDF: ColFunc = callUDF(udf, _)
  }


}
