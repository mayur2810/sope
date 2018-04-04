package com.mayurb.dwp.transform

import com.fasterxml.jackson.annotation.JsonProperty
import com.mayurb.dwp.transform.model.action._
import com.mayurb.dwp.transform.model.io.input.SourceTypeRoot
import com.mayurb.dwp.transform.model.io.output.TargetTypeRoot


/**
  *
  * @author mbadgujar
  */
package object model {

  case class DFTransformation(@JsonProperty(required = true, value = "input") source: String,
                              alias: Option[String],
                              persist: Boolean = false,
                              @JsonProperty(required = true) transform: Seq[_ <: TransformActionRoot]) {
    def getAlias: String = alias.getOrElse(source)
  }

  trait TransformModel

  case class TransformModelWithoutSourceTarget(@JsonProperty(required = true, value = "inputs") sources: Seq[String],
                                               @JsonProperty(required = true) transformations: Seq[DFTransformation]) extends TransformModel

  case class TransformModelWithSourceTarget(@JsonProperty(required = true, value = "inputs") sources: Seq[_ <: SourceTypeRoot],
                                            @JsonProperty(required = true) transformations: Seq[DFTransformation],
                                            @JsonProperty(required = true, value = "outputs") targets: Seq[TargetTypeRoot]) extends TransformModel

}
