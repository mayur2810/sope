package com.mayurb.dwp.transform

import com.mayurb.dwp.transform.model.action._
import com.mayurb.dwp.transform.model.io.{SourceTypeRoot, TargetTypeRoot}
import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}


/**
  *
  * @author mbadgujar
  */
package object model {

  case class DFTransformation(@JsonProperty(required = true) source: String,
                              alias: Option[String],
                              persist: Boolean = false,
                              @JsonProperty(required = true)transform: Seq[_ <: TransformActionRoot]) {
    def getAlias: String = alias.getOrElse(source)
  }

  trait TransformModel

  case class TransformModelWithoutSourceTarget(@JsonProperty(required = true) sources: Seq[String],
                                               @JsonProperty(required = true) transformations: Seq[DFTransformation]) extends TransformModel

  case class TransformModelWithSourceTarget(@JsonProperty(required = true) sources: Seq[_ <: SourceTypeRoot],
                                            @JsonProperty(required = true) transformations: Seq[DFTransformation],
                                            @JsonProperty(required = true) target: TargetTypeRoot) extends TransformModel

}
