package com.mayurb.dwp.transform

import com.fasterxml.jackson.annotation.JsonProperty
import com.mayurb.dwp.transform.exception.YamlDataTransformException
import com.mayurb.dwp.transform.model.action._
import com.mayurb.dwp.transform.model.io.input.SourceTypeRoot
import com.mayurb.dwp.transform.model.io.output.TargetTypeRoot


/**
  * Package contains YAML Transformer Root construct mappings and definitions
  *
  * @author mbadgujar
  */
package object model {

  /**
    * Class represents a transformation entity.
    *
    * @param source  input source name
    * @param alias   alias for the transformation
    * @param persist whether to persist this transformation
    * @param coalesce Coalesce partitions
    * @param actions Actions to performed on source. Either 'actions' or 'sql' should be provided
    * @param sql     Transformation provided as sql query. Either 'sql' or 'actions' should be provided
    */
  case class DFTransformation(@JsonProperty(required = true, value = "input") source: String,
                              alias: Option[String],
                              persist: Boolean = false,
                              coalesce: Int,
                              actions: Option[Seq[_ <: TransformActionRoot]],
                              sql: Option[String]) {

    // validate options
    if (actions.isDefined && sql.isDefined)
      throw new YamlDataTransformException("Please provide either 'actions' or 'sql' option in transformation construct, not both..")

    if (actions.isEmpty && sql.isEmpty)
      throw new YamlDataTransformException("Please provide either 'actions' or 'sql' option in transformation construct")

    val isSQLTransform: Boolean = sql.isDefined

    /**
      * Get Transformation alias. If not provided, defaults for source name
      *
      * @return alias
      */
    def getAlias: String = alias.getOrElse(source)
  }

  trait TransformModel

  // Model for YAML without source target information
  case class TransformModelWithoutSourceTarget(@JsonProperty(required = true, value = "inputs") sources: Seq[String],
                                               @JsonProperty(required = true) transformations: Seq[DFTransformation]) extends TransformModel
  // Model for YAML with source target information
  case class TransformModelWithSourceTarget(@JsonProperty(required = true, value = "inputs") sources: Seq[_ <: SourceTypeRoot],
                                            @JsonProperty(required = true) transformations: Seq[DFTransformation],
                                            @JsonProperty(required = true, value = "outputs") targets: Seq[TargetTypeRoot]) extends TransformModel

}
