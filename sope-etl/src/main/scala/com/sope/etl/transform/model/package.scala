package com.sope.etl.transform

import com.fasterxml.jackson.annotation.JsonProperty
import com.sope.etl.transform.exception.YamlDataTransformException
import com.sope.etl.transform.model.action._
import com.sope.etl.transform.model.io.input.SourceTypeRoot
import com.sope.etl.transform.model.io.output.TargetTypeRoot


/**
  * Package contains YAML Transformer Root construct mappings and definitions
  *
  * @author mbadgujar
  */
package object model {

  /**
    * Base Trait for Transformation Model
    */
  trait TransformModel {
    /**
      * Get the sources involved
      *
      * @return Seq[_]
      */
    def sources: Seq[_]

    /**
      * Transformations list
      *
      * @return Seq[DFTransformation]
      */
    def transformations: Seq[DFTransformation]

    /**
      * Output Targets
      *
      * @return Seq[TargetTypeRoot]
      */
    def targets: Seq[TargetTypeRoot]
  }

  /**
    * Class represents a transformation entity.
    *
    * @param source       input source name
    * @param alias        alias for the transformation
    * @param persistLevel Persistence level for this transformation
    * @param coalesce     Coalesce partitions
    * @param description  Description for this transformation
    * @param actions      Actions to performed on source. Either 'actions' or 'sql' should be provided
    * @param sql          Transformation provided as sql query. Either 'sql' or 'actions' should be provided
    */
  case class DFTransformation(@JsonProperty(required = true, value = "input") source: String,
                              alias: Option[String],
                              @JsonProperty(value = "persist") persistLevel: Option[String],
                              coalesce: Int,
                              description: Option[String],
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

  // Model for YAML without source target information
  case class TransformModelWithoutSourceTarget(@JsonProperty(required = true, value = "inputs") sources: Seq[String],
                                               @JsonProperty(required = true) transformations: Seq[DFTransformation]) extends TransformModel {

    override def targets: Seq[TargetTypeRoot] = Nil
  }

  // Model for YAML with source target information
  case class TransformModelWithSourceTarget(@JsonProperty(required = true, value = "inputs") sources: Seq[SourceTypeRoot],
                                            @JsonProperty(required = true) transformations: Seq[DFTransformation],
                                            @JsonProperty(required = true, value = "outputs") targets: Seq[TargetTypeRoot],
                                            configs: Option[Map[String, String]]) extends TransformModel

}
