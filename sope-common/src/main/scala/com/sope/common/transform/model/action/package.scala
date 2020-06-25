package com.sope.common.transform.model

import com.fasterxml.jackson.annotation.{JsonProperty, JsonTypeInfo}
import com.sope.common.sql.Types.TFunc
import com.sope.common.utils.Logging

/**
 * Package contains YAML Transformer Action (transform) construct mappings and definitions
 *
 * @author mbadgujar
 */
package object action {

  /*
   ETL Actions
 */
  object Actions {
    final val Rename = "rename"
    final val RenameAll = "rename_all"
    final val RenameFindReplace = "rename_replace"
    final val Filter = "filter"
    final val Join = "join"
    final val GroupBy = "group_by"
    final val Aggregate = "aggregate"
    final val Transform = "transform"
    final val TransformAll = "transform_all"
    final val Select = "select"
    final val SelectRegex = "select_regex"
    final val SelectAlias = "select_alias"
    final val SelectReorder = "select_reorder"
    final val SelectNot = "select_not"
    final val Distinct = "distinct"
    final val Limit = "limit"
    final val Union = "union"
    final val Intersect = "intersect"
    final val Except = "except"
    final val Sequence = "sequence"
    final val SCD = "scd"
    final val OrderBy = "order_by"
    final val DropDuplicates = "drop_duplicates"
    final val DropColumn = "drop"
    final val Unstruct = "unstruct"
    final val NA = "na"
    final val Yaml = "yaml"
    final val Named = "named_transform"
    final val DQCheck = "dq_check"
    final val Watermark = "watermark"
    final val Partition = "partition"
    final val Router = "router"
    final val Coalesce = "coalesce"
    final val Repartition = "repartition"
    final val Collect = "collect"
  }

  /**
   * Root Class for Transform Action. To be extended by each Actions.
   *
   * @param id Action Id
   */
  @JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
  abstract class TransformActionRoot[D](@JsonProperty(value = "type", required = true) id: String) extends Logging {

    def apply(datasets: D*): Seq[TFunc[D]]

    def inputAliases: Seq[String] = Nil

    def runtimeModifier: TransformActionRoot[D]

  }

  /**
   * Class representing Transformation with single output
   *
   * @param id Action Id
   */
  abstract class SingleOutputTransform[D](id: String) extends TransformActionRoot[D](id) {
    def transformFunction(datasets: D*): TFunc[D]

    override def apply(datasets: D*): Seq[TFunc[D]] = transformFunction(datasets: _*) +: Nil

    def runtimeModifier: TransformActionRoot[D] = this
  }


  /**
   * Class representing Transformation with multiple output
   *
   * @param id Action Id
   */
  abstract class MultiOutputTransform[D](id: String) extends TransformActionRoot[D](id) {

    def transformFunctions(datasets: D*): Seq[TFunc[D]]

    override def apply(datasets: D*): Seq[TFunc[D]] = transformFunctions(datasets: _*)

    def runtimeModifier: TransformActionRoot[D] = this
  }



}
