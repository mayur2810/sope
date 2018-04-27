package com.mayurb.dwp.transform.model

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.mayurb.dwp.scd.DimensionTable
import com.mayurb.dwp.transform.exception.YamlDataTransformException
import com.mayurb.spark.sql._
import com.mayurb.spark.sql.dsl._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

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
    final val Filter = "filter"
    final val Join = "join"
    final val GroupBy = "group_by"
    final val Transform = "transform"
    final val Select = "select"
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
  @JsonSubTypes(Array(
    new Type(value = classOf[RenameAction], name = Actions.Rename),
    new Type(value = classOf[FilterAction], name = Actions.Filter),
    new Type(value = classOf[JoinAction], name = Actions.Join),
    new Type(value = classOf[GroupAction], name = Actions.GroupBy),
    new Type(value = classOf[TransformAction], name = Actions.Transform),
    new Type(value = classOf[SelectAction], name = Actions.Select),
    new Type(value = classOf[SelectNotAction], name = Actions.SelectNot),
    new Type(value = classOf[DistinctAction], name = Actions.Distinct),
    new Type(value = classOf[LimitAction], name = Actions.Limit),
    new Type(value = classOf[UnionAction], name = Actions.Union),
    new Type(value = classOf[IntersectAction], name = Actions.Intersect),
    new Type(value = classOf[SequenceAction], name = Actions.Sequence),
    new Type(value = classOf[SCDAction], name = Actions.SCD),
    new Type(value = classOf[OrderByAction], name = Actions.OrderBy),
    new Type(value = classOf[DropDuplicateAction], name = Actions.DropDuplicates),
    new Type(value = classOf[DropColumnAction], name = Actions.DropColumn),
    new Type(value = classOf[UnstructAction], name = Actions.Unstruct)
  ))
  abstract class TransformActionRoot(@JsonProperty(value = "type", required = true) id: String) {
    def apply(dataframes: DataFrame*): DFFunc

    def inputAliases: Seq[String] = Nil
  }


  case class RenameAction(@JsonProperty(required = true) list: Map[String, String]) extends TransformActionRoot(Actions.Rename) {
    override def apply(dataframes: DataFrame*): DFFunc = Rename(list.toSeq: _*)
  }


  case class TransformAction(@JsonProperty(required = true) list: Map[String, String]) extends TransformActionRoot(Actions.Transform) {
    override def apply(dataframes: DataFrame*): DFFunc = Transform(list.toSeq: _*)
  }


  case class JoinAction(@JsonProperty(value = "condition", required = false) joinCondition: String,
                        @JsonProperty(value = "columns", required = false) joinColumns: Seq[String],
                        @JsonProperty(value = "join_type", required = true) joinType: String,
                        @JsonProperty(value = "with", required = true) joinSource: String,
                        @JsonProperty(value = "broadcast_hint") broadcastHint: String) extends TransformActionRoot(Actions.Join) {

    private val joinTypeFunc: (DFJoinFunc) => DataFrame => DFFunc = (joinFunc: DFJoinFunc) => joinType match {
      case "inner" => joinFunc >< _
      case "left" => joinFunc << _
      case "right" => joinFunc >> _
      case "full" => joinFunc <> _
    }

    override def apply(dataframes: DataFrame*): DFFunc = {
      if (joinCondition == null && joinColumns == null)
        throw new YamlDataTransformException("Please provide either 'condition' or 'columns' option in join action definition")

      if (joinCondition != null)
        joinTypeFunc(Join(Option(broadcastHint), expr(joinCondition)))(dataframes.head)
      else
        joinTypeFunc(Join(Option(broadcastHint), joinColumns: _*))(dataframes.head)
    }

    override def inputAliases: Seq[String] = Seq(joinSource)
  }


  case class GroupAction(@JsonProperty(value = "columns", required = true) groupColumns: Seq[String],
                         @JsonProperty(value = "expr", required = true) groupExpr: String)
    extends TransformActionRoot(Actions.GroupBy) {
    override def apply(dataframes: DataFrame*): DFFunc = Group(groupColumns: _*) ^ groupExpr
  }


  case class FilterAction(@JsonProperty(required = true) condition: String)
    extends TransformActionRoot(Actions.Filter) {
    override def apply(dataframes: DataFrame*): DFFunc = Filter(condition)
  }


  case class SelectAction(@JsonProperty(required = true) columns: Seq[String])
    extends TransformActionRoot(Actions.Select) {
    override def apply(dataframes: DataFrame*): DFFunc = Select(columns: _*)
  }

  case class SelectNotAction(@JsonProperty(required = true) columns: Seq[String])
    extends TransformActionRoot(Actions.SelectNot) {
    override def apply(dataframes: DataFrame*): DFFunc = SelectNot(columns: _*)
  }

  case class UnionAction(@JsonProperty(required = true, value = "with") unionWith: Seq[String])
    extends TransformActionRoot(Actions.Union) {
    override def apply(dataframes: DataFrame*): DFFunc = Union(dataframes: _*)

    override def inputAliases: Seq[String] = unionWith
  }

  case class IntersectAction(@JsonProperty(required = true, value = "with") intersectWith: Seq[String])
    extends TransformActionRoot(Actions.Intersect) {
    override def apply(dataframes: DataFrame*): DFFunc = Intersect(dataframes: _*)

    override def inputAliases: Seq[String] = intersectWith
  }

  case class ExceptAction(@JsonProperty(required = true, value = "with") exceptWith: Seq[String])
    extends TransformActionRoot(Actions.Except) {
    override def apply(dataframes: DataFrame*): DFFunc = Except(dataframes: _*)

    override def inputAliases: Seq[String] = exceptWith
  }

  case class OrderByAction(@JsonProperty(required = true) columns: Seq[String])
    extends TransformActionRoot(Actions.OrderBy) {
    override def apply(dataframes: DataFrame*): DFFunc = OrderBy(columns: _*)
  }

  case class LimitAction(@JsonProperty(required = true) size: Int) extends TransformActionRoot(Actions.Limit) {
    override def apply(dataframes: DataFrame*): DFFunc = Limit(size)
  }

  case class DistinctAction() extends TransformActionRoot(Actions.Distinct) {
    override def apply(dataframes: DataFrame*): DFFunc = Distinct.apply
  }

  case class DropDuplicateAction(@JsonProperty(required = true) columns: Seq[String])
    extends TransformActionRoot(Actions.DropDuplicates) {
    override def apply(dataframes: DataFrame*): DFFunc = DropDuplicates(columns: _*)
  }

  case class DropColumnAction(@JsonProperty(required = true) columns: Seq[String])
    extends TransformActionRoot(Actions.DropColumn) {
    override def apply(dataframes: DataFrame*): DFFunc = Drop(columns: _*)
  }

  case class UnstructAction(@JsonProperty(required = true) column: String)
    extends TransformActionRoot(Actions.Unstruct) {
    override def apply(dataframes: DataFrame*): DFFunc = Unstruct(column)
  }

  case class SequenceAction(@JsonProperty(value = "sk_source", required = true) skSource: String,
                            @JsonProperty(value = "sk_column", required = true) skColumn: String)
    extends TransformActionRoot(Actions.Sequence) {
    override def apply(dataframes: DataFrame*): DFFunc = Sequence(dataframes.head.maxKeyValue(skColumn), skColumn)

    override def inputAliases: Seq[String] = Seq(skSource)
  }

  case class SCDAction(@JsonProperty(value = "dim_table", required = true) dimTable: String,
                       @JsonProperty(value = "sk_column", required = true) surrogateKey: String,
                       @JsonProperty(value = "natural_keys", required = true) naturalKeys: Seq[String],
                       @JsonProperty(value = "derived_columns", required = true) derivedColumns: Seq[String],
                       @JsonProperty(value = "meta_columns", required = true) metaColumns: Seq[String],
                       @JsonProperty(value = "incremental_load", required = false) incrementalLoad: Option[Boolean])
    extends TransformActionRoot(Actions.SCD) {
    override def apply(dataframes: DataFrame*): DFFunc = (scdInput: DataFrame) =>
      new DimensionTable(dataframes.head, surrogateKey, naturalKeys, derivedColumns, metaColumns)
        .getDimensionChangeSet(scdInput, incrementalLoad.getOrElse(true)).getUnion

    override def inputAliases: Seq[String] = Seq(dimTable)
  }

}
