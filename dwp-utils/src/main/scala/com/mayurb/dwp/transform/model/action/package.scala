package com.mayurb.dwp.transform.model

import com.mayurb.spark.sql.dsl._
import com.mayurb.spark.sql._
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

/**
  * Package contains YAML Transformer Action (transform) construct mappings and definitions
  *
  * @author mbadgujar
  */
package object action {

  @JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
  @JsonSubTypes(Array(
    new Type(value = classOf[RenameAction], name = "rename"),
    new Type(value = classOf[FilterAction], name = "filter"),
    new Type(value = classOf[JoinAction], name = "join"),
    new Type(value = classOf[GroupAction], name = "group"),
    new Type(value = classOf[TransformAction], name = "transform"),
    new Type(value = classOf[RenameAction], name = "rename"),
    new Type(value = classOf[SelectAction], name = "select"),
    new Type(value = classOf[SequenceAction], name = "sequence")
  ))
  abstract class TransformActionRoot(@JsonProperty(value = "type", required = true) id: String) {
    def apply(dataframes: DataFrame*): DFFunc
  }


  case class RenameAction(@JsonProperty(required = true) list: Map[String, String]) extends TransformActionRoot("rename") {
    override def apply(dataframes: DataFrame*): DFFunc = Rename(list.toSeq: _*)
  }


  case class TransformAction(@JsonProperty(required = true) list: Map[String, String]) extends TransformActionRoot("transform") {
    override def apply(dataframes: DataFrame*): DFFunc = Transform(list.toSeq: _*)
  }


  case class JoinAction(@JsonProperty(value = "condition", required = false) joinCondition: String,
                        @JsonProperty(value = "columns", required = false) joinColumns: Seq[String],
                        @JsonProperty(value = "join_type", required = true) joinType: String,
                        @JsonProperty(value = "with", required = true) joinSource: String,
                        @JsonProperty(value = "broadcast_hint") broadcastHint: String) extends TransformActionRoot("join") {

    private val joinTypeFunc: (DFJoinFunc) => DataFrame => DFFunc = (joinFunc: DFJoinFunc) => joinType match {
      case "inner" => joinFunc >< _
      case "left" => joinFunc << _
      case "right" => joinFunc >> _
      case "full" => joinFunc <> _
    }

    override def apply(dataframes: DataFrame*): DFFunc = {
      if (joinCondition == null && joinColumns == null) throw new Exception("Please provide either join 'condition' or join 'columns' option")
      if (joinCondition != null)
        joinTypeFunc(Join(Option(broadcastHint), expr(joinCondition)))(dataframes.head)
      else
        joinTypeFunc(Join(Option(broadcastHint), joinColumns: _*))(dataframes.head)
    }
  }


  case class GroupAction(@JsonProperty(required = true) groupColumns: Seq[String],
                         @JsonProperty(required = true) groupExpr: String) extends TransformActionRoot("group") {
    override def apply(dataframes: DataFrame*): DFFunc = Group(groupColumns: _*) ^ groupExpr
  }


  case class FilterAction(@JsonProperty(required = true) condition: String) extends TransformActionRoot("filter") {
    override def apply(dataframes: DataFrame*): DFFunc = Filter(condition)
  }


  case class SelectAction(@JsonProperty(required = true) columns: Seq[String]) extends TransformActionRoot("select") {
    override def apply(dataframes: DataFrame*): DFFunc = Select(columns: _*)
  }


  case class SequenceAction(@JsonProperty(value = "sk_source", required = true) skSource: String,
                            @JsonProperty(value = "sk_column", required = true) skColumn: String) extends TransformActionRoot("sequence") {
    override def apply(dataframes: DataFrame*): DFFunc = Sequence(dataframes.head.maxKeyValue(skColumn), skColumn)
  }

}
