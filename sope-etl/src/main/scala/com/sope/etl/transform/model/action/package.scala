package com.sope.etl.transform.model

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.sope.etl.annotations.SqlExpr
import com.sope.etl.register.TransformationRegistration
import com.sope.etl.sqlLiteralExpr
import com.sope.etl.transform.exception.YamlDataTransformException
import com.sope.etl.yaml.{IntermediateYaml, YamlParserUtil}
import com.sope.spark.sql._
import com.sope.spark.sql.dsl.{Select => Select1, _}
import com.sope.utils.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable
import scala.reflect.runtime.universe._

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
    final val TransformMultiArg = "transform_multi_arg"
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
  @JsonSubTypes(Array(
    new Type(value = classOf[RenameAction], name = Actions.Rename),
    new Type(value = classOf[RenameFindReplace], name = Actions.RenameFindReplace),
    new Type(value = classOf[RenameAllAction], name = Actions.RenameAll),
    new Type(value = classOf[FilterAction], name = Actions.Filter),
    new Type(value = classOf[JoinAction], name = Actions.Join),
    new Type(value = classOf[GroupAction], name = Actions.GroupBy),
    new Type(value = classOf[AggregateAction], name = Actions.Aggregate),
    new Type(value = classOf[TransformAction], name = Actions.Transform),
    new Type(value = classOf[TransformAllAction], name = Actions.TransformAll),
    new Type(value = classOf[TransformAllMultiArgAction], name = Actions.TransformMultiArg),
    new Type(value = classOf[SelectAction], name = Actions.Select),
    new Type(value = classOf[SelectWithRegexAction], name = Actions.SelectRegex),
    new Type(value = classOf[SelectWithAliasAction], name = Actions.SelectAlias),
    new Type(value = classOf[SelectWithReorderedAction], name = Actions.SelectReorder),
    new Type(value = classOf[SelectNotAction], name = Actions.SelectNot),
    new Type(value = classOf[DistinctAction], name = Actions.Distinct),
    new Type(value = classOf[LimitAction], name = Actions.Limit),
    new Type(value = classOf[UnionAction], name = Actions.Union),
    new Type(value = classOf[IntersectAction], name = Actions.Intersect),
    new Type(value = classOf[ExceptAction], name = Actions.Except),
    new Type(value = classOf[SequenceAction], name = Actions.Sequence),
    new Type(value = classOf[SCDAction], name = Actions.SCD),
    new Type(value = classOf[OrderByAction], name = Actions.OrderBy),
    new Type(value = classOf[DropDuplicateAction], name = Actions.DropDuplicates),
    new Type(value = classOf[DropColumnAction], name = Actions.DropColumn),
    new Type(value = classOf[UnstructAction], name = Actions.Unstruct),
    new Type(value = classOf[NAAction], name = Actions.NA),
    new Type(value = classOf[YamlAction], name = Actions.Yaml),
    new Type(value = classOf[NamedAction], name = Actions.Named),
    new Type(value = classOf[DQCheckAction], name = Actions.DQCheck),
    new Type(value = classOf[WatermarkAction], name = Actions.Watermark),
    new Type(value = classOf[PartitionAction], name = Actions.Partition),
    new Type(value = classOf[RouterAction], name = Actions.Router),
    new Type(value = classOf[CoalesceAction], name = Actions.Coalesce),
    new Type(value = classOf[RepartitionAction], name = Actions.Repartition),
    new Type(value = classOf[CollectAction], name = Actions.Collect)
  ))
  abstract class TransformActionRoot(@JsonProperty(value = "type", required = true) id: String) extends Logging {

    def apply(dataframes: DataFrame*): Seq[DFFunc]

    def inputAliases: Seq[String] = Nil

    /**
     * Modifies any SQL expression which have placeholders at Runtime.
     * Internally converts to Yaml and reconstructs the action instance
     *
     * @return [[TransformActionRoot]]
     */
    def runtimeModifier: TransformActionRoot = {
      import YamlParserUtil._
      val mirror = runtimeMirror(this.getClass.getClassLoader)
      val clazz = mirror.staticClass(this.getClass.getCanonicalName)
      val objMirror = mirror.reflect(this)
      val collectedValues = CollectAction.getCollectedValues
      val expressionMap = clazz.selfType.members.collect {
        case m: MethodSymbol if m.isCaseAccessor && m.annotations.exists(_.tree.tpe =:= typeOf[SqlExpr]) =>
          m.name.toString -> objMirror.reflectField(m).get
      }.toMap
      if (expressionMap.nonEmpty && collectedValues.nonEmpty) {
        val ymlString = convertToYaml2(this)
        val updatedYmlStr = collectedValues.foldLeft(ymlString) {
          case (ymlStr, (placeholder, expression)) =>
            val find = "${" + placeholder + "}"
            val replaceWith = sqlLiteralExpr(expression)
            if (ymlStr.contains(find)) {
              logInfo(s"Substituting placeholder: $find with value: $replaceWith")
              ymlStr.replace(find, replaceWith)
            }
            else ymlStr
        }
        return parseYAML(updatedYmlStr, classOf[TransformActionRoot])
      }
      this
    }

    /**
     * Get the Multi arg function that is registered in Spark Function registry
     *
     * @param name Function name
     * @return [[MultiColFunc]]
     */
    protected def getMultiArgFunction(name: String): MultiColFunc = (columns: Seq[Column]) => callUDF(name, columns: _*)


    /**
     * Applies know arguments to MultiArg Function in order provided and returns
     * a single arg function to which the actual column can be provided
     *
     * @param multiArgFunc Multi Argument Column Function
     * @param columns      Columns to be applied
     * @return [[ColFunc]]
     */
    protected def multiArgToSingleArgFunc(multiArgFunc: MultiColFunc, columns: Seq[Column]): ColFunc =
      (column: Column) => multiArgFunc(column +: columns)

    /**
     * Get the Single Arg Function that is registered in Spark Function registry
     *
     * @param name Function name
     * @return [[ColFunc]]
     */
    protected def getSingleArgFunction(name: String): ColFunc = callUDF(name, _)

  }

  /**
   * Class representing Transformation with single output
   *
   * @param id Action Id
   */
  abstract class SingleOutputTransform(id: String) extends TransformActionRoot(id) {
    def transformFunction(dataframes: DataFrame*): DFFunc

    def apply(dataframes: DataFrame*): Seq[DFFunc] = transformFunction(dataframes: _*) +: Nil
  }


  /**
   * Class representing Transformation with multiple output
   *
   * @param id Action Id
   */
  abstract class MultiOutputTransform(id: String) extends TransformActionRoot(id) {

    def transformFunctions(dataframes: DataFrame*): Seq[DFFunc]

    def apply(dataframes: DataFrame*): Seq[DFFunc] = transformFunctions(dataframes: _*)
  }


  // ===========================================  Concrete Actions are defined below  ====================================== //


  /*
      Coalesce
   */
  case class CoalesceAction(@JsonProperty(required = true, value = "num_partitions") numPartitions: Int)
    extends SingleOutputTransform(Actions.Coalesce) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Coalesce(numPartitions)
  }


  /*
     Repartition
   */
  case class RepartitionAction(@JsonProperty(required = false, value = "num_partitions") numPartitions: Int,
                               @JsonProperty(required = false) columns: Option[Seq[String]])
    extends SingleOutputTransform(Actions.Repartition) {
    override def transformFunction(dataframes: DataFrame*): DFFunc =
      Repartition(Some(numPartitions), columns.getOrElse(Nil): _*)
  }

  /*
      Rename
   */
  case class RenameAction(@JsonProperty(required = true) list: Map[String, String])
    extends SingleOutputTransform(Actions.Rename) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Rename(list.toSeq: _*)
  }


  /*
     Rename All or selected, supports prefix and suffix and searching with pattern
   */
  case class RenameAllAction(@JsonProperty(required = true) append: String,
                             @JsonProperty(required = false) prefix: Option[Boolean],
                             @JsonProperty(required = false) columns: Option[List[String]],
                             @JsonProperty(required = false) pattern: Option[String])
    extends SingleOutputTransform(Actions.RenameAll) {
    override def transformFunction(dataframes: DataFrame*): DFFunc =
      (columns, pattern) match {
        case (_, Some(regex)) => Rename(append, prefix.getOrElse(false), regex)
        case (Some(cols), None) => Rename(append, prefix.getOrElse(false), cols: _*)
        case (None, None) => Rename(append, prefix.getOrElse(false))
      }

  }

  /*
      Rename columns matching pattern, and replace provided text (find) with replacement text.
   */
  case class RenameFindReplace(@JsonProperty(required = true) pattern: String,
                               @JsonProperty(required = true) find: String,
                               @JsonProperty(required = true) replace: String)
    extends SingleOutputTransform(Actions.RenameFindReplace) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Rename(pattern, find, replace)
  }


  /*
     Column Transform
   */
  case class TransformAction(@SqlExpr @JsonProperty(required = true) list: Map[String, Any])
    extends SingleOutputTransform(Actions.Transform) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Transform(
      list.toSeq.flatMap {
        case (col, expression: String) => Some(col -> expression)
        case (col, expressions: Seq[_]) => expressions.map(col -> _.toString)
        case _ => throw new IllegalArgumentException("list values should be a string or list of strings")
      }: _*)
  }


  /*
     Column Transform Bulk, Applies a function to all provided columns.
   */
  case class TransformAllAction(@JsonProperty(value = "function", required = true) transformExpr: String,
                                @JsonProperty(required = false) suffix: Option[String],
                                @SqlExpr @JsonProperty(required = false) columns: Option[List[String]],
                                @JsonProperty(required = false) pattern: Option[String])
    extends SingleOutputTransform(Actions.TransformAll) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = {
      val transformFunc = getSingleArgFunction(transformExpr)
      (columns, pattern) match {
        case (None, None) => Transform(transformFunc, suffix)
        case (_, Some(regex)) => Transform(transformFunc, suffix, regex)
        case (Some(cols), None) => Transform(transformFunc, suffix, cols: _*)
      }
    }
  }

  /*
    Column transform for multi-arg function
  */
  case class TransformAllMultiArgAction(@JsonProperty(required = true) function: String,
                                        @SqlExpr @JsonProperty(required = true) list: Map[String, List[String]])
    extends SingleOutputTransform(Actions.TransformMultiArg) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = {
      val transformFunc = getMultiArgFunction(function)
      Transform(transformFunc, list.toSeq: _*)
    }
  }

  /*
    Join Transform
   */
  case class JoinAction(@SqlExpr @JsonProperty(value = "condition", required = false) joinCondition: String,
                        @JsonProperty(value = "columns", required = false) joinColumns: Seq[String],
                        @JsonProperty(value = "join_type", required = true) joinType: String,
                        @JsonProperty(value = "with", required = true) joinSource: String,
                        @JsonProperty(value = "broadcast_hint") broadcastHint: String)
    extends SingleOutputTransform(Actions.Join) {

    private val joinTypeFunc: DFJoinFunc => DataFrame => DFFunc = (joinFunc: DFJoinFunc) => joinType match {
      case "inner" => joinFunc >< _
      case "left" => joinFunc << _
      case "right" => joinFunc >> _
      case "full" => joinFunc <> _
    }

    def expressionBased: Boolean = {
      if (joinCondition == null && joinColumns == null)
        throw new YamlDataTransformException("Please provide either 'condition' or 'columns' option in join action definition")
      joinCondition != null
    }

    override def transformFunction(dataframes: DataFrame*): DFFunc = {
      if (expressionBased)
        joinTypeFunc(Join(Option(broadcastHint), expr(joinCondition)))(dataframes.head)
      else
        joinTypeFunc(Join(Option(broadcastHint), joinColumns: _*))(dataframes.head)
    }

    override def inputAliases: Seq[String] = Seq(joinSource)
  }


  /*
     Group By
   */
  case class GroupAction(@SqlExpr @JsonProperty(value = "columns", required = true) groupColumns: Seq[String],
                         @SqlExpr @JsonProperty(value = "exprs", required = true) groupExprs: Seq[String],
                         @JsonProperty(value = "pivot_column") pivotColumn: Option[String])
    extends SingleOutputTransform(Actions.GroupBy) {
    override def transformFunction(dataframes: DataFrame*): DFFunc =
      GroupByAndPivot(groupColumns.map(expr): _*)(pivotColumn).agg(groupExprs: _*)
  }

  /*
      Aggregate (without group by)
   */
  case class AggregateAction(@SqlExpr @JsonProperty(required = true) exprs: Seq[String])
    extends SingleOutputTransform(Actions.Aggregate) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Aggregate(exprs: _*)
  }

  /*
      Filter
   */
  case class FilterAction(@SqlExpr @JsonProperty(required = true) condition: String)
    extends SingleOutputTransform(Actions.Filter) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Filter(condition)
  }


  /*
      Select
   */
  case class SelectAction(@SqlExpr @JsonProperty(required = true) columns: Seq[String])
    extends SingleOutputTransform(Actions.Select) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Select1(columns: _*)
  }


  /*
    Select using regex
 */
  case class SelectWithRegexAction(@JsonProperty(required = true) regex: String)
    extends SingleOutputTransform(Actions.SelectRegex) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Select1(regex)
  }

  /*
      Select using alias
   */
  case class SelectWithAliasAction(@JsonProperty(required = true) alias: String,
                                   @JsonProperty(value = "include_columns") includeColumns: Option[Seq[String]],
                                   @JsonProperty(value = "skip_columns") skipColumns: Option[Seq[String]])
    extends SingleOutputTransform(Actions.SelectAlias) {
    override def transformFunction(dataframes: DataFrame*): DFFunc =
      Select1(dataframes.head, alias, includeColumns.getOrElse(Nil), skipColumns.getOrElse(Nil))

    override def inputAliases: Seq[String] = Seq(alias)
  }

  /*
     Reorder Select
   */
  case class SelectWithReorderedAction(@JsonProperty(required = true) alias: String)
    extends SingleOutputTransform(Actions.SelectReorder) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Select1(dataframes.head)

    override def inputAliases: Seq[String] = Seq(alias)
  }


  /*
     Select Not
   */
  case class SelectNotAction(@JsonProperty(required = true) columns: Seq[String])
    extends SingleOutputTransform(Actions.SelectNot) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = SelectNot(columns: _*)
  }

  /*
     Union Transform
   */
  case class UnionAction(@JsonProperty(required = true, value = "with") unionWith: Seq[String])
    extends SingleOutputTransform(Actions.Union) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Union(dataframes: _*)

    override def inputAliases: Seq[String] = unionWith
  }

  /*
     Intersect Transform
   */
  case class IntersectAction(@JsonProperty(required = true, value = "with") intersectWith: Seq[String])
    extends SingleOutputTransform(Actions.Intersect) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Intersect(dataframes: _*)

    override def inputAliases: Seq[String] = intersectWith
  }

  /*
    Except Transform
  */
  case class ExceptAction(@JsonProperty(required = true, value = "with") exceptWith: Seq[String])
    extends SingleOutputTransform(Actions.Except) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Except(dataframes: _*)

    override def inputAliases: Seq[String] = exceptWith
  }

  /*
     Order By
   */
  case class OrderByAction(@JsonProperty(required = true) columns: Seq[String])
    extends SingleOutputTransform(Actions.OrderBy) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = OrderBy(columns: _*)
  }

  /*
     Limit
   */
  case class LimitAction(@JsonProperty(required = true) size: Int) extends SingleOutputTransform(Actions.Limit) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Limit(size)
  }

  /*
      Distinct
   */
  case class DistinctAction() extends SingleOutputTransform(Actions.Distinct) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Distinct.apply()
  }

  /*
     Drop Duplicate
   */
  case class DropDuplicateAction(@JsonProperty(required = true) columns: Seq[String])
    extends SingleOutputTransform(Actions.DropDuplicates) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = DropDuplicates(columns: _*)
  }

  /*
      Drop Column
   */
  case class DropColumnAction(@JsonProperty(required = true) columns: Seq[String])
    extends SingleOutputTransform(Actions.DropColumn) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Drop(columns: _*)
  }

  /*
      Unstruct
   */
  case class UnstructAction(@JsonProperty(required = true) column: String)
    extends SingleOutputTransform(Actions.Unstruct) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Unstruct(column)
  }


  /*
      Collect action, This action is a NoOp. It causes a side effect where the collected values
      are stored for future reference
   */
  case class CollectAction(@JsonProperty(required = true) placeholder: String,
                           @JsonProperty(required = true) alias: String,
                           @JsonProperty(required = true) column: String)
    extends SingleOutputTransform(Actions.Collect) {

    import CollectAction._

    override def transformFunction(dataframes: DataFrame*): DFFunc = {
      mutableRef += (placeholder -> dataframes.head.select(column).collect.map(_.get(0)).toSeq)
      NoOp()
    }

    override def inputAliases: Seq[String] = Seq(alias)
  }

  object CollectAction {
    private val mutableRef: mutable.Map[String, Seq[Any]] = mutable.Map()

    def getCollectedValues: Map[String, Any] = {
      mutableRef
        .mapValues { arr => if (arr.size == 1) arr.head else arr.toList }
        .toMap
    }
  }

  /*
      Sequence
   */
  case class SequenceAction(@JsonProperty(value = "sk_source", required = true) skSource: String,
                            @JsonProperty(value = "sk_column", required = true) skColumn: String)
    extends SingleOutputTransform(Actions.Sequence) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = Sequence(dataframes.head.maxKeyValue(skColumn), skColumn)

    override def inputAliases: Seq[String] = Seq(skSource)
  }

  /*
      SCD
   */
  case class SCDAction(@JsonProperty(value = "dim_table", required = true) dimTable: String,
                       @JsonProperty(value = "sk_column", required = true) surrogateKey: String,
                       @JsonProperty(value = "natural_keys", required = true) naturalKeys: Seq[String],
                       @JsonProperty(value = "derived_columns", required = true) derivedColumns: Seq[String],
                       @JsonProperty(value = "meta_columns", required = true) metaColumns: Seq[String],
                       @JsonProperty(value = "incremental_load", required = false) incrementalLoad: Option[Boolean])
    extends SingleOutputTransform(Actions.SCD) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = DimensionChangeSet(dataframes.head, surrogateKey,
      naturalKeys, derivedColumns, metaColumns, incrementalLoad.getOrElse(true))

    override def inputAliases: Seq[String] = Seq(dimTable)
  }

  /*
      NA
   */
  case class NAAction(@JsonProperty(value = "default_numeric", required = true) defaultNumericValue: Double,
                      @JsonProperty(value = "default_string", required = true) defaultStringValue: String,
                      columns: Option[Seq[String]])
    extends SingleOutputTransform(Actions.NA) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = NA(defaultNumericValue, defaultStringValue, columns.getOrElse(Nil))
  }


  /*
     Yaml Action
   */
  case class YamlAction(@JsonProperty(value = "yaml_file", required = true) yamlFile: String,
                        @JsonProperty(value = "input_aliases", required = false) inputs: Option[Seq[String]],
                        @JsonProperty(value = "output_alias", required = true) outputAlias: String,
                        @JsonProperty(value = "substitutions", required = false) substitutions: Option[Map[String, Any]])
    extends SingleOutputTransform(Actions.Yaml) {

    override def transformFunction(dataframes: DataFrame*): DFFunc =
      (df: DataFrame) => {
        val transformed = IntermediateYaml(yamlFile, substitutions).getTransformedDFs(df +: dataframes: _*).toMap
        transformed.getOrElse(outputAlias, throw new YamlDataTransformException(s"Output Alias $outputAlias not found in $yamlFile yaml file"))
      }

    override def inputAliases: Seq[String] = inputs.getOrElse(Nil)
  }

  /*
     Named Transform
   */
  case class NamedAction(@JsonProperty(value = "name", required = true) transformationName: String,
                         @JsonProperty(value = "input_aliases", required = false) inputs: Option[Seq[String]])
    extends SingleOutputTransform(Actions.Named) {

    override def transformFunction(dataframes: DataFrame*): DFFunc = (df: DataFrame) => TransformationRegistration
      .getTransformation(transformationName)
      .fold(throw new YamlDataTransformException(s"Named transformation: '$transformationName' is not registered")) {
        transformation => transformation.apply(df +: dataframes)
      }

    override def inputAliases: Seq[String] = inputs.getOrElse(Nil)
  }


  /*
     DQ Check Transform
   */
  case class DQCheckAction(@JsonProperty(required = true) id: String,
                           @JsonProperty(value = "dq_function", required = true) dqFunction: String,
                           @JsonProperty(value = "options") functionOptions: Option[Seq[Any]],
                           @JsonProperty(required = true) columns: Seq[String])
    extends SingleOutputTransform(Actions.DQCheck) {

    override def transformFunction(dataframes: DataFrame*): DFFunc = {
      val multiArgFunction = getMultiArgFunction(dqFunction)
      val commonMultiArgs = functionOptions.fold(Nil: Seq[Column])(_.map(lit))
      DQCheck(id, multiArgToSingleArgFunc(multiArgFunction, commonMultiArgs), columns)
    }
  }


  /*
     Watermark (For Structured Streaming)
   */
  case class WatermarkAction(@JsonProperty(required = true, value = "event_time") eventTime: String,
                             @JsonProperty(required = true, value = "delay_threshold") delayThreshold: String)
    extends SingleOutputTransform(Actions.Watermark) {
    override def transformFunction(dataframes: DataFrame*): DFFunc = (df: DataFrame) => df.withWatermark(eventTime, delayThreshold)
  }

  /*
      Partition Transform (Multi Output), outputs two dataframes
  */
  case class PartitionAction(@SqlExpr @JsonProperty(required = true) condition: String)
    extends MultiOutputTransform(Actions.Partition) {
    override def transformFunctions(dataframes: DataFrame*): Seq[DFFunc] = Partition(condition)
  }


  /*
     Router Transform (Multi Output)
   */
  case class RouterAction(@SqlExpr @JsonProperty(required = true) conditions: Seq[String])
    extends MultiOutputTransform(Actions.Router) {
    override def transformFunctions(dataframes: DataFrame*): Seq[DFFunc] = Routes(conditions: _*)
  }

}
