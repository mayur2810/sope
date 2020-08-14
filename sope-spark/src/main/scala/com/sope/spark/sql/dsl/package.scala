package com.sope.spark.sql

import com.sope.common.sql.Types.GFunc
import com.sope.common.sql.dsl.DSL
import com.sope.common.transform.exception.TransformException
import com.sope.spark.etl.register.TransformationRegistration
import com.sope.spark.utils.etl.DimensionTable
import com.sope.spark.yaml.IntermediateYaml
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import scala.reflect.ClassTag

/**
 * @author mbadgujar
 */
package object dsl extends DSL {


  /*
   DropDuplicates Transform
  */
  object DropDuplicates {
    /**
     * Apply drop duplicates transform
     *
     * @param columns Column names on which duplicates will be dropped
     * @return [[DFFunc]]
     */
    def apply(columns: String*): DFFunc = (df: DataFrame) => df.dropDuplicates(columns.head, columns.tail: _*)
  }

  /*
      Update/Add Surrogate key columns by joining on the 'Key' dimension table.
   */
  object UpdateKeys {

    /**
     * Update/Add Surrogate key columns by joining on the 'Key' dimension table.
     * It is assumed that the key column naming convention is followed. e.g. 'sale_date' column will have key column
     * 'sale_date_key'
     *
     * @param columns     Columns for which key columns are to be generated.
     * @param keyTable    Key Dimension table
     * @param joinColumn  Join column
     * @param valueColumn 'Key' value column
     * @return [[DFFunc]]
     */
    def apply(columns: Seq[String], keyTable: DataFrame, joinColumn: String, valueColumn: String): DFFunc =
      (df: DataFrame) => df.updateKeys(columns, keyTable, joinColumn, valueColumn)
  }


  /*
      NA Functions
   */
  object NA {
    /**
     * Apply NA functions with default Numeric and String column values
     *
     * @param defaultNumeric Default Numeric value
     * @param defaultString  Default String value
     * @return [[DFFunc]]
     */
    def apply(defaultNumeric: Double, defaultString: String, columns: Seq[String] = Nil): DFFunc =
      (df: DataFrame) => columns match {
        case Nil => df.na.fill(defaultNumeric).na.fill(defaultString)
        case _ => df.na.fill(defaultNumeric, columns).na.fill(defaultString, columns)
      }

    /**
     * Apply NA functions with default values for porvided columns in map
     *
     * @param valueMap mapping of default null values for specified columns in map
     * @return [[DFFunc]]
     */
    def apply(valueMap: Map[String, Any]): DFFunc = (df: DataFrame) => df.na.fill(valueMap)
  }


  /*
    Generate Sequence numbers for a column based on the previous Maximum sequence value
 */
  object Sequence {
    /**
     * Apply Sequence Function
     *
     * @param startIndex Start index for sequence generation. The Sequence column is assumed as first column in
     *                   the dataframe
     * @return [[DFFunc]]
     */
    def apply(startIndex: Long): DFFunc = (df: DataFrame) => df.generateSequence(startIndex)

    /**
     * Apply Sequence Function
     *
     * @param startIndex Start index for sequence generation
     * @param column     Column name for sequence column
     * @return [[DFFunc]]
     */
    def apply(startIndex: Long, column: String): DFFunc = (df: DataFrame) =>
      df.drop(column).generateSequence(startIndex, Some(column))
  }

  /*
  Unstruct the fields in a given Struct as columns.
 */
  object Unstruct {
    /**
     * Apply Unstruct function
     *
     * @param unstructCol      unstruct column name
     * @param keepStructColumn Pass true to keep the sturct column, else will be dropped.
     * @return [[DFFunc]]
     */
    def apply(unstructCol: String, keepStructColumn: Boolean = true): DFFunc =
      (df: DataFrame) => df.unstruct(unstructCol, keepStructColumn)
  }

  /*
      Get Dimension Change Set
   */
  object DimensionChangeSet {
    /**
     * Get the dimension change set for the provided input data.
     * The resultant dataframe will contain a columns named 'change_status' containing the
     * change statuses: INSERT, UPDATE, NCD, INVALID
     *
     * @param dimensionDF     [[DataFrame]] Dimension table dataframe
     * @param surrogateKey    [[String]] Surrogate key column name
     * @param naturalKeys     [[Seq[String]] List of natural key columns for the Dimension
     * @param derivedColumns  [[Seq[String]] List of derived columns for the Dimension. Derived columns will not be
     *                        considered for SCD
     *                        For update records, If source has the derived columns then source value will be
     *                        considered else target values
     * @param metaColumns     [[Seq[String]] List of meta columns for the Dimension table. Meta columns will not be
     *                        considered for SCD
     *                        Meta columns will be null for the insert and update records
     * @param incrementalLoad Flag denoting whether it is a incremental load or full load
     * @return [[DFFunc]]
     */
    def apply(dimensionDF: DataFrame,
              surrogateKey: String,
              naturalKeys: Seq[String],
              derivedColumns: Seq[String] = Seq(),
              metaColumns: Seq[String] = Seq(),
              incrementalLoad: Boolean = true): DFFunc = {
      inputData: DataFrame =>
        DimensionTable(dimensionDF, surrogateKey, naturalKeys, derivedColumns, metaColumns)
          .getDimensionChangeSet(inputData, incrementalLoad).getUnion
    }
  }


  /*
      Routes, useful for splitting dataset
   */
  object Routes {

    /**
     * Returns a list of [[DFFunc]] that will each filter on the respective conditions
     * A non-matching condition is returned as the last [[DFFunc]]
     *
     * @param routingConditions [[Column]] conditions
     * @return [[DFFuncSeq]]
     */
    def apply(routingConditions: Column*): DFFuncSeq = routingConditions
      .map { condition => (df: DataFrame) => df.filter(condition) } :+ {
      df: DataFrame => df.filter(routingConditions.map { condition => not(condition) }.reduce(_ and _))
    }

    /**
     * Returns a list of [[DFFunc]] that will each filter on the respective string expressions
     * A non-matching condition is returned as the last [[DFFunc]]
     *
     * @param routingConditions [[String]] String expressions
     * @return [[DFFuncSeq]]
     */
    def apply[T: ClassTag](routingConditions: String*): DFFuncSeq = apply(routingConditions.map(expr): _*)

    /**
     * Returns a Map of identifier and [[DFFunc]] that will each filter on the respective conditions
     * A non-matching condition is returned by an identifier named "default"
     *
     * @param routingConditions [[Column]] conditions
     * @return [[DFFuncMap]]
     */
    def apply(routingConditions: (String, Column)*): DFFuncMap = routingConditions
      .unzip match {
      case (names, conditions) => (names :+ "default", apply(conditions: _*)).zipped.toMap
    }

    /**
     * Returns a Map of identifier and [[DFFunc]] that will each filter on the respective string expressions
     * A non-matching condition is returned by an identifier named "default"
     *
     * @param routingConditions String expressions
     * @return [[DFFuncMap]]
     */
    def apply[T: ClassTag](routingConditions: (String, String)*): DFFuncMap = apply(
      routingConditions.map { case (name, condition) => (name, expr(condition)) }: _*
    )
  }

  /*
       Splits dataset into binary set
   */
  object Partition {
    /**
     * Partition based on provided column condition. The first set is for the matching criteria
     *
     * @param condition Column condition
     * @return [[DFFuncSeq]]
     */
    def apply(condition: Column): DFFuncSeq = Routes(condition)

    /**
     * Partition based on provided string expression. The first set is for the matching criteria
     *
     * @param condition String expression
     * @return [[DFFuncSeq]]
     */
    def apply(condition: String): DFFuncSeq = apply(expr(condition))
  }

  /*
      DQCheck
   */
  object DQCheck {
    private val DQStatusSuffix = "dq_failed"
    private val DQColumnListSuffix = "dq_failed_columns"

    /**
     * Applies Data Quality Check. A new column will be created with {original_column_name}_dq_failed
     * which will contain the failed status. Another column with {id}_dq_failed_columns lists all the
     * columns names that failed the check for the overall data row.
     *
     * @param id         the data check id
     * @param dqFunction [[ColFunc]] data check function
     * @param columns    Columns on which DQ is to be applied
     * @return [[DFFunc]]
     */
    def apply(id: String, dqFunction: ColFunc, columns: Seq[Column]): DFFunc = {
      columns match {
        case Nil => NoOp()
        case _ =>
          Transform(columns.map(column => s"${column}_${id}_$DQStatusSuffix" -> dqFunction(column)): _*) +
            Transform {
              val dqColumns = columns.map(column =>
                when(col(s"${column}_${id}_$DQStatusSuffix") === true, lit(column.toString)).otherwise(lit(null)))
              s"${id}_$DQColumnListSuffix" -> concat_ws(",", dqColumns: _*)
            }
      }
    }

    /**
     * Applies Data Quality Check. A new column will be created with {original_column_name}_dq_failed
     * which will contain the failed status. Another column with {id}_dq_failed_columns lists all the
     * columns names that failed the check for the overall data row.
     *
     * @param id         the data check id
     * @param dqFunction [[ColFunc]] data check function
     * @param columns    Columns on which DQ is to be applied
     * @return [[DFFunc]]
     */
    def apply[T: ClassTag](id: String, dqFunction: String, functionOptions: Option[Seq[Any]], columns: Seq[String]): DFFunc = {
      val multiArgFunction = ColumnOps.resolveMultiArgFunction(dqFunction)
      val commonMultiArgs = functionOptions.fold(Nil: Seq[Column])(_.map(lit))
      DQCheck(id, ColumnOps.multiArgToSingleArgFunc(multiArgFunction, commonMultiArgs), columns.map(col))
    }
  }


  object Repartition {

    /**
     * Applies repartitioning
     *
     * @param numPartitions Number of partitions, optional
     * @param columns       [[Column]]s to partition on
     * @return [[DFFunc]]
     */
    def apply(numPartitions: Option[Int], columns: Column*): DFFunc = (df: DataFrame) => {
      (numPartitions, columns) match {
        case (None, Nil) => df /* do nothing */
        case (Some(0), Nil) => df /* do nothing */
        case (None, cols) => df.repartition(cols: _*)
        case (Some(0), cols) => df.repartition(cols: _*)
        case (Some(number), Nil) => df.repartition(number)
        case (Some(number), cols) => df.repartition(number, cols: _*)
      }
    }

    /**
     * Applies repartitioning
     *
     * @param numPartitions Number of partitions, optional
     * @param columns       String expressions to partition on
     * @return [[DFFunc]]
     */
    def apply[T: ClassTag](numPartitions: Option[Int], columns: String*): DFFunc =
      apply(numPartitions, columns.map(expr): _*)
  }


  object Coalesce {
    /**
     * Applies Coalesce
     *
     * @param numPartitions number of partitions
     * @return [[DFFunc]]
     */
    def apply(numPartitions: Int): DFFunc = (df: DataFrame) => df.coalesce(numPartitions)
  }

  object Watermark {

    /**
     * Applies an event time watermark for this Dataframe (Streaming mode)
     *
     * @param eventTime      event time column name
     * @param delayThreshold the minimum delay to wait to data to arrive late
     * @return [[DFFunc]]
     */
    def apply(eventTime: String, delayThreshold: String): DFFunc = (df: DataFrame) =>
      df.withWatermark(eventTime, delayThreshold)
  }

  object CallYaml {
    def apply(yamlFile: String,
              dataframes: Seq[DataFrame],
              outputAlias: String,
              substitutions: Option[Map[String, Any]]): DFFunc =
      (df: DataFrame) => {
        val transformed = IntermediateYaml(yamlFile, substitutions).getTransformedDFs(df +: dataframes: _*).toMap
        transformed.getOrElse(outputAlias, throw new TransformException(s"Output Alias $outputAlias not found in $yamlFile yaml file"))
      }
  }

  object NamedTransform {
    def apply(transformationName: String, dataframes: Seq[DataFrame]): DFFunc =
      (df: DataFrame) => TransformationRegistration
        .getTransformation(transformationName)
        .fold(throw new TransformException(s"Named transformation: '$transformationName' is not registered")) {
          transformation => transformation.apply(df +: dataframes)
        }
  }

  object GroupByAndPivot {

    /**
     * Apply Group Function
     *
     * @param groupColumns Group columns
     * @param pivotColumn  pivot column
     * @return [[DFGroupFunc]]
     */
    def apply(groupColumns: String*)(pivotColumn: String): GFunc[DataFrame, Column] =
      apply(groupColumns.map(expr): _*)(col(pivotColumn))

    /**
     * Apply Group Function
     *
     * @param groupColumns Group column Expressions
     * @param pivotColumn  pivot column
     * @return [[DFGroupFunc]]
     */
    def apply[T: ClassTag](groupColumns: Column*)(pivotColumn: Column): GFunc[DataFrame, Column] =
      (aggExprs: Seq[(String, Column)]) => (df: DataFrame) => {
        val aggregateExprs = aggExprs.map { case (name, aggExpr) => aggExpr.as(name) }
        df
          .groupBy(groupColumns: _*)
          .pivot(pivotColumn)
          .agg(aggregateExprs.head, aggregateExprs.tail: _*)
      }
  }


}
