package com.sope.spark

import com.sope.spark.sql.udfs.CollectStruct
import com.sope.utils.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Implicit utility functions for [[DataFrame]]
  *
  * @author mbadgujar
  */
package object sql {


  // Type Aliases
  type DFFunc = (DataFrame) => DataFrame
  type DFFunc2 = (SQLContext) => DataFrame
  type ColFunc = Column => Column
  type MultiColFunc = Seq[Column] => Column
  type DFJoinFunc = (DataFrame, DataFrame, String) => DataFrame
  type DFGroupFunc = (DataFrame, Seq[Column]) => DataFrame


  implicit class DataFrameImplicits(dataframe: DataFrame) extends Logging {

    import dataframe.sqlContext.implicits._

    /**
      * Get Spark [[DataType]] given the string name of Type
      *
      * @param name [[String]] string
      * @return [[DataType]]
      */
    private def nameToType(name: String): DataType = {
      val nonDecimalNameToType = {
        Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
          DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType)
          .map(t => t.getClass.getSimpleName.replace("$", "") -> t).toMap
      }
      val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)""".r
      name match {
        case "decimal" => DecimalType.USER_DEFAULT
        case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
        case other => nonDecimalNameToType(other)
      }
    }

    /**
      * Get [[DataFrame]] columns
      *
      * @return [[Array]] of [[ColumnName]]
      */
    def getColumns: Array[ColumnName] = dataframe.columns.map(columnName => $"$columnName")

    /**
      * Get [[DataFrame]] columns excluding the provided columns
      *
      * @param excludeColumns Columns to be excluded
      * @return [[Array]] of [[ColumnName]]
      */
    def getColumns(excludeColumns: Seq[String]): Array[ColumnName] = dataframe.columns
      .filterNot(excludeColumns.contains(_))
      .map(columnName => $"$columnName")

    /**
      * Get [[DataFrame]] columns with Table alias
      *
      * @param alias Table alias
      * @return [[Array]] of [[ColumnName]]
      */
    def getColumns(alias: String): Array[ColumnName] = dataframe.columns.map(columnName => $"$alias.$columnName")

    /**
      * Get [[DataFrame]] columns excluding the provided columns and with Table alias
      *
      * @param alias          Table alias
      * @param excludeColumns Columns to be excluded
      * @return [[Array]] of [[ColumnName]]
      */
    def getColumns(alias: String, excludeColumns: Seq[String]): Array[ColumnName] = dataframe.columns
      .filterNot(excludeColumns.contains(_))
      .map(columnName => $"$alias.$columnName")


    /**
      * Check is Dataframe is not empty
      *
      * @return [[Boolean]]
      *
      */
    def isNotEmpty: Boolean = dataframe.head(1).nonEmpty

    /**
      * Check is Dataframe is empty
      *
      * @return [[Boolean]]
      *
      */
    def isEmpty: Boolean = dataframe.head(1).isEmpty


    /**
      * Drop Columns from Dataframe
      * Note: Supported for Spark 1.x version, Spark 2.x has this version
      *
      * @param columns [[Seq]] of column [[String]]
      * @return [[DataFrame]]
      */
    def dropColumns(columns: Seq[String]): DataFrame = {
      columns.foldLeft(dataframe)((df, column) => df.drop(column))
    }


    /**
      * Rename Columns as provided in the Map's key & value.
      * If reverse is false, key will be renamed to value else values will be renamed to keys
      *
      * @param nameMap [[Map[String, String]] name -> rename string map
      * @param reverse [[Boolean]] consider map values as columns to be renamed
      * @return [[DataFrame]] with columns renamed
      */
    def renameColumns(nameMap: Map[String, String], reverse: Boolean = false): DataFrame = {
      val renameMap = if (reverse) nameMap.map { case (k, v) => (v, k) } else nameMap
      renameMap.keys.foldLeft(dataframe) {
        (df, key) => df.withColumnRenamed(key, renameMap(key))
      }
    }

    /**
      * Applies the sql string expression to provided columns. If the column is existing, it will be replaced with expression output,
      * else a new column will be created
      *
      * @param exprMap [[Map[String, String]] name -> string expression map
      * @return [[DataFrame]] with expressions applied to columns
      */
    def applyStringExpressions(exprMap: Map[String, String]): DataFrame = {
      exprMap.keys.foldLeft(dataframe) {
        (df, key) => df.withColumn(key, expr(exprMap(key)))
      }
    }

    /**
      * Applies the column expression to provided columns. If the column is existing, it will be replaced with expression output,
      * else a new column will be created
      *
      * @param exprMap [[Map[String, Column]] name -> column expression map
      * @return [[DataFrame]] with expressions applied to columns
      */
    def applyColumnExpressions(exprMap: Map[String, Column]): DataFrame = {
      exprMap.keys.foldLeft(dataframe) {
        (df, key) => df.withColumn(key, exprMap(key))
      }
    }


    /**
      * Applies the provided transformation functions to [[DataFrame]]
      * Use if the function return different type and some selection function is to be used
      *
      * @param transformFunctionList Transformation functions
      * @param selectorFunc Selection function that will generate the dataframe for next function
      * @tparam A The return type of transformation function
      * @return Transformed [[DataFrame]]
      */
    def applyDFTransformations[A](transformFunctionList: Seq[DataFrame => A])(selectorFunc: A => DataFrame): DataFrame = {
      transformFunctionList.foldLeft(dataframe) { (df, function) => selectorFunc(function(df)) }
    }

    /**
      * Applies the provided transformation functions to [[DataFrame]]
      *
      * @param transformFunctionList Transformation functions
      * @return Transformed [[DataFrame]]
      */
    def applyDFTransformations(transformFunctionList: Seq[DFFunc]): DataFrame = {
      applyDFTransformations[DataFrame](transformFunctionList)((df: DataFrame) => df)
    }


    /**
      * Applies the provided transformation functions to [[DataFrame]].
      * The Intermediate transformation result can be stored at provided Temporary directory, to materialize the results
      * for complex transformations. The fuseFactor decides the number of transformations to be fused together and saved to
      * temporary directory
      *
      * @param transformFunctionList Transformation functions with function type information
      * @param temporaryResultDir    Temporary Directory to store results
      * @param format                Output format of output files. Default is parquet
      * @param fuseFactor            Fuse factor for transformations. Defaults to 1.
      * @return Transformed [[DataFrame]]
      */
    def applyDFTransformations(transformFunctionList: Seq[(String, DFFunc)], temporaryResultDir: String,
                               format: String = "parquet", fuseFactor: Int = 1):
    DataFrame = {
      val functionList = transformFunctionList
        .sliding(fuseFactor, fuseFactor)
        .map(funcSeq => {
          funcSeq.reduce[(String, DFFunc)] {
            case ((fType1, function1), (fType2, function2)) => (fType1 + ", " + fType2, function1.andThen(function2))
          }
        })
        .map { case (fType, function) =>
          (df: DataFrame) => {
            logInfo("Applying function :- " + fType)
            val result = df.transform(function)
            val tempOutputPath = temporaryResultDir + "/" + fType
            logInfo(s"Saving transformation results $fType to temporary location :- $tempOutputPath")
            result.write.format(format).mode(SaveMode.Overwrite).save(tempOutputPath)
            result.sqlContext.read.format(format).load(tempOutputPath)
          }
        }.toSeq
      applyDFTransformations[DataFrame](functionList)((df: DataFrame) => df)
    }

    /**
      * Cast given columns to provided [[DataType]]
      *
      * @param columns  Column [[Seq]]
      * @param dataType Spark [[DataType]]
      * @return [[DataFrame]] with columns casted to given type
      */
    def castColumns(columns: Seq[String], dataType: DataType): DataFrame = {
      columns.foldLeft(dataframe)((df, column) => df.withColumn(column, $"$column".cast(dataType)))
    }

    /**
      * Cast all columns from current [[DataType]] to provided [[DataType]]
      *
      * @param currentDataType Current [[DataType]]
      * @param toDataType      Requred  [[DataType]]
      * @return [[DataFrame]] with columns casted to given type
      */
    def castColumns(currentDataType: DataType, toDataType: DataType): DataFrame = {
      val columnMD = dataframe.dtypes.map { case (columnName, dType) => (columnName, nameToType(dType)) }
      val columnsToCast = columnMD.filter(_._2 == currentDataType).map(_._1)
      columnsToCast.foldLeft(dataframe)((df, column) => df.withColumn(column, $"$column".cast(toDataType)))
    }

    /**
      * Partitions dataframe based on the filter conditions
      * First dataframe in returned tuple will contain matched records to filter condition
      * Second dataframe will consist unmatched records
      *
      * @param filterCondition [[Column]] filter condition
      * @return [[Tuple2]] [Dataframe, Dataframe]
      */
    def partition(filterCondition: Column): (DataFrame, DataFrame) = {
      (dataframe.filter(filterCondition), dataframe.filter(not(filterCondition)))
    }

    /**
      * Get Max Value of a Key column
      *
      * @param keyColumn Key [[Column]]
      * @return Max column value
      */
    def maxKeyValue(keyColumn: Column): Long = {
      dataframe.select(max($"$keyColumn"))
        .collect()
        .headOption match {
        case None => 0
        case Some(row) => row.get(0).toString.toLong
      }
    }

    /**
      * Get Max Value of a Key column
      *
      * @param keyColumn Key [[Column]]
      * @return Max column value
      */
    def maxKeyValue(keyColumn: String): Long = {
      dataframe.maxKeyValue(col(keyColumn))
    }

    /**
      * Generate Sequence numbers for a column based on the previous Maximum sequence value
      *
      * @param previousSequenceNum Previous maximum sequence number
      * @param seqColumn           Optional column name if new sequence column is to be added.
      *                            If sequence column is present, it should be first column in Row of LongType
      * @return [[DataFrame]] with Sequence generated
      */
    def generateSequence(previousSequenceNum: Long, seqColumn: Option[String] = None): DataFrame = {
      // generate sequence
      val rowWithIndex = dataframe.rdd.zipWithIndex
      // Get schema
      val (rankedRows, schema) = seqColumn match {
        case None =>
          (rowWithIndex.map { case (row, id) => Row.fromSeq((id + previousSequenceNum + 1) +: row.toSeq.tail) }, dataframe.schema)
        case Some(seqColName) =>
          (rowWithIndex.map { case (row, id) => Row.fromSeq((id + previousSequenceNum + 1) +: row.toSeq) },
            StructType(StructField(seqColName, LongType, nullable = false) +: dataframe.schema.fields))
      }
      // Convert back to dataframe
      dataframe.sqlContext.createDataFrame(rankedRows, schema)
    }


    /**
      * Note: Supported for Spark 1.x version, Spark 2.x has this version (function collect_list)
      *
      *             Group by on Columns and return dataframe as grouped list of row structure of remaining columns
      *             The returned dataframe has two columns 'grouped_data' which has the grouped data
      *             and 'grouped_count' which has the element counts in the grouped list
      * @param groupByColumns     Columns to Group on
      * @param toBeGroupedColumns Optional Columns to be grouped in to List of spark struct, else all columns will be considered
      * @return Grouped [[DataFrame]]
      */
    def groupByAsList(groupByColumns: Seq[String], toBeGroupedColumns: Seq[String] = Seq()): DataFrame = {
      val arraySizeUDF = udf((array: Seq[Row]) => array.size)
      val dataframeFieldMap = dataframe.schema.fields.map(field => field.name -> field).toMap

      // Get fields to be grouped
      val toBeGroupedCols = toBeGroupedColumns match {
        case Nil => dataframe.columns.toSeq
        case _ => toBeGroupedColumns
      }

      // Struct of fields to be grouped
      val toBeGroupedColsStruct = struct(toBeGroupedCols.head, toBeGroupedCols.tail: _*).as("to_be_grouped_cols")

      // Instantiate UDAF
      val collectStructUDAF = new CollectStruct(StructType(toBeGroupedCols.map(dataframeFieldMap(_))))

      // Return grouped result
      dataframe.select(groupByColumns.map(col) :+ toBeGroupedColsStruct: _*)
        .groupBy(groupByColumns.head, groupByColumns.tail: _*)
        .agg(collectStructUDAF($"to_be_grouped_cols").as("grouped_data"))
        .withColumn("grouped_count", arraySizeUDF($"grouped_data"))
    }

    /**
      * Unstruct the fields in a given Struct as columns.
      * This is a no-op if struct column is not found
      *
      * @param unstructCol      Column to unstruct
      * @param keepStructColumn Keep Struct Column
      * @return [[DataFrame]]
      */
    def unstruct(unstructCol: String, keepStructColumn: Boolean = true): DataFrame = {
      val structColumn = dataframe.schema.find(field => field.name.equals(unstructCol)
        && field.dataType.isInstanceOf[StructType])
      structColumn match {
        case None => dataframe
        case Some(field) =>
          val structSchema = field.dataType.asInstanceOf[StructType]
          val unstructDF = structSchema.fields.map(field =>
            (field.name, s"$unstructCol.${field.name}")).foldLeft(dataframe) {
            case (df, column) => df.withColumn(column._1, col(column._2))
          }
          if (keepStructColumn) unstructDF else unstructDF.drop(unstructCol)
      }
    }

    /**
      * Update/Add Surrogate key columns by joining on the 'Key' dimension table.
      * It is assumed that the key column naming convention is followed. e.g. 'sale_date' column will have key column 'sale_date_key'
      *
      * @param columns     Columns for which key columns are to be generated.
      * @param keyTable    Key Dimension table
      * @param joinColumn  Join column
      * @param valueColumn 'Key' value column
      * @return Updated [[DataFrame]]
      */
    def updateKeys(columns: Seq[String], keyTable: DataFrame, joinColumn: String, valueColumn: String): DataFrame = {
      columns.foldLeft(dataframe) {
        (df, column) =>
          df.join(broadcast(keyTable.select(joinColumn, valueColumn)), col(column) === col(joinColumn), "left")
            .withColumn(s"${column}_key", col(valueColumn))
            .drop(joinColumn).drop(valueColumn)
      }.na.fill(-1, columns.map(_ + "_key"))
    }

  }


}
