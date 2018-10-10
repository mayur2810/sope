package com.sope.spark.sql

import com.sope.utils.Logging
import org.apache.spark.sql.functions.{broadcast, col, desc, expr}
import org.apache.spark.sql.{Column, DataFrame}

import scala.reflect.ClassTag

/**
  * This package consists of Spark SQL DSLs.
  * import to use: "com.sope.spark.sql.dsl._"
  *
  * @author mbadgujar
  */
package object dsl {


  /*
     No Operation Transform
   */
  object NoOp {
    /**
      * No Operation Dataframe function
      *
      * @return [[DFFunc]]
      */
    def apply(): DFFunc = (df: DataFrame) => df
  }


  /*
     Select Transform
   */
  object Select {
    /**
      * Select columns
      *
      * @param columns column names
      * @return [[DFFunc]]
      */
    def apply(columns: String*): DFFunc = (df: DataFrame) => df.selectExpr(columns: _*)

    /**
      * Select columns
      *
      * @param columns Column objects
      * @return [[DFFunc]]
      */
    def apply[_: ClassTag](columns: Column*): DFFunc = (df: DataFrame) => df.select(columns: _*)

    /**
      * Select columns from given dataframe
      *
      * @param reorderDF [[DataFrame]] which has the ordered columns
      * @return [[DFFunc]]
      */
    def apply(reorderDF: DataFrame): DFFunc = (df: DataFrame) => df.select(reorderDF.getColumns: _*)

    /**
      * Select columns from given dataframe which are aliased
      *
      * @param joinedDF [[DataFrame]] which has aliased columns
      * @param alias    DF alias
      * @return [[DFFunc]]
      */
    def apply(joinedDF: DataFrame, alias: String): DFFunc = (df: DataFrame) => df.select(joinedDF.getColumns(alias): _*)
  }


  /*
    Filter Select Transform
  */
  object SelectNot {
    /**
      * Select columns excepts passed columns
      *
      * @param excludeColumns columns to exclude
      * @return [[DFFunc]]
      */
    def apply(excludeColumns: String*): DFFunc = (df: DataFrame) => df.select(df.getColumns(excludeColumns): _*)

    /**
      * Select columns excepts passed columns from a aliased dataframe
      *
      * @param joinedDF       [[DataFrame]] which has aliased columns
      * @param alias          DF alias
      * @param excludeColumns columns to exclude
      * @return [[DFFunc]]
      */
    def apply(joinedDF: DataFrame, alias: String, excludeColumns: Seq[String]): DFFunc =
      (df: DataFrame) => df.select(joinedDF.getColumns(alias, excludeColumns): _*)
  }


  /*
    Filter Transform
  */
  object Filter {

    /**
      * Filter Dataframe function
      *
      * @param filterCondition [[String]] filter condition
      * @return [[DFFunc]]
      */
    def apply(filterCondition: String): DFFunc = (df: DataFrame) => df.filter(filterCondition)

    /**
      * Filter Dataframe function
      *
      * @param filterCondition [[Column]] filter condition
      * @return [[DFFunc]]
      */
    def apply[_: ClassTag](filterCondition: Column): DFFunc = (df: DataFrame) => df.filter(filterCondition)
  }


  /*
      Rename Transform
    */
  object Rename {
    /**
      * Rename columns function
      *
      * @param tuples Tuple of existing name and new name
      * @return [[DFFunc]]
      */
    def apply(tuples: (String, String)*): DFFunc = (df: DataFrame) => df.renameColumns(tuples.toMap)

    /**
      * Append prefix or suffix to all columns
      *
      * @param append String to append to column name
      * @param prefix is true adds as prefix else suffix
      * @return [[DFFunc]]
      */
    def apply(append: String, prefix: Boolean = true): DFFunc =
      (df: DataFrame) =>
        if (prefix)
          df.renameColumns(df.columns.map(col => col -> s"$append$col").toMap)
        else
          df.renameColumns(df.columns.map(col => col -> s"$col$append").toMap)
  }


  /*
    Drop Transform
  */
  object Drop {
    /**
      * Drop Columns function
      *
      * @param dropColumns columns to drop
      * @return [[DFFunc]]
      */
    def apply(dropColumns: String*): DFFunc = (df: DataFrame) => df.dropColumns(dropColumns)
  }


  /*
    Column Transformations
  */
  object Transform {
    /**
      * Apply String Expressions Transformations
      *
      * @param tuples column name and string expressions tuple
      * @return [[DFFunc]]
      */
    def apply(tuples: (String, String)*): DFFunc = (df: DataFrame) => df.applyStringExpressions(tuples.toMap)

    /**
      * Apply Column Expressions Transformations
      *
      * @param tuples column name and column expressions tuple
      * @return [[DFFunc]]
      */
    def apply[_: ClassTag](tuples: (String, Column)*): DFFunc = (df: DataFrame) => df.applyColumnExpressions(tuples.toMap)

    /**
      * Apply a column expression to provided columns
      *
      * @param columnFunc column expression
      * @param columns    column names to which function is to be applied
      * @return [[DFFunc]]
      */
    def apply(columnFunc: ColFunc, columns: String*): DFFunc = (df: DataFrame) =>
      df.applyColumnExpressions(columns.map(column => column -> columnFunc(col(column))).toMap)

    /**
      * Apply column expressions to all columns in dataframe
      *
      * @param columnFunc column expression
      * @return [[DFFunc]]
      */
    def apply(columnFunc: ColFunc): DFFunc = (df: DataFrame) =>
      df.applyColumnExpressions(df.columns.map(column => column -> columnFunc(col(column))).toMap)
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
    def apply(startIndex: Long, column: String): DFFunc = (df: DataFrame) => df.drop(column).generateSequence(startIndex, Some(column))
  }


  /*
    Join Transform
   */
  object Join extends Logging {

    /**
      * Apply Join Function
      *
      * @param broadcastHint Hint for broadcasting, "left" for broadcast hint to left [[DataFrame]]
      *                      or right for broadcast hint to "right" [[DataFrame]]
      * @param conditions    Join columns
      * @return [[DFJoinFunc]]
      */
    def apply(broadcastHint: Option[String], conditions: String*): DFJoinFunc =
      (ldf: DataFrame, rdf: DataFrame, jType: String) =>
        broadcastHint.map(_.toLowerCase) match {
          case Some("left") =>
            logInfo("Broadcast hint provided for left dataframe")
            broadcast(ldf).join(rdf, conditions.toSeq, jType)
          case Some("right") =>
            logInfo("Broadcast hint provided for right dataframe")
            ldf.join(broadcast(rdf), conditions.toSeq, jType)
          case _ => ldf.join(rdf, conditions.toSeq, jType)
        }


    /**
      * Apply Join Function
      *
      * @param broadcastHint Hint for broadcasting, "left" for broadcast hint to left [[DataFrame]]
      *                      or right for broadcast hint to "right" [[DataFrame]]
      * @param conditions    Join Column Expression
      * @return [[DFJoinFunc]]
      */
    def apply(broadcastHint: Option[String], conditions: Column): DFJoinFunc =
      (ldf: DataFrame, rdf: DataFrame, jType: String) =>
        broadcastHint.map(_.toLowerCase) match {
          case Some("left") =>
            logInfo("Broadcast hint provided for left dataframe")
            broadcast(ldf).join(rdf, conditions, jType)
          case Some("right") =>
            logInfo("Broadcast hint provided for right dataframe")
            ldf.join(broadcast(rdf), conditions, jType)
          case _ => ldf.join(rdf, conditions, jType)
        }
  }


  /*
    Group Transform
   */
  object Group {
    /**
      * Apply Group Function
      *
      * @param groupColumns Group columns
      * @return [[DFGroupFunc]]
      */
    def apply(groupColumns: String*): DFGroupFunc =
      (df: DataFrame, columns: Seq[Column]) =>
        df.groupBy(groupColumns.head, groupColumns.tail: _*).agg(columns.head, columns.tail: _*)

    /**
      * Apply Group Function
      *
      * @param groupColumns Group column Expressions
      * @return [[DFGroupFunc]]
      */
    def apply[_: ClassTag](groupColumns: Column*): DFGroupFunc =
      (df: DataFrame, columns: Seq[Column]) =>
        df.groupBy(groupColumns: _*).agg(columns.head, columns.tail: _*)
  }

  /*
     Union Transform
    */
  object Union {
    /**
      * Apply Union transform on provided dataframes
      *
      * @param dataframes [[DataFrame]]s to be unioned
      * @return [[DFFunc]]
      */
    def apply(dataframes: DataFrame*): DFFunc = (df: DataFrame) => dataframes.foldLeft(df)(_ union _)
  }

  /*
   Intersect Transform
  */
  object Intersect {
    /**
      * Apply Intersect transform on provided dataframes
      *
      * @param dataframes [[DataFrame]]s to be intersected
      * @return [[DFFunc]]
      */
    def apply(dataframes: DataFrame*): DFFunc = (df: DataFrame) => dataframes.foldLeft(df)(_ intersect _)
  }

  /*
   Except Transform
  */
  object Except {
    /**
      * Apply Except transform on provided dataframes
      *
      * @param dataframes [[DataFrame]]s for except transformation
      * @return [[DFFunc]]
      */
    def apply(dataframes: DataFrame*): DFFunc = (df: DataFrame) => dataframes.foldLeft(df)(_ except _)
  }

  /*
   OrderBy Transform
  */
  object OrderBy {
    /**
      * Apply order by transform
      *
      * @param columns Column names followed by ':' and sort order, e.g. col1:desc, col2:asc
      *                or just column name which will asc ordering by default
      * @return [[DFFunc]]
      */
    def apply(columns: String*): DFFunc = (df: DataFrame) =>
      df.orderBy(columns.map(exp =>
        if (exp.toLowerCase.endsWith("desc"))
          desc(exp.split(":").head)
        else
          col(exp.split(":").head)): _*)

    /**
      * Apply order by transform
      *
      * @param columns [[Column]] objects
      * @return [[DFFunc]]
      */
    def apply[_: ClassTag](columns: Column*): DFFunc = (df: DataFrame) => df.orderBy(columns: _*)
  }

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
   Distinct Transform
  */
  object Distinct {
    /**
      * Apply Distinct duplicates transform
      *
      * @return [[DFFunc]]
      */
    def apply(): DFFunc = (df: DataFrame) => df.distinct
  }

  /*
   Limit Transform
  */
  object Limit {
    /**
      * Apply Limit transform
      *
      * @param n number of rows
      * @return [[DFFunc]]
      */
    def apply(n: Int): DFFunc = (df: DataFrame) => df.limit(n)
  }


  /*
    Cube Transform
   */
  object Cube {
    /**
      * Apply Cube Function
      *
      * @param groupColumns Group columns
      * @return [[DFGroupFunc]]
      */
    def apply(groupColumns: String*): DFGroupFunc =
      (df: DataFrame, columns: Seq[Column]) =>
        df.cube(groupColumns.head, groupColumns.tail: _*).agg(columns.head, columns.tail: _*)

    /**
      * Apply Cube Function
      *
      * @param groupColumns Group column Expressions
      * @return [[DFGroupFunc]]
      */
    def apply[_: ClassTag](groupColumns: Column*): DFGroupFunc =
      (df: DataFrame, columns: Seq[Column]) =>
        df.cube(groupColumns: _*).agg(columns.head, columns.tail: _*)
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
      Update/Add Surrogate key columns by joining on the 'Key' dimension table.
   */
  object UpdateKeys {

    /**
      * Update/Add Surrogate key columns by joining on the 'Key' dimension table.
      * It is assumed that the key column naming convention is followed. e.g. 'sale_date' column will have key column 'sale_date_key'
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
      Dataframe Implicit methods
   */
  implicit class DFImplicits(dataframe: DataFrame) {
    /**
      * Apply transformation funtion to [[DataFrame]]
      *
      * @param func transformation funtion [[DFFunc]]
      * @return [[DataFrame]]
      */
    def -->(func: DFFunc): DataFrame = dataframe.transform(func)
  }


  /*
    [[DFFunc]] implicits
   */
  implicit class DFFuncImplicits(dfFunc: DFFunc) {
    /**
      * Apply Transformation function to passed [[DataFrame]]
      *
      * @param df dataframe
      * @return [[DataFrame]]
      */
    def -->(df: DataFrame): DataFrame = df.transform(dfFunc)

    /**
      * Apply Transformation function to passed [[DataFrame]]
      *
      * @param df dataframe
      * @return [[DataFrame]]
      */
    def using(df: DataFrame): DataFrame = df.transform(dfFunc)

    /**
      * Compose [[DFFunc]] transformations
      *
      * @param rightDFFunc [[DFFunc]] transformation
      * @return [[DFFunc]]
      */
    def +(rightDFFunc: DFFunc): DFFunc = dfFunc andThen rightDFFunc
  }


  /*
      Join Function implicits
   */
  implicit class DFJoinFuncImplicits(dfJFunc: DFJoinFunc) {

    /**
      * Inner join
      *
      * @param rightDF join [[DataFrame]]
      * @return [[DFFunc]]
      */
    def inner(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "inner")

    /**
      * Inner join
      *
      * @param rightDF join [[DataFrame]]
      * @return [[DFFunc]]
      */
    def ><(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "inner")

    /**
      * Left join
      *
      * @param rightDF join [[DataFrame]]
      * @return [[DFFunc]]
      */
    def left(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "left")

    /**
      * Left join
      *
      * @param rightDF join [[DataFrame]]
      * @return [[DFFunc]]
      */
    def <<(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "left")

    /**
      * Right join
      *
      * @param rightDF join [[DataFrame]]
      * @return [[DFFunc]]
      */
    def right(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "right")

    /**
      * Right join
      *
      * @param rightDF join [[DataFrame]]
      * @return [[DFFunc]]
      */
    def >>(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "right")

    /**
      * Full join
      *
      * @param rightDF join [[DataFrame]]
      * @return [[DFFunc]]
      */
    def full(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "full")

    /**
      * Full join
      *
      * @param rightDF join [[DataFrame]]
      * @return [[DFFunc]]
      */
    def <>(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "full")
  }

  /*
     Group function implicits
   */
  implicit class DFGroupFuncImplicits(dfGFunc: DFGroupFunc) {
    /**
      * Apply Aggregation expressions for Group by
      *
      * @param aggExprs Aggregation column expressions
      * @return [[DFFunc]]
      */
    def ^(aggExprs: Column*): DFFunc = dfGFunc(_: DataFrame, aggExprs)

    /**
      * Apply Aggregation expressions for Group by
      *
      * @param aggExprs Aggregation String expressions
      * @return [[DFFunc]]
      */
    def ^[_: ClassTag](aggExprs: String*): DFFunc = dfGFunc(_: DataFrame, aggExprs.map(expr))
  }

}
