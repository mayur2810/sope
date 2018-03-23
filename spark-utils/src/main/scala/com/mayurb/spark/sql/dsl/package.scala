package com.mayurb.spark.sql

import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Column, DataFrame}

import scala.reflect.ClassTag

/**
  * This package consists of Spark SQL DSLs.
  * import to use: "com.mayurb.spark.sql.dsl._"
  *
  * @author mbadgujar
  */
package object dsl {


  /*
     No Operation Transform
   */
  object NoOp {
    def apply(): DFFunc = (df: DataFrame) => df
  }



  /*
     Select Transform
   */
  object Select {
    def apply(columns: String*): DFFunc = (df: DataFrame) => df.select(columns.head, columns.tail: _*)

    def apply[_: ClassTag](columns: Column*): DFFunc = (df: DataFrame) => df.select(columns: _*)

    def apply(reorderDF: DataFrame): DFFunc = (df: DataFrame) => df.select(reorderDF.getColumns: _*)

    def apply(joinedDF: DataFrame, alias: String): DFFunc = (df: DataFrame) => df.select(joinedDF.getColumns(alias): _*)
  }




  /*
    Filter Select Transform
  */
  object SelectNot {
    def apply(excludeColumn: String*): DFFunc = (df: DataFrame) => df.select(df.getColumns: _*)

    def apply(joinedDF: DataFrame, alias: String, excludeColumn: Seq[String]): DFFunc =
      (df: DataFrame) => df.select(joinedDF.getColumns(alias, excludeColumn): _*)
  }




  /*
    Filter Transform
  */
  object Filter {
    def apply(filterCondition: String): DFFunc = (df: DataFrame) => df.filter(filterCondition)

    def apply[_: ClassTag](filterCondition: Column): DFFunc = (df: DataFrame) => df.filter(filterCondition)
  }




  /*
      Rename Transform
    */
  object Rename {
    def apply(tuples: (String, String)*): DFFunc = (df: DataFrame) => df.renameColumns(tuples.toMap)

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
    def apply(dropColumns: String*): DFFunc = (df: DataFrame) => df.dropColumns(dropColumns)
  }




  /*
    Column Transformations
  */
  object Transform {
    def apply(tuples: (String, String)*): DFFunc = (df: DataFrame) => df.applyStringExpressions(tuples.toMap)

    def apply[_: ClassTag](tuples: (String, Column)*): DFFunc = (df: DataFrame) => df.applyColumnExpressions(tuples.toMap)

    def apply(columnFunc: ColFunc, columns: String*): DFFunc = (df: DataFrame) =>
      df.applyColumnExpressions(columns.map(column => column -> columnFunc(col(column))).toMap)

    def apply(columnFunc: ColFunc): DFFunc = (df: DataFrame) =>
      df.applyColumnExpressions(df.columns.map(column => column -> columnFunc(col(column))).toMap)
  }




  /*
      Generate Sequence numbers for a column based on the previous Maximum sequence value
   */
  object Sequence {
    def apply(startIndex: Long): DFFunc = (df: DataFrame) => df.generateSequence(startIndex)

    def apply(startIndex: Long, column: String): DFFunc = (df: DataFrame) => df.generateSequence(startIndex, Some(column))
  }




  /*
    Join Transform
   */
  object Join {
    def apply(conditions: String*): DFJoinFunc =
      (ldf: DataFrame, rdf: DataFrame, jType: String) => ldf.join(rdf, conditions.toSeq, jType)

    def apply(conditions: Column): DFJoinFunc =
      (ldf: DataFrame, rdf: DataFrame, jType: String) => ldf.join(rdf, conditions, jType)
  }




  /*
    Group Transform
   */
  object Group {
    def apply(groupColumns: String*): DFGroupFunc =
      (df: DataFrame, columns: Seq[Column]) =>
        df.groupBy(groupColumns.head, groupColumns.tail: _*).agg(columns.head, columns.tail: _*)

    def apply[_: ClassTag](groupColumns: Column*): DFGroupFunc =
      (df: DataFrame, columns: Seq[Column]) =>
        df.groupBy(groupColumns: _*).agg(columns.head, columns.tail: _*)
  }




  /*
    Cube Transform
   */
  object Cube {
    def apply(groupColumns: String*): DFGroupFunc =
      (df: DataFrame, columns: Seq[Column]) =>
        df.cube(groupColumns.head, groupColumns.tail: _*).agg(columns.head, columns.tail: _*)

    def apply[_: ClassTag](groupColumns: Column*): DFGroupFunc =
      (df: DataFrame, columns: Seq[Column]) =>
        df.cube(groupColumns: _*).agg(columns.head, columns.tail: _*)
  }




  /*
    Unstruct the fields in a given Struct as columns.
   */
  object Unstruct {
    def apply(unstructCol: String, keepStructColumn: Boolean = true): DFFunc =
      (df: DataFrame) => df.unstruct(unstructCol, keepStructColumn)
  }




  /*
      Update/Add Surrogate key columns by joining on the 'Key' dimension table.
   */
  object UpdateKeys {
    def apply(columns: Seq[String], keyTable: DataFrame, joinColumn: String, valueColumn: String): DFFunc =
      (df: DataFrame) => df.updateKeys(columns, keyTable, joinColumn, valueColumn)
  }




  implicit class DFImplicits(dataframe: DataFrame) {
    def -->(func: DFFunc): DataFrame = dataframe.transform(func)
  }




  implicit class DFFuncImplicits(dfFunc: DFFunc) {
    def -->(df: DataFrame): DataFrame = df.transform(dfFunc)

    def using(df: DataFrame): DataFrame = df.transform(dfFunc)

    def +(rightDFFunc: DFFunc): DFFunc = dfFunc andThen rightDFFunc
  }





  implicit class DFJoinFuncImplicits(dfJFunc: DFJoinFunc) {

    def inner(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "inner")

    def ><(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "inner")

    def left(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "left")

    def <<(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "left")

    def right(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "right")

    def >>(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "right")

    def full(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "full")

    def <>(rightDF: DataFrame): DFFunc = dfJFunc(_: DataFrame, rightDF, "full")
  }




  implicit class DFGroupFuncImplicits(dfGFunc: DFGroupFunc) {
    def ^(joinExprs: Column*): DFFunc = dfGFunc(_: DataFrame, joinExprs)

    def ^[_: ClassTag](joinExprs: String*): DFFunc = dfGFunc(_: DataFrame, joinExprs.map(expr))
  }

}
