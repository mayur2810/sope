package com.sope.spark.sql

import com.sope.common.sql.{SqlOps, Types}
import com.sope.common.sql.Types._
import org.apache.spark.sql.{Column, DataFrame}


/**
 * @author mbadgujar
 */
object ColumnExprSqlOps extends SqlOps[DataFrame, Column, Column] {

  override def columns(dataset: DataFrame): Seq[Column] = dataset.columns.map(dataset.col)

  override def columnName(column: Column): String = column.toString

  override def select(columns: Column*): TFunc[DataFrame] = (df: DataFrame) => df.select(columns: _*)

  override def transform(columns: (String, Column)*): TFunc[DataFrame] =
    (df: DataFrame) => df.applyColumnExpressions(columns.toMap)

  override def transformAll(singleArgFunction: Types.ColFunc[Column], columns: (String, Column)*): TFunc[DataFrame] = {
    val transformMap =
      columns
        .toMap
        .mapValues(columnExpr => singleArgFunction(columnExpr))
    (df: DataFrame) => df.applyColumnExpressions(transformMap)
  }

  override def transformAllMultiArg(multiArgFunction: Types.MultiColFunc[Column], columns: (String, Seq[Column])*)
  : TFunc[DataFrame] = {
    val transformMap =
      columns
        .toMap
        .mapValues(columnExpr => multiArgFunction(columnExpr))
    (df: DataFrame) => df.applyColumnExpressions(transformMap)
  }

  override def filter(condition: Column): TFunc[DataFrame] = (df: DataFrame) => df.filter(condition)

  override def rename(columns: (String, Column)*): TFunc[DataFrame] = (df: DataFrame) =>
    df.renameColumns(columns.map { case (newName, column) => (column.toString, newName) }.toMap)

  override def drop(columns: Column*): TFunc[DataFrame] = (df: DataFrame) =>
    columns.foldLeft(df) {
      (df, column) => df.drop(column)
    }

  override def join(columns: Column*): JFunc[DataFrame] = (joinType: String) => (rdf: DataFrame) => (ldf: DataFrame) =>
    ldf.join(rdf, columns.map(_.toString()), joinType)

  override def joinExprs(expressions: Column*): JFunc[DataFrame] = (joinType: String) => (rdf: DataFrame) =>
    (ldf: DataFrame) =>
    ldf.join(rdf, expressions.reduce(_ and _), joinType)

  override def aggregate(aggExprs: (String, Column)*): TFunc[DataFrame] = {
    val aggregateExprs = aggExprs.map { case (name, aggExpr) => aggExpr.as(name) }
    df: DataFrame => df.agg(aggregateExprs.head, aggregateExprs.tail: _*)
  }

  override def groupBy(groupColumns: Column*): GFunc[DataFrame, Column] =
    (aggExprs: Seq[(String, Column)])
  => (df: DataFrame)
    => {
      val aggregateExprs = aggExprs.map { case (name, aggExpr) => aggExpr.as(name) }
      df.groupBy(groupColumns: _*).agg(aggregateExprs.head, aggregateExprs.tail: _*)
    }


  override def orderBy(columns: Column*): TFunc[DataFrame] = (df: DataFrame) => df.orderBy(columns: _*)


}
