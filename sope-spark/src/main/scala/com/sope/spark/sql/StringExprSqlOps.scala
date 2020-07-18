package com.sope.spark.sql

import com.sope.common.sql.Types.{GFunc, JFunc, TFunc}
import com.sope.common.sql.{SqlOps, Types}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
 * @author mbadgujar
 */
object StringExprSqlOps extends SqlOps[DataFrame, String, Column] {

  //
  //  /**
  //   * Applies know arguments to MultiArg Function in order provided and returns
  //   * a single arg function to which the actual column can be provided
  //   *
  //   * @param multiArgFunc Multi Argument Column Function
  //   * @param columns      Columns to be applied
  //   * @return [[ColFunc]]
  //   */
  //  protected def multiArgToSingleArgFunc(multiArgFunc: MultiColFunc, columns: Seq[Column]): ColFunc =
  //    (column: Column) => multiArgFunc(column +: columns)
  //


  override def select(columns: String*): TFunc[DataFrame] = (df: DataFrame) => df.selectExpr(columns: _*)

  override def transform(columns: (String, String)*): TFunc[DataFrame] =
    (df: DataFrame) => df.applyStringExpressions(columns.toMap)


  override def transformAll(singleArgFunction: Types.ColFunc[Column], columns: (String, String)*): TFunc[DataFrame] = {
    val transformMap =
      columns
        .toMap
        .mapValues(columnExpr => singleArgFunction(expr(columnExpr)))
    (df: DataFrame) => df.applyColumnExpressions(transformMap)
  }


  override def transformAllMultiArg(multiArgFunction: Types.MultiColFunc[Column], columns: (String, Seq[String])*)
  : TFunc[DataFrame] = {
    val transformMap =
      columns
        .toMap
        .mapValues(columnExprs => multiArgFunction(columnExprs.map(expr)))
    (df: DataFrame) => df.applyColumnExpressions(transformMap)
  }

  override def filter(condition: String): TFunc[DataFrame] = (df: DataFrame) => df.filter(condition)

  override def rename(columns: (String, String)*): TFunc[DataFrame] = (df: DataFrame) => df.renameColumns(columns.toMap)

  override def drop(columns: String*): TFunc[DataFrame] = (df: DataFrame) => df.drop(columns :_*)

  override def join(columns: String*): JFunc[DataFrame] = (joinType: String) => (rdf: DataFrame) => (ldf: DataFrame) =>
    ldf.join(rdf, columns.toSeq, joinType)

  override def joinExprs(expressions: String*): JFunc[DataFrame] =
    (joinType: String) => (rdf: DataFrame) => (ldf: DataFrame) =>
      ldf.join(rdf, expressions.map(expr).reduce(_ and _), joinType)

  override def aggregate(aggExprs: (String, String)*): TFunc[DataFrame] = {
    val aggregateExprs = aggExprs.map { case (name, aggExpr) => expr(aggExpr).as(name) }
    df: DataFrame => df.agg(aggregateExprs.head, aggregateExprs.tail: _*)
  }

  override def groupBy(groupColumns: String*): GFunc[DataFrame, String] =
    (aggExprs: Seq[(String, String)])
    => (df: DataFrame)
    => {
      val aggregateExprs = aggExprs.map { case (name, aggExpr) => expr(aggExpr).as(name) }
      df.groupBy(groupColumns.map(expr): _*).agg(aggregateExprs.head, aggregateExprs.tail: _*)
    }

  override def orderBy(columns: String*): TFunc[DataFrame] = (df: DataFrame) =>
    df.orderBy(columns.map(exp =>
      if (exp.toLowerCase.endsWith("desc"))
        desc(exp.split(":").head)
      else
        col(exp.split(":").head)): _*)

}