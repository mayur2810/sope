package com.sope.common.sql.dsl

import com.sope.common.sql.SqlOps
import com.sope.common.sql.Types._

trait DSL {

  object NoOp {
    def apply[D, C]()(implicit sqlOps: SqlOps[D, C]): TFunc[D] = sqlOps.noOp()
  }

  object Select {
    def apply[D, C](columns: C*)(implicit sqlOps: SqlOps[D, C]): TFunc[D] = sqlOps.select(columns: _*)
  }


  object Rename {
    def apply[D, C](tuples: (String, String)*)(implicit sqlOps: SqlOps[D, C]): TFunc[D] = sqlOps.rename(tuples: _*)
  }

  object Drop {
    def apply[D, C](columns: String*)(implicit sqlOps: SqlOps[D, C]): TFunc[D] = sqlOps.drop(columns: _*)
  }

  object Filter {
    def apply[D, C](condition: C)(implicit sqlOps: SqlOps[D, C]): TFunc[D] = sqlOps.filter(condition)
  }

  object Transform {
    def apply[D, C](columns: (String, C)*)(implicit sqlOps: SqlOps[D, C]): TFunc[D] = sqlOps.transform(columns: _*)
  }

  object TransformAll {
    def apply[D, C](singleArgFunction: String, columns: (String, C)*)(implicit sqlOps: SqlOps[D, C]): TFunc[D] =
      sqlOps.transformAll(singleArgFunction, columns: _*)
  }

  object TransformAllMultiArg {
    def apply[D, C](multiArgFunction: String, columns: (String, Seq[C])*)(implicit sqlOps: SqlOps[D, C]): TFunc[D] =
      sqlOps.transformAll(multiArgFunction, columns: _*)
  }

  object Join {
    def apply[D, C](columns: String*)(implicit sqlOps: SqlOps[D, C]): JFunc[D] = sqlOps.join(columns: _*)
  }

  object JoinExpr {
    def apply[D, C](expressions: C*)(implicit sqlOps: SqlOps[D, C]): JFunc[D] = sqlOps.joinExprs(expressions: _*)
  }

  object Aggregate {
    def apply[D, C](expressions: (String, C)*)(implicit sqlOps: SqlOps[D, C]): TFunc[D] =
      sqlOps.aggregate(expressions: _*)
  }

  object GroupBy {
    def apply[D, C](groupingColumns: C*)(implicit sqlOps: SqlOps[D, C]): GFunc[D, C] =
      sqlOps.groupBy(groupingColumns: _*)
  }

  object Union {
    def apply[D, C](datasets: D*)(implicit sqlOps: SqlOps[D, C]): TFunc[D] = sqlOps.union(datasets: _*)
  }

  object Intersect {
    def apply[D, C](datasets: D*)(implicit sqlOps: SqlOps[D, C]): TFunc[D] = sqlOps.intersect(datasets: _*)
  }

  object Except {
    def apply[D, C](datasets: D*)(implicit sqlOps: SqlOps[D, C]): TFunc[D] = sqlOps.except(datasets: _*)
  }

  object OrderBy {
    def apply[D, C](columns: C*)(implicit sqlOps: SqlOps[D, C]): TFunc[D] = sqlOps.orderBy(columns: _*)
  }

  object Distinct {
    def apply[D, C]()(implicit sqlOps: SqlOps[D, C]): TFunc[D] = sqlOps.distinct
  }


  implicit class TFuncImplicits[D](tFunc: TFunc[D]) {

    def using(dataset: D): D = tFunc(dataset)

    def -->(dataset: D): D = using(dataset)

    def +(rightTFunc: TFunc[D]): TFunc[D] = tFunc andThen rightTFunc
  }


  implicit class DSJoinFuncImplicits[D](dsJFunc: JFunc[D]) {

    def innerType: TFunc2[D] = dsJFunc("INNER")

    def inner(rightDS: D): TFunc[D] = innerType(rightDS)

    def ><(rightDS: D): TFunc[D] = inner(rightDS)

    def leftType: TFunc2[D] = dsJFunc("LEFT")

    def left(rightDS: D): TFunc[D] = leftType(rightDS)

    def <<(rightDS: D): TFunc[D] = left(rightDS)

    def rightType: TFunc2[D] = dsJFunc("RIGHT")

    def right(rightDS: D): TFunc[D] = rightType(rightDS)

    def >>(rightDS: D): TFunc[D] = right(rightDS)

    def fullType: TFunc2[D] = dsJFunc("FULL")

    def full(rightDS: D): TFunc[D] = fullType(rightDS)

    def <>(rightDS: D): TFunc[D] = full(rightDS)
  }

  // TODO Impl for other C types?
  implicit class DSGroupByFuncImplicits[D](dsGFunc: GFunc[D, String]) {
    def agg(aggExprs: (String, String)*): TFunc[D] = dsGFunc(aggExprs)

    def ^(aggExprs: (String, String)*): TFunc[D] = agg(aggExprs: _*)
  }

}
