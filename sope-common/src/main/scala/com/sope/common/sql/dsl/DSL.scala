package com.sope.common.sql.dsl

import com.sope.common.sql.{SqlColumnOps, SqlDatasetOps, SqlOps}
import com.sope.common.sql.Types._

trait DSL {

  object NoOp {
    def apply[D, C, CF]()(implicit sqlDatasetOps: SqlDatasetOps[D]): TFunc[D] = sqlDatasetOps.noOp()
  }

  object Select {
    def apply[D, C, CF](columns: C*)(implicit sqlOps: SqlOps[D, C, CF]): TFunc[D] = sqlOps.select(columns: _*)

    def apply[D, C, CF](pattern: String)(implicit sqlOps: SqlOps[D, String, CF]): TFunc[D] = (dataset: D) => {
      val columnsToSelect = sqlOps.columns(dataset).filter(column => sqlOps.columnName(column).matches(pattern))
      sqlOps.select(columnsToSelect :_*)(dataset)
    }

  }

  /*
      Select columns from a dataset which was joined using aliased dataset.
      Useful if you want to get a structure of pre-joined dataframe and include some join columns from opposite side
      of join.
   */
  object SelectReorder {
    def apply[D, C, CF](reorderDataset: D)(implicit sqlOps: SqlOps[D, String, CF]): TFunc[D] = (dataset: D) => {
      sqlOps.select(sqlOps.columns(reorderDataset) :_*)(dataset)
    }
  }

  object SelectAliased {
    def apply[D, C, CF](priorDataset: D, alias: String, includeColumns: Seq[C] = Nil, excludeColumns: Seq[C] = Nil)
                       (implicit sqlOps: SqlOps[D, C, CF]): TFunc[D] = {
     val aliasedColumns =  sqlOps.columns(priorDataset)
        .filterNot(excludeColumns.contains(_))
        .map(column => sqlOps.aliasedColumn(alias, column))
      (dataset: D) => sqlOps.select(aliasedColumns ++ includeColumns: _*)(dataset)
    }
  }


  object Rename {
    def apply[D, C, CF](tuples: (C, String)*)(implicit sqlOps: SqlOps[D, C, CF]): TFunc[D] = sqlOps.rename(tuples: _*)
  }

  object RenameAll {

    private def appendFunc[D, C, CF](prefix: Boolean, append: String, columns: Seq[C])(implicit sqlOps: SqlOps[D, C, CF]):
    Seq[(C, String)] = {
      if (prefix)
        columns.map(column =>   column -> s"$append${sqlOps.columnName(column)}")
      else
        columns.map(column => column -> s"${sqlOps.columnName(column)}$append")
    }

    def apply[D, C, CF](append: String, prefix: Boolean, columns: C*)
                       (implicit sqlOps: SqlOps[D, C, CF]): TFunc[D] = {
      (dataset: D) => {
        val columnsToRename = if (columns.isEmpty) sqlOps.columns(dataset) else columns
        sqlOps.rename(appendFunc(prefix, append, columnsToRename) :_*)(dataset)
      }
    }

    def apply[D, C, CF](append: String, prefix: Boolean, pattern: String)
                       (implicit sqlOps: SqlOps[D, String, CF]): TFunc[D] = {
      (dataset: D) => {
        val columnsToRename =  sqlOps.columns(dataset)
          .filter(column => sqlOps.columnName(column).matches(pattern))
        sqlOps.rename(appendFunc(prefix, append, columnsToRename) :_*)(dataset)
      }
    }

    def apply[D, C, CF](pattern: String, find: String, replace: String)
                       (implicit sqlOps: SqlOps[D, String, CF]): TFunc[D] = {
      (dataset: D) => {
        val columnsToRename =  sqlOps.columns(dataset)
          .filter(column => sqlOps.columnName(column).matches(pattern))
          .map(column => column -> sqlOps.columnName(column).replaceAll(find, replace))
        sqlOps.rename(columnsToRename :_*)(dataset)
      }
    }

  }

  object Drop {
    def apply[D, C, CF](columns: C*)(implicit sqlOps: SqlOps[D, C, CF]): TFunc[D] = sqlOps.drop(columns: _*)
  }

  object Filter {
    def apply[D, C, CF](condition: C)(implicit sqlOps: SqlOps[D, C, CF]): TFunc[D] = sqlOps.filter(condition)
  }

  object Transform {
    def apply[D, C, CF](columns: (String, C)*)
                       (implicit sqlOps: SqlOps[D, C, CF]): TFunc[D] = sqlOps.transform(columns: _*)
  }

  object TransformAll {
    def apply[D, C, CF](singleArgFunction: ColFunc[CF], suffix: Option[String], columns: C*)
                       (implicit sqlOps: SqlOps[D, C, CF]): TFunc[D] = (dataset: D) => {
      val transformColumns = if (columns.isEmpty) sqlOps.columns(dataset) else columns
      val columnMap = transformColumns
        .map(column => sqlOps.columnName(column) -> column)
        .map { case (name, column) =>
          val newName = suffix.fold(name)(someSuffix => name + "_" + someSuffix)
          newName -> column
        }
      sqlOps.transformAll(singleArgFunction, columnMap: _*)(dataset)
    }


    def apply[D, C, CF](singleArgFunction: ColFunc[CF], suffix: Option[String], pattern: String)
                       (implicit sqlOps: SqlOps[D, String, CF]): TFunc[D] =
      (dataset: D) => {
        val transformColumns = sqlOps.columns(dataset)
          .map(column => sqlOps.columnName(column) -> column)
          .filter { case (name, _) => name.matches(pattern) }
        val columnMap = transformColumns
          .map { case (name, column) =>
            val newName = suffix.fold(name)(someSuffix => name + "_" + someSuffix)
            newName -> column
          }
        sqlOps.transformAll(singleArgFunction, columnMap: _*)(dataset)
      }

    def apply[D, C, CF](singleArgFunctionName: String, suffix: Option[String], columns: C*)
                       (implicit sqlOps: SqlOps[D, C, CF], colOps: SqlColumnOps[CF]): TFunc[D] =
      apply(colOps.resolveSingleArgFunction(singleArgFunctionName), suffix, columns: _*)

    def apply[D, C, CF](singleArgFunctionName: String, suffix: Option[String], pattern: String)
                       (implicit sqlOps: SqlOps[D, String, CF], colOps: SqlColumnOps[CF]): TFunc[D] =
      apply(colOps.resolveSingleArgFunction(singleArgFunctionName), suffix, pattern)
  }

  object TransformAllMultiArg {
    def apply[D, C, CF](multiArgFunction: MultiColFunc[CF], columns: (String, Seq[C])*)(implicit sqlOps: SqlOps[D, C,
      CF]): TFunc[D] =
      sqlOps.transformAllMultiArg(multiArgFunction, columns: _*)

    def apply[D, C, CF](multiArgFunctionName: String, columns: (String, Seq[C])*)
                       (implicit sqlOps: SqlOps[D, C, CF], colOps: SqlColumnOps[CF]): TFunc[D] =
      sqlOps.transformAllMultiArg(colOps.resolveMultiArgFunction(multiArgFunctionName), columns: _*)
  }

  object Join {
    def apply[D, C, CF](columns: C*)(implicit sqlOps: SqlOps[D, C, CF]): JFunc[D] = sqlOps.join(columns: _*)
  }

  object JoinExpr {
    def apply[D, C, CF](expressions: C*)(implicit sqlOps: SqlOps[D, C, CF]): JFunc[D] =
      sqlOps.joinExprs(expressions: _*)
  }

  object Aggregate {
    def apply[D, C, CF](expressions: (String, C)*)(implicit sqlOps: SqlOps[D, C, CF]): TFunc[D] =
      sqlOps.aggregate(expressions: _*)
  }

  object GroupBy {
    def apply[D, C, CF](groupingColumns: C*)(implicit sqlOps: SqlOps[D, C, CF]): GFunc[D, C] =
      sqlOps.groupBy(groupingColumns: _*)
  }

  object Union {
    def apply[D, C, CF](datasets: D*)(implicit sqlDatasetOps: SqlDatasetOps[D]): TFunc[D] =
      sqlDatasetOps.union(datasets: _*)
  }

  object Intersect {
    def apply[D, C, CF](datasets: D*)(implicit sqlDatasetOps: SqlDatasetOps[D]): TFunc[D] =
      sqlDatasetOps.intersect(datasets: _*)
  }

  object Except {
    def apply[D, C, CF](datasets: D*)(implicit sqlDatasetOps: SqlDatasetOps[D]): TFunc[D] =
      sqlDatasetOps.except(datasets: _*)
  }

  object Limit {
    def apply[D, C, CF](size: Int)(implicit sqlDatasetOps: SqlDatasetOps[D]): TFunc[D] =
      sqlDatasetOps.limit(size)
  }

  object OrderBy {
    def apply[D, C, CF](columns: C*)(implicit sqlOps: SqlOps[D, C, CF]): TFunc[D] = sqlOps.orderBy(columns: _*)
  }

  object Distinct {
    def apply[D, C, CF]()(implicit sqlDatasetOps: SqlDatasetOps[D]): TFunc[D] = sqlDatasetOps.distinct
  }


  implicit class TFuncImplicits[D](tFunc: TFunc[D]) {

    def using(dataset: D): D = tFunc(dataset)

    def -->(dataset: D): D = using(dataset)

    def +(rightTFunc: TFunc[D]): TFunc[D] = tFunc andThen rightTFunc

    def +(rightDFFuncSeq: Seq[TFunc[D]]): Seq[TFunc[D]] = rightDFFuncSeq.map(tFunc + _)

    def +(rightDFFuncMap: Map[String, TFunc[D]]): Map[String, TFunc[D]] = rightDFFuncMap.map {
      case (name, rightFunc) => name -> (tFunc + rightFunc)
    }

    def +(dfFuncMapSelectiveLeft: (String, Map[String, TFunc[D]])): Map[String, TFunc[D]] =
      dfFuncMapSelectiveLeft match {
        case (name, dfFuncMap) => dfFuncMap.map {
          case (key, rightFunc) => if (name == key) key -> (tFunc + rightFunc) else key -> rightFunc
        }
      }
  }

  implicit class SeqDFFuncImplicits[D](dfFuncSeq: Seq[TFunc[D]]) {

    def using(df: D): Seq[D] = dfFuncSeq.map(_ using df)

    def -->(df: D): Seq[D] = using(df)

    def +(rightDFFunc: TFunc[D]): Seq[TFunc[D]] = dfFuncSeq.map(_ + rightDFFunc)

    def +(rightDFFuncs: Seq[TFunc[D]]): Seq[TFunc[D]] = dfFuncSeq.flatMap(left => rightDFFuncs.map(left + _))
  }

  implicit class MapDFFuncImplicits[D](dfFuncMap: Map[String, TFunc[D]]) {

    def using(df: D): Map[String, D] = dfFuncMap.mapValues(_ using df)

    def -->(df: D): Map[String, D] = using(df)


    def +(rightDFFunc: TFunc[D]): Map[String, TFunc[D]] = dfFuncMap.mapValues(_ + rightDFFunc)

    def +(name: String, rightDFFunc: TFunc[D]): Map[String, TFunc[D]] = dfFuncMap.map {
      case (key, func) => if (name == key) key -> (func + rightDFFunc) else key -> func
    }

    def +(rightDFFuncs: Map[String, TFunc[D]]): Map[String, TFunc[D]] = dfFuncMap.flatMap {
      case (leftKey, leftFunc) =>
        rightDFFuncs.map { case (rightKey, rightFunc) => (leftKey + rightKey) -> (leftFunc + rightFunc) }
    }
  }

  implicit class MapDFFuncSelectiveImplicits[D](dfFuncMapSelective: (String, Map[String, TFunc[D]])) {
    def +(rightDFFunc: TFunc[D]): Map[String, TFunc[D]] = dfFuncMapSelective match {
      case (name, dfFuncMap) => dfFuncMap.map {
        case (key, func) => if (name == key) key -> (func + rightDFFunc) else key -> func
      }
    }
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
