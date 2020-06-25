package com.sope.common.sql

import com.sope.common.sql.Types._

import scala.reflect.ClassTag


trait SqlOps[D, C] {

  def noOp(): TFunc[D] = (dataset: D) => dataset

  def select(columns: C*): TFunc[D]

  def transform(columns: (String, C)*): TFunc[D]

  def transformAll(singleArgFunctionName: String, columns: (String, C)*): TFunc[D]

  def transformAll[T: ClassTag](multiArgFunctionName: String, columns: (String, Seq[C])*): TFunc[D]

  def filter(condition: C): TFunc[D]

  def rename(tuples: (String, String)*): TFunc[D]

  def drop(columns: String*): TFunc[D]

  def join(columns: String*): JFunc[D]

  def joinExprs(columns: C*): JFunc[D]

  def aggregate(aggExprs: (String, C)*): TFunc[D]

  def groupBy(groupColumns: C*): GFunc[D, C]

  def union(datasets: D*): TFunc[D]

  def intersect(datasets: D*): TFunc[D]

  def except(datasets: D*): TFunc[D]

  def orderBy(columns: C*): TFunc[D]

  def distinct : TFunc[D]

  def limit: TFunc[D]
}
