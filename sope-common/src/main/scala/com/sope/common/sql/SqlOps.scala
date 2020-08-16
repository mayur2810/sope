package com.sope.common.sql

import com.sope.common.sql.Types._

trait SqlOps[D, C, CF] {

  def columns(dataset: D): Seq[C]

  def aliasedColumn(datasetAlias: String, column: C): C

  def columnName(column:C): String

  def select(columns: C*): TFunc[D]

  def transform(columns: (String, C)*): TFunc[D]

  def transformAll(singleArgFunction: ColFunc[CF], columns: (String, C)*): TFunc[D]

  def transformAllMultiArg(multiArgFunction: MultiColFunc[CF], columns: (String, Seq[C])*): TFunc[D]

  def filter(condition: C): TFunc[D]

  def rename(columns: (C, String)*): TFunc[D]

  def drop(columns: C*): TFunc[D]

  def join(columns: C*): JFunc[D]

  def joinExprs(expressions: C*): JFunc[D]

  def aggregate(aggExprs: (String, C)*): TFunc[D]

  def groupBy(groupColumns: C*): GFunc[D, C]

  def orderBy(columns: C*): TFunc[D]
}
