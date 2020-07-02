package com.sope.common.sql

import com.sope.common.sql.Types.TFunc

/**
 * @author mbadgujar
 */
trait SqlDatasetOps[D] {

  def noOp(): TFunc[D] = (dataset: D) => dataset

  def distinct: TFunc[D]

  def limit(size: Int): TFunc[D]

  def union(datasets: D*): TFunc[D]

  def intersect(datasets: D*): TFunc[D]

  def except(datasets: D*): TFunc[D]

}
