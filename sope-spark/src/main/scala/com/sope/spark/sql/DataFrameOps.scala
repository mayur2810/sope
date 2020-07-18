package com.sope.spark.sql

import com.sope.common.sql.SqlDatasetOps
import com.sope.common.sql.Types.TFunc
import org.apache.spark.sql.DataFrame

/**
 * @author mbadgujar
 */
object DataFrameOps extends SqlDatasetOps[DataFrame] {

  override def union(datasets: DataFrame*): TFunc[DataFrame] = (df: DataFrame) => datasets.foldLeft(df)(_ union _)

  override def intersect(datasets: DataFrame*): TFunc[DataFrame] =
    (df: DataFrame) => datasets.foldLeft(df)(_ intersect _)

  override def except(datasets: DataFrame*): TFunc[DataFrame] = (df: DataFrame) => datasets.foldLeft(df)(_ except _)

  override def distinct: TFunc[DataFrame] = (df: DataFrame) => df.distinct

  override def limit(size: Int): TFunc[DataFrame] = (df: DataFrame) => df.limit(size)

}
