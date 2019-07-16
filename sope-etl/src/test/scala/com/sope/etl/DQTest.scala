package com.sope.etl

import com.sope.etl.TestContext.getSQlContext
import com.sope.etl.yaml.{IntermediateYaml, ParallelizeYaml}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Data Quality Template Test
  *
  * @author mbadgujar
  */
class DQTest extends FlatSpec with Matchers {

  private val sqlContext = getSQlContext
  private val transactionData = ParallelizeYaml("data/transactions_dq.yaml").parallelize(sqlContext)

  private val dqResult = {
    val transactionsDF = transactionData
    IntermediateYaml("templates/data_quality_template.yaml", Some(Map("null_check_cols" -> Seq("product"),
      "date_format" -> "yyyy-mm-dd", "empty_check_cols" ->  Seq("product", "loc"), "  date_check_cols  " -> Seq("date"))))
      .getTransformedDFs(transactionsDF).toMap
  }

  "Data Quality Check" should "generate the DQ results correctly" in {
    val dqResultDF = dqResult("dq_output").cache
    val expectedResult = Map(
      "is_empty_dq_failed_columns" -> Seq((4, "loc"), (6, "product"), (7, "product,loc")),
      "is_null_dq_failed_columns" -> Seq((6, "product"), (7, "product")),
      "date_check_dq_failed_columns" -> Seq((6, "date"), (7, "date"))
    )
    dqResultDF.show(false)
    expectedResult
      .flatMap { case (dq_res_col, result) => result.map(data => (dq_res_col, data._1, data._2)) }
      .foreach { case (dq_res_col, index, expected) => dqResultDF.filter(s"id = $index")
        .collect.head.getAs[String](dq_res_col) should be(expected)
      }
  }
}
