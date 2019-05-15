package com.sope.etl

import com.sope.etl.TestContext.getSQlContext
import com.sope.etl.model.Transactions
import com.sope.etl.yaml.IntermediateYaml
import org.scalatest.{FlatSpec, Matchers}

/**
  * Data Quality Template Test
  *
  * @author mbadgujar
  */
class DQTest extends FlatSpec with Matchers {

  private val sqlContext = getSQlContext

  import sqlContext.implicits._

  private val transactionData = Seq(
    Transactions(1, "pune", "tshirt", "2018-01-01"),
    Transactions(2, "Pune", "jeans", "2018-01-02"),
    Transactions(3, "mumbAi", "shirt", "2018-01-03"),
    Transactions(4, "", "shirt", "2018-01-03"),
    Transactions(5, "Bangalore", "t-shirt", "22018-01-03"),
    Transactions(6, "Chennai", null, "2018/01/03"),
    Transactions(7, "", null, "2018-12-")
  )

  private val dqResult = {
    val transactionsDF = transactionData.toDF
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
