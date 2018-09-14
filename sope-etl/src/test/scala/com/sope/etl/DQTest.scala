package com.sope.etl

import com.sope.etl.TestContext.getSQlContext
import com.sope.etl.dq.DQTransform
import com.sope.etl.model.Transactions
import org.scalatest.{FlatSpec, Matchers}

/**
  * [[DQTransform]] Test
  *
  * @author mbadgujar
  */
class DQTest extends FlatSpec with Matchers {

  private val sqlContext = getSQlContext
  import sqlContext.implicits._

  val TransactionData = Seq(
    Transactions(1, "pune", "tshirt", "2018-01-01"),
    Transactions(2, "Pune", "jeans", "2018-01-02"),
    Transactions(0, "mumbAi", "", "2018-01-03"),
    Transactions(4, null, "shirt", "2018-01-03"),
    Transactions(5, "chennai", "shirt", "2011:01:03")
  )

  "Yaml File for DQ Check" should "generate the tranfomation Dataframe correctly" in {
    val transactionsDF = TransactionData.toDF
    val yamlPath = this.getClass.getClassLoader.getResource("./dq.yaml").getPath
    val dq = new DQTransform(yamlPath, transactionsDF)
    val dqDF = dq.performChecks.persist
    dqDF.show(false)
    dqDF.filter("`loc_null-check_dq` = true").count should be(1)
    dqDF.filter("`product_empty-check_dq` = true").count should be(1)
    dqDF.filter("`date_date-check_dq` = true").count should be(1)
  }
}
