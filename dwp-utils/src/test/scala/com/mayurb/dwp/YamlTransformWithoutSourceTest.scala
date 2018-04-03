package com.mayurb.dwp

import com.mayurb.dwp.TestContext._
import com.mayurb.dwp.model.{Date, Product, Transactions}
import com.mayurb.dwp.transform.YamlDataTransform
import org.scalatest.{FlatSpec, Matchers}

/**
  *
  * @author mbadgujar
  */
class YamlTransformWithoutSourceTest extends FlatSpec with Matchers {

  private val sqlContext = getSQlContext
  import sqlContext.implicits._

  val TransactionData = Seq(
    Transactions(1, "pune", "tshirt", "2018-01-01"),
    Transactions(2, "Pune", "jeans", "2018-01-02"),
    Transactions(3, "mumbAi", "shirt", "2018-01-03"),
    Transactions(4, "DELHI", "shirt", "2018-01-03"),
    Transactions(5, "chennai", "shirt", "2018-01-03")
  )

  val productData = Seq(
    Product(1, "tshirt", 3, 4),
    Product(2, "jeans", 5, 1),
    Product(3, "shirt", 6, 7),
    Product(-1, "N.A.", 0, 0),
    Product(0, "N.A.", 0, 0)
  )

  val dateData = Seq(
    Date(1, "2018-01-01"),
    Date(2, "2018-01-02"),
    Date(3, "2018-01-03")
  )

  "Yaml File without Source " should "generate the tranfomation Dataframe correctly" in {
    val transactionsDF = TransactionData.toDF
    val productDF = productData.toDF
    val dateDF = dateData.toDF
    val yamlPath = this.getClass.getClassLoader.getResource("./withoutSourceInfo.yaml").getPath
    val ydt = new YamlDataTransform(yamlPath, transactionsDF, productDF, dateDF)
    val transformedDF = ydt.getTransformedDFs.last._2.persist
    println("transactions ==>")
    transactionsDF.show(false)
    println("product ==>")
    productDF.show(false)
    println("date ==>")
    dateDF.show(false)
    println("transformed ==>")
    transformedDF.show(false)
    transformedDF.count should be(5)
    transformedDF.filter("product = 'shirt'")
      .select("product_id")
      .distinct.collect.head.getAs[Int](0) should be(3)
  }


}
