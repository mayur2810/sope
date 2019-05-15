package com.sope.etl

import java.sql.{Date => SDate}

import com.sope.etl.TestContext._
import com.sope.etl.model.{Date, Product, ProductDim, Transactions}
import com.sope.etl.yaml.IntermediateYaml
import org.scalatest.{FlatSpec, Matchers}

/**
  * Yaml Transformer Unit tests
  *
  * @author mbadgujar
  */
class YamlTransformWithoutSourceTest extends FlatSpec with Matchers {

  private val sqlContext = getSQlContext

  import sqlContext.implicits._

  private val transactionData = Seq(
    Transactions(1, "pune", "tshirt", "2018-01-01"),
    Transactions(2, "Pune", "jeans", "2018-01-02"),
    Transactions(3, "mumbAi", "shirt", "2018-01-03"),
    Transactions(4, "DELHI", "shirt", "2018-01-03"),
    Transactions(5, "chennai", "shirt", "2018-01-03")
  )

  private val productData = Seq(
    Product(1, "tshirt", 3, 4, "popular"),
    Product(2, "jeans", 5, 1, "not-popular"),
    Product(3, "shirt", 6, 8, "popular"),
    Product(4, "trouser", 3, 5, "popular"),
    Product(6, "cap", 2, 9, "not-popular")
  )

  private val productDimData = Seq(
    ProductDim(1l, -1, "N.A.", 0, 0, "not-available", SDate.valueOf("2018-01-01")),
    ProductDim(2l, 0, "N.A.", 0, 0, "not-available", SDate.valueOf("2018-01-01")),
    ProductDim(3l, 1, "tshirt", 3, 4, "not-available", SDate.valueOf("2018-01-01")),
    ProductDim(4l, 2, "jeans", 5, 1, "not-available", SDate.valueOf("2018-01-01")),
    ProductDim(5l, 3, "shirt", 6, 7, "not-available", SDate.valueOf("2018-01-01")),
    ProductDim(6l, 4, "trouser", 4, 3, "not-available", SDate.valueOf("2018-01-01")),
    ProductDim(7l, 5, "kurta", 4, 4, "not-available", SDate.valueOf("2018-01-01"))

  )
  private val dateData = Seq(
    Date(1, "2018-01-01"),
    Date(2, "2018-01-02"),
    Date(3, "2018-01-03")
  )

  private val transformedResult = {
    System.setProperty(UDFRegistrationClassProperty, "com.sope.etl.custom.CustomUDF")
    System.setProperty(TransformationRegistrationClassProperty, "com.sope.etl.custom.CustomTransformation")
    val transactionsDF = transactionData.toDF
    val productDF = productData.toDF
    val dateDF = dateData.toDF
    val productDimDF = productDimData.toDF
    IntermediateYaml("withoutSourceInfo.yaml").getTransformedDFs(transactionsDF, productDF, dateDF, productDimDF).toMap
  }


  "trxn_transformed" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("trxn_transformed")
    transformedDF.show(false)
    transformedDF.count should be(5)
    transformedDF.filter("product = 'shirt'")
      .select("product_id")
      .distinct.collect.head.getAs[Int](0) should be(3)
  }


  "grp_by" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("grp_by")
    transformedDF.show(false)
    transformedDF.count should be(3)
    transformedDF.filter("product = 'shirt'")
      .select("p_cnt")
      .distinct.collect.head.getAs[Int](0) should be(3)
  }


  "grp_by_pivot" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("grp_by_pivot")
    transformedDF.show(false)
    transformedDF.count should be(3)
    transformedDF.filter("product = 'shirt'")
      .select("2018")
      .distinct.collect.head.getAs[Int](0) should be(3)
  }

  "product scd-1" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("final_dim_out")
    transformedDF.show(false)
    transformedDF.count should be(8)
    transformedDF.filter("scd_status = 'NCD'").count should be(5)
    transformedDF.filter("scd_status = 'INSERT'").count should be(1)
    transformedDF.filter("scd_status = 'UPDATE'").count should be(2)
  }

  "distinct_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("distinct_test")
    transformedDF.show(false)
    transformedDF.count should be(8)
  }

  "drop_duplicate_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("drop_duplicate_test")
    transformedDF.show(false)
    transformedDF.count should be(3)
  }

  "aliasing_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("aliasing_test")
    transformedDF.show(false)
    transformedDF.count should be(5)
  }


  "na_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("na_test")
    transformedDF.show(false)
    transformedDF.filter("product_key = -1").count should be(3)
  }

  "limit_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("limit_test")
    transformedDF.show(false)
    transformedDF.count should be(2)
  }

  "unstruct_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("unstruct_test")
    transformedDF.show(false)
    transformedDF.columns.length should be(5)
  }

  "intersect_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("intersect_test")
    transformedDF.show(false)
    transformedDF.count should be(2)
  }

  "except_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("except_test")
    transformedDF.show(false)
    transformedDF.count should be(0)
  }

  "transform_all_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("transform_all_test")
    transformedDF.show(false)
    transformedDF.columns.length should be(6)
  }

  "transform_whole_table_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("transform_whole_table_test")
    transformedDF.show(false)
    transformedDF.columns.length should be(8)
  }

  "rename_all_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("rename_all_test")
    transformedDF.show(false)
    transformedDF.columns.forall(_.contains("_renamed"))
  }

  "custom_udf_call_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("custom_udf_call_test")
    transformedDF.show(false)
    transformedDF.columns should contain("upper_loc")
  }

  "custom_transform_call_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("custom_transform_call_test")
    transformedDF.show(false)
    transformedDF.columns should contain("new_column")
  }

  "aggregate_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("aggregate_test")
    transformedDF.show(false)
    transformedDF.collect().head.getInt(0) should be(5)
  }

  "partitioning_test" should "generate the transformation Dataframes correctly" in {
    val transformedDF = transformedResult("p_set")
    transformedDF.show(false)
    transformedDF.count should be(2)
    val transformedDF1 = transformedResult("np_set")
    transformedDF1.show(false)
    transformedDF1.count should be(3)
  }

  "router_test" should "generate the transformation Dataframes correctly" in {
    val tSet = transformedResult("t_set")
    tSet.show(false)
    tSet.count() should be(2)
    val lSet = transformedResult("l_set")
    lSet.show(false)
    lSet.count() should be(2)
    val noMatchSet = transformedResult("no_match_set")
    noMatchSet.show(false)
    noMatchSet.count() should be(3)
  }

  "repartition_test" should "generate the transformation Dataframe correctly" in {
    val transformedDF = transformedResult("repartition_test")
    transformedDF.rdd.partitions.length should be(10)
  }
}
