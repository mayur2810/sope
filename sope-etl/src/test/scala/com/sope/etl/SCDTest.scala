package com.sope.etl

import java.sql.Date

import com.sope.etl.TestContext.getSQlContext
import com.sope.etl.model.{Product, ProductDim}
import com.sope.etl.scd.DimensionTable
import org.apache.spark.SparkException
import org.scalatest.{FlatSpec, Matchers}

/**
  * Unit Tests for Dimension SCD
  *
  * @author mbadgujar
  */
class SCDTest extends FlatSpec with Matchers {

  private val sqlContext = getSQlContext

  import sqlContext.implicits._

  val productData = Seq(
    Product(1, "tshirt", 3, 4, "popular"),
    Product(2, "jeans", 5, 1, "not-popular"),
    Product(3, "shirt", 6, 8, "popular"),
    Product(4, "trouser", 3, 5, "popular"),
    Product(6, "cap", 2, 9, "not-popular")
  )

  val productDimData = Seq(
    ProductDim(1l, -1, "N.A.", 0, 0, "not-available", Date.valueOf("2018-01-01")),
    ProductDim(2l, 0, "N.A.", 0, 0, "not-available", Date.valueOf("2018-01-01")),
    ProductDim(3l, 1, "tshirt", 3, 4, "not-available", Date.valueOf("2018-01-01")),
    ProductDim(4l, 2, "jeans", 5, 1, "not-available", Date.valueOf("2018-01-01")),
    ProductDim(5l, 3, "shirt", 6, 7, "not-available", Date.valueOf("2018-01-01")),
    ProductDim(6l, 4, "trouser", 4, 3, "not-available", Date.valueOf("2018-01-01")),
    ProductDim(7l, 5, "kurta", 4, 4, "not-available", Date.valueOf("2018-01-01"))

  )

  "Dimension table SCD " should "throw exception for incorrect surrogate Key column in dimension table" in {
    intercept[SparkException] {
      new DimensionTable(productDimData.toDF(), "product_ky", Seq("product_id"), metaColumns = Seq("last_updated_date"))
    }
  }

  "Dimension table SCD " should "throw exception for incorrect Natural Keys column in dimension table" in {
    intercept[SparkException] {
      new DimensionTable(productDimData.toDF(), "product_key", Seq("product_d"), metaColumns = Seq("last_updated_date"))
    }
  }

  "Dimension table SCD " should "throw exception for incorrect Meta Keys column in dimension table" in {
    intercept[SparkException] {
      new DimensionTable(productDimData.toDF(), "product_key", Seq("product_id"), metaColumns = Seq("last_update_date"))
    }
  }

  "Dimension table SCD with Derived Column in source" should "generate the Change Set correctly" in {
    val productDim = new DimensionTable(productDimData.toDF(), "product_key", Seq("product_id"),
      derivedColumns = Seq("derived_attr"), metaColumns = Seq("last_updated_date"))
    val changeSet = productDim.getDimensionChangeSet(productData.toDF())
    val (insertRecords, updateRecords, ncdRecords, invalidRecords) = (changeSet.insertRecords, changeSet.updateRecords,
      changeSet.noChangeOrDeleteRecords, changeSet.invalidRecords)
    insertRecords.count shouldBe 1
    insertRecords.select("product_id").collect.head.getAs[Int](0) shouldBe 6
    insertRecords.select("derived_attr").collect.toSeq.map(_.getAs[String](0)).distinct.head should be("not-popular")
    insertRecords.show
    updateRecords.count shouldBe 2
    updateRecords.select("product_id").collect.toSeq.map(_.getAs[Int](0)) should contain theSameElementsAs Seq(3, 4)
    updateRecords.select("derived_attr").collect.toSeq.map(_.getAs[String](0)).distinct.head should be("popular")
    updateRecords.show
    ncdRecords.count shouldBe 5
    ncdRecords.select("product_id").collect.toSeq.map(_.getAs[Int](0)) should contain theSameElementsAs Seq(0, 1, -1, 5, 2)
    ncdRecords.show
    invalidRecords.count shouldBe 0
    invalidRecords.show
  }


  "Dimension table SCD without Derived Column in source" should "generate the Change Set correctly" in {
    val productDim = new DimensionTable(productDimData.toDF(), "product_key", Seq("product_id"),
      derivedColumns = Seq("derived_attr"), metaColumns = Seq("last_updated_date"))
    val changeSet = productDim.getDimensionChangeSet(productData.toDF().drop("derived_attr"))
    val (insertRecords, updateRecords, ncdRecords, invalidRecords) = (changeSet.insertRecords, changeSet.updateRecords,
      changeSet.noChangeOrDeleteRecords, changeSet.invalidRecords)
    insertRecords.count shouldBe 1
    insertRecords.select("product_id").collect.head.getAs[Int](0) shouldBe 6
    //insertRecords.select("derived_attr").collect.toSeq.map(_.getAs[String](0)).distinct.head should be(null)
    insertRecords.show
    updateRecords.count shouldBe 2
    updateRecords.select("product_id").collect.toSeq.map(_.getAs[Int](0)) should contain theSameElementsAs Seq(3, 4)
    updateRecords.select("derived_attr").collect.toSeq.map(_.getAs[String](0)).distinct.head should be("not-available")
    updateRecords.show
    ncdRecords.count shouldBe 5
    ncdRecords.select("product_id").collect.toSeq.map(_.getAs[Int](0)) should contain theSameElementsAs Seq(0, 1, -1, 5, 2)
    ncdRecords.show
    invalidRecords.count shouldBe 0
    invalidRecords.show
  }


  "Dimension table SCD with Derived Column in source and Full Load" should "generate the Change Set correctly" in {
    val productDim = new DimensionTable(productDimData.toDF(), "product_key", Seq("product_id"),
      derivedColumns = Seq("derived_attr"), metaColumns = Seq("last_updated_date"))
    val changeSet = productDim.getDimensionChangeSet(productData.toDF(), incrementalLoad = false)
    val (insertRecords, updateRecords, ncdRecords, invalidRecords) = (changeSet.insertRecords, changeSet.updateRecords,
      changeSet.noChangeOrDeleteRecords, changeSet.invalidRecords)
    insertRecords.count shouldBe 1
    insertRecords.select("product_id").collect.head.getAs[Int](0) shouldBe 6
    insertRecords.select("derived_attr").collect.toSeq.map(_.getAs[String](0)).distinct.head should be("not-popular")
    insertRecords.show
    updateRecords.count shouldBe 2
    updateRecords.select("product_id").collect.toSeq.map(_.getAs[Int](0)) should contain theSameElementsAs Seq(3, 4)
    updateRecords.select("derived_attr").collect.toSeq.map(_.getAs[String](0)).distinct.head should be("popular")
    updateRecords.show
    ncdRecords.count shouldBe 2
    ncdRecords.select("product_id").collect.toSeq.map(_.getAs[Int](0)) should contain theSameElementsAs Seq(1, 2)
    ncdRecords.show
    invalidRecords.count shouldBe 3
    invalidRecords.select("product_id").collect.toSeq.map(_.getAs[Int](0)) should contain theSameElementsAs Seq(0, -1, 5)
    invalidRecords.show
  }

}
