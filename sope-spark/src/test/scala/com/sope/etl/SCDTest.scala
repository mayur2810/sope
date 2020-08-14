package com.sope.etl

import com.sope.TestContext.getSQlContext
import com.sope.spark.utils.etl.DimensionTable
import com.sope.spark.yaml.{ParallelizeYaml, SchemaYaml}
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.scalatest.{FlatSpec, Matchers}

/**
  * Unit Tests for Dimension SCD
  *
  * @author mbadgujar
  */
class SCDTest extends FlatSpec with Matchers {

  private val sqlContext = getSQlContext

  val productData: DataFrame = ParallelizeYaml("data/product_input.yaml").parallelize(sqlContext)
  val productDimSchema: StructType = SchemaYaml("data/product_dim_schema.yaml").getSparkSchema
  val productDimData: DataFrame = ParallelizeYaml("data/product_dim.yaml").parallelize(sqlContext, Option(productDimSchema))

  "Dimension table SCD " should "throw exception for incorrect surrogate Key column in dimension table" in {
    intercept[SparkException] {
      new DimensionTable(productDimData, "product_ky", Seq("product_id"), metaColumns = Seq("last_updated_date"))
    }
  }

  "Dimension table SCD " should "throw exception for incorrect Natural Keys column in dimension table" in {
    intercept[SparkException] {
      new DimensionTable(productDimData, "product_key", Seq("product_d"), metaColumns = Seq("last_updated_date"))
    }
  }

  "Dimension table SCD " should "throw exception for incorrect Meta Keys column in dimension table" in {
    intercept[SparkException] {
      new DimensionTable(productDimData, "product_key", Seq("product_id"), metaColumns = Seq("last_update_date"))
    }
  }

  "Dimension table SCD with Derived Column in source" should "generate the Change Set correctly" in {
    val productDim = new DimensionTable(productDimData, "product_key", Seq("product_id"),
      derivedColumns = Seq("derived_attr"), metaColumns = Seq("last_updated_date"))
    val changeSet = productDim.getDimensionChangeSet(productData)
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
    val productDim = new DimensionTable(productDimData, "product_key", Seq("product_id"),
      derivedColumns = Seq("derived_attr"), metaColumns = Seq("last_updated_date"))
    val changeSet = productDim.getDimensionChangeSet(productData.drop("derived_attr"))
    val (insertRecords, updateRecords, ncdRecords, invalidRecords) = (changeSet.insertRecords, changeSet.updateRecords,
      changeSet.noChangeOrDeleteRecords, changeSet.invalidRecords)
    insertRecords.count shouldBe 1
    insertRecords.select("product_id").collect.head.getAs[Int](0) shouldBe 6
    insertRecords.select("derived_attr").collect.toSeq.map(_.getAs[String](0)).distinct.head should be(null)
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
    val productDim = new DimensionTable(productDimData, "product_key", Seq("product_id"),
      derivedColumns = Seq("derived_attr"), metaColumns = Seq("last_updated_date"))
    val changeSet = productDim.getDimensionChangeSet(productData, incrementalLoad = false)
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
