package com.mayurb

import com.mayurb.model.{Class, Person, Student}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.mayurb.spark.sql._
import com.mayurb.TestContext.getSQlContext
import org.apache.spark.sql.types.{StringType, IntegerType}
import org.scalatest.{FlatSpec, Matchers}

/**
  *
  * @author mbadgujar
  */
class FunctionTest extends FlatSpec with Matchers {

  private val sqlContext = getSQlContext

  import sqlContext.implicits._

  private val testSData = Seq(
    Person("Sherlock", "Holmes", "baker street", "sholmes@gmail.com", "999999"),
    Person("John", "Watson", "east street", "jwatson@gmail.com", "55555")
  ).toDF

  private val studentDF = Seq(
    Student("A", "B", 1, 10),
    Student("B", "C", 2, 10),
    Student("C", "E", 4, 9),
    Student("E", "F", 5, 9),
    Student("F", "G", 6, 10),
    Student("G", "H", 7, 10),
    Student("H", "I", 9, 8),
    Student("H", "I", 9, 7)
  ).toDF

  private val classDF = Seq(
    Class(1, 10, "Tenth"),
    Class(2, 9, "Ninth"),
    Class(3, 8, "Eighth")
  ).toDF

  "Dataframe Function transformations" should "generate the transformations correctly" in {
    val nameUpperFunc = (df: DataFrame) => df.withColumn("first_name", upper(col("first_name")))
    val nameConcatFunc = (df: DataFrame) => df.withColumn("name", concat(col("first_name"), col("last_name")))
    val addressUpperFunc = (df: DataFrame) => df.withColumn("address", upper(col("address")))
    val transformed = testSData.applyDFTransformations(Seq(nameUpperFunc, nameConcatFunc, addressUpperFunc))
    transformed.show(false)
    transformed.schema.fields.map(_.name) should contain("name")
  }

  "Group by as list Function Transformation" should  "generate the transformations correctly" in {
    val grouped = studentDF.groupByAsList(Seq("cls"))
        .withColumn("grouped_data", explode($"grouped_data"))
        .unstruct("grouped_data", keepStructColumn = false)
    grouped.show(false)
    grouped.filter("cls = 10").head.getAs[Long]("grouped_count") should be(4)
  }

  "Cast Transformation" should  "generate the transformations correctly" in {
    val casted = studentDF.castColumns(IntegerType, StringType)
    casted.dtypes.count(_._2 == "StringType") should be(4)
  }


  "Update Keys Transformation" should  "generate the transformations correctly" in {
    val updatedWithKey = studentDF
      .updateKeys(Seq("cls"), classDF.renameColumns(Map("cls" -> "class")), "class", "key")
      .dropColumns(Seq("last_name", "roll_no"))
    updatedWithKey.show(false)
    updatedWithKey.filter("first_name = 'A'").head.getAs[Long]("cls_key") should be(1)
  }

}
