package com.sope

import com.sope.TestContext.getSQlContext
import com.sope.model.{Class, Student}
import com.sope.spark.sql.dsl._
import com.sope.spark.sql._
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}

/**
  *
  * @author mbadgujar
  */
class DSLTest extends FlatSpec with Matchers {

  private val sqlContext = getSQlContext

  import sqlContext.implicits._

  private val studentDF = Seq(
    Student("Sherlock", "Holmes", 1, 10),
    Student("John", "Watson", 2, 10),
    Student("N.A.", "N.A.", -1, -1)
  ).toDF

  private val classDF = Seq(
    Class(1, 10, "Tenth"),
    Class(2, 9, "Ninth"),
    Class(3, 8, "Ninth")
  ).toDF

  // Fields
  val FirstName = "first_name"
  val LastName = "last_name"
  val RollNo = "roll_no"
  val Cls = "cls"

  "Select DSL" should "generate the transformations correctly" in {
    val selectStr = Select(FirstName, LastName)
    val selectCol = Select($"$LastName", $"$FirstName")
    val transformed = selectStr + selectCol --> studentDF
    println("Select Test DF output =>")
    transformed.show(false)
    transformed.schema.fields.map(_.name) should contain allOf(FirstName, LastName)
    val transformed1 = SelectNot(Cls) --> studentDF
    transformed1.show(false)
    transformed1.schema.fields.map(_.name) should contain allOf(FirstName, LastName, RollNo)
  }

  "Filter and Rename DSL" should "generate the transformations correctly" in {
    val filterCol = Filter(col(FirstName) =!= "N.A.")
    val filterStr = Filter(s"$LastName != 'Watson'")
    val rename = Rename((FirstName, "name"))
    val transformed = filterCol + filterStr + rename --> studentDF
    println("Filter, Rename Test DF output =>")
    transformed.show(false)
    transformed.count should be(1)
    transformed.schema.fields.map(_.name) should contain("name")
    // Rename All test
    val transformed1 = Rename("tmp_") + Rename("_column", prefix = false) --> studentDF
    transformed1.show(false)
    assert(transformed1.schema.fields.map(_.name).forall(_.startsWith("tmp_")))
  }


  "Transform & Sequence DSL" should "generate the transformations correctly" in {
    val transformStr = Transform(FirstName -> upper($"$FirstName"))
    val transformCol = Transform(LastName -> s"upper($LastName)")
    val transformed = transformStr + transformCol + Distinct() + Sequence(0l, "id") --> studentDF
    println("Transform & Sequence DF output =>")
    transformed.show(false)
    transformed.filter("roll_no = 1").select(FirstName).head.getString(0) should be("SHERLOCK")
    transformed.maxKeyValue("id") should be(3)
  }

  "Join DSL" should "generate the transformations correctly" in {
    val innerJoin =  Transform(upper _, FirstName, LastName) + (Join(Some("left") , Cls) inner classDF)
    (innerJoin --> studentDF).count should be(2)

    val leftJoin = Transform(upper _) + (Join(None, Cls) left classDF)
    (leftJoin --> studentDF).count should be(3)

    val rightJoin = Join(Some("left"), $"$Cls" === $"class") right (Rename((Cls, "class")) --> classDF)
    (rightJoin --> studentDF).count should be(4)

    val fullJoin = Join(None, $"$Cls" === $"class") full (Rename((Cls, "class")) --> classDF)
    (fullJoin --> studentDF).count should be(5)
  }

  "Aggregate DSL" should "generate the transformations correctly" in {
    (Aggregate("max(cls)") --> classDF).collect().head.getInt(0) should be(10)
  }

}
