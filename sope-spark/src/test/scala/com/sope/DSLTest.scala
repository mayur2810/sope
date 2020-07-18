package com.sope

import com.sope.TestContext.getSQlContext
import com.sope.model.{Class, Student}
import com.sope.spark.sql._
import com.sope.spark.sql.dslo._
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
    Student("Tony", "Stark", 3, 9),
    Student("Steve", "Rogers", 4, 9),
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
    val selectPattern = Select(".*name")
    val transformed = selectStr + selectCol + selectPattern --> studentDF
    println("Select Tests DF output =>")
    transformed.show(false)
    transformed.columns should contain allOf(FirstName, LastName)

    // Select Not
    val transformed1 = SelectNot(Cls) --> studentDF
    transformed1.show(false)
    transformed1.columns should contain allOf(FirstName, LastName, RollNo)
  }

  "Filter and Rename DSL" should "generate the transformations correctly" in {
    val filterCol = Filter(col(FirstName) =!= "N.A.")
    val filterStr = Filter(s"$LastName != 'Watson'")
    val rename = Rename((FirstName, "name"))
    val transformed = filterCol + filterStr + rename --> studentDF
    println("Filter, Rename Test DF output =>")
    transformed.show(false)
    transformed.count should be(3)
    transformed.columns should contain("name")
    // Rename All test
    val transformed1 = Rename("tmp_", prefix = true) + Rename("_column", prefix = false) --> studentDF
    transformed1.show(false)
    assert(transformed1.columns.forall(_.startsWith("tmp_")))

    // Rename Selected test
    val transformed2 = Rename("_tmp", prefix = false, FirstName, LastName) --> studentDF
    transformed2.show(false)
    transformed2.columns should contain allOf("first_name_tmp", "last_name_tmp")

    // Rename pattern test
    val transformed3 = Rename("tmp_", prefix = true, ".*name") --> studentDF
    transformed3.show(false)
    transformed3.columns should contain allOf("tmp_first_name", "tmp_last_name")

    // Rename pattern test
    val transformed4 = Rename(".*name", "tmp_", "") --> transformed3
    transformed4.show(false)
    transformed4.columns should contain allOf(FirstName, LastName)
  }


  "Transform & Sequence DSL" should "generate the transformations correctly" in {
    val transformStr = Transform(FirstName -> upper($"$FirstName"))
    val transformCol = Transform(LastName -> s"upper($LastName)")
    val transformed = transformStr + transformCol + Distinct() + Sequence(0l, "id") --> studentDF
    println("Transform & Sequence DF output =>")
    transformed.show(false)
    transformed.filter("roll_no = 1").select(FirstName).head.getString(0) should be("SHERLOCK")
    transformed.maxKeyValue("id") should be(5)

    // Transform select
    val transformed1 = Transform(upper _, None, FirstName, LastName) --> studentDF
    transformed1.show()
    transformed1.filter("roll_no = 1")
      .select(FirstName, LastName).head
      .toSeq should contain inOrder("SHERLOCK", "HOLMES")

    // Transform select
    val transformed2 = Transform(upper _, None, ".*name") --> studentDF
    transformed2.show()
    transformed2.filter("roll_no = 1")
      .select(FirstName, LastName).head
      .toSeq should contain inOrder("SHERLOCK", "HOLMES")


  }

  "Join DSL" should "generate the transformations correctly" in {
    val innerJoin = Transform(upper _, None, FirstName, LastName) + (Join(Some("left"), Cls) inner classDF)
    (innerJoin --> studentDF).count should be(4)

    val leftJoin = Transform(upper _, None) + (Join(None, Cls) left classDF)
    (leftJoin --> studentDF).count should be(5)

    val rightJoin = Join(Some("left"), $"$Cls" === $"class") right (Rename((Cls, "class")) --> classDF)
    (rightJoin --> studentDF).count should be(5)

    val fullJoin = Join(None, $"$Cls" === $"class") full (Rename((Cls, "class")) --> classDF)
    (fullJoin --> studentDF).count should be(6)
  }

  "Aggregate DSL" should "generate the transformations correctly" in {
    (Aggregate("max(cls)") --> classDF).collect().head.getInt(0) should be(10)
  }

  "MultiOutTest" should "generate the transformations correctly" in {
    val routes = Routes("suspense" -> "cls = 10", "sci-fi" -> "cls = 9")
    val lower = Transform("first_name" -> "lower(first_name)")
    val upper = Transform("last_name" -> "upper(last_name)")
    val trans = lower + routes + ("suspense", upper) --> studentDF
    trans.foreach {
      case ("suspense", df) =>
        df.show
        df.filter("roll_no = 1")
          .select(FirstName, LastName).head
          .toSeq should contain inOrder("sherlock", "HOLMES")
      case ("sci-fi", df) =>
        df.show
        df.filter("roll_no = 3")
          .select(FirstName, LastName).head
          .toSeq should contain inOrder("tony", "Stark")
      case ("default", df) =>
        df.show
        df.count should be(1)
    }
  }

}
