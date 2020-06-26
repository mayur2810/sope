package com.sope.common.sql.dsl

import java.sql.Date

import com.sope.common.sql.{SqlOps, TestInit}
import org.scalatest.{FlatSpec, Matchers}

/**
 * @author mbadgujar
 */
trait DSLResults[D] {
  this: FlatSpec with Matchers with DSL with TestInit[D] =>

  type DatasetColsFunc = D => Seq[String]
  type DatasetRecordCntFunc = D => Long
  type DatasetRecordsFunc = D => Seq[Seq[Any]]

  def getDatasetColsFunc: DatasetColsFunc

  def getDatasetRecordCntFunc: DatasetRecordCntFunc

  def getDatasetRecordsFunc: DatasetRecordsFunc

  implicit def sqlOps: SqlOps[D, String]

  def noOpResult(): Unit = {
    val resultDataset = NoOp() --> testDatasets(CustomerDataset)
    getDatasetColsFunc(resultDataset) should contain theSameElementsAs CustomerFields
    getDatasetRecordCntFunc(resultDataset) should be(customers.size)
  }

  def selectResult(): Unit = {
    val resultDataset = Select(FirstName, LastName) --> testDatasets(CustomerDataset)
    getDatasetColsFunc(resultDataset) should contain allOf(FirstName, LastName)
    getDatasetRecordCntFunc(resultDataset) should be(customers.size)
  }

  def filterResult(): Unit = {
    val resultDataset = Filter(s"$FirstName = 'Sherlock' || $FirstName = 'John'") --> testDatasets(CustomerDataset)
    getDatasetRecordCntFunc(resultDataset) should be(2)
    getDatasetColsFunc(resultDataset) should contain theSameElementsAs CustomerFields
    getDatasetRecordsFunc(resultDataset).head should contain theSameElementsAs
      customers.filter(rec => rec.first_name == "Sherlock" || rec.first_name == "John").head.toSeq
  }

  def renameResult(): Unit = {
    val resultDataset = Rename(FirstName -> "name", LastName -> "surname") --> testDatasets(CustomerDataset)
    getDatasetRecordCntFunc(resultDataset) should be(customers.size)
    getDatasetColsFunc(resultDataset) should contain theSameElementsAs CustomerFields.map {
      fieldName => if (fieldName == FirstName) "name" else if (fieldName == LastName) "surname" else fieldName
    }
  }

  def dropColumnResult(): Unit = {
    val resultDataset = Drop(Address, City) --> testDatasets(CustomerDataset)
    getDatasetRecordCntFunc(resultDataset) should be(customers.size)
    getDatasetColsFunc(resultDataset) should contain theSameElementsAs CustomerFields.filterNot {
      fieldName => fieldName == Address || fieldName == City
    }
  }

  def transformColumnResult(upperCaseFnName: String = "upper"): Unit = {
    val resultDataset =
      Transform(
        "first_name_upper" -> s"$upperCaseFnName($FirstName)",
        LastName -> s"$upperCaseFnName($LastName)"
      ) + Filter(s"$FirstName = 'Sherlock'") --> testDatasets(CustomerDataset)
    getDatasetRecordCntFunc(resultDataset) should be(1)
    getDatasetColsFunc(resultDataset) should
      contain theSameElementsAs CustomerFields :+ "first_name_upper"
    getDatasetRecordsFunc(resultDataset).head should
      contain theSameElementsAs Seq[Any](1, "Sherlock", "HOLMES", "Baker Street", "LD", "SHERLOCK")
  }

  def transformAllColumnResult(upperCaseFnName: String = "upper"): Unit = {
    val resultDataset =
      TransformAll(upperCaseFnName, "first_name_upper" -> FirstName, LastName -> LastName) +
        Filter(s"$FirstName = 'Sherlock'") --> testDatasets(CustomerDataset)
    getDatasetRecordCntFunc(resultDataset) should be(1)
    getDatasetColsFunc(resultDataset) should
      contain theSameElementsAs CustomerFields :+ "first_name_upper"
    getDatasetRecordsFunc(resultDataset).head should
      contain theSameElementsAs Seq[Any](1, "Sherlock", "HOLMES", "Baker Street", "LD", "SHERLOCK")
  }

  def transformAllColumnNoColumnsProvidedResult(upperCaseFnName: String = "upper"): Unit = {
    val resultDataset =
      TransformAll(upperCaseFnName) + Filter(s"$FirstName = 'Sherlock'") --> testDatasets(CustomerDataset)
    getDatasetRecordCntFunc(resultDataset) should be(1)
    getDatasetColsFunc(resultDataset) should
      contain theSameElementsAs CustomerFields
    getDatasetRecordsFunc(resultDataset).head should
      contain theSameElementsAs Seq[Any](1, "Sherlock", "Holmes", "Baker Street", "LD")
  }

  def transformAllColumnMultiArgFunctionResult(concatFnName: String = "concat",
                                               upperCaseFnName: String = "upper"): Unit = {
    val resultDataset =
      TransformAllMultiArg(concatFnName,
        "name" -> Seq(s"$upperCaseFnName($FirstName)", "' '", s"$upperCaseFnName($LastName)"),
        "full_address" -> Seq(Address, "' '", City)) +
        Filter(s"$FirstName = 'Sherlock'") --> testDatasets(CustomerDataset)
    getDatasetRecordCntFunc(resultDataset) should be(1)
    getDatasetColsFunc(resultDataset) should
      contain theSameElementsAs CustomerFields :+ "name" :+ "full_address"
    getDatasetRecordsFunc(resultDataset).head should
      contain theSameElementsAs
      Seq[Any](1, "Sherlock", "Holmes", "Baker Street", "LD", "SHERLOCK HOLMES", "Baker Street LD")
  }


  def joinOnColumnsResult(): Unit = {
    val joinFn = Join(CustomerId) inner testDatasets(TransactionsDataset)
    val resultDataset = joinFn + Filter(s"$FirstName = 'Tony'") --> testDatasets(CustomerDataset)
    getDatasetRecordCntFunc(resultDataset) should be(1)
    getDatasetColsFunc(resultDataset) should contain theSameElementsAs (CustomerFields ++ TransactionFields).toSet
    getDatasetRecordsFunc(resultDataset).head should contain theSameElementsAs
      Seq[Any](3, "Tony", "Stark", "Malibu Point", "ML", 3, Date.valueOf("2020-02-01"))
  }

  def joinOnExpressionResult(): Unit = {
    val customerDatasetWithCidRenamed = Rename(CustomerId -> "cid") --> testDatasets(CustomerDataset)
    val joinFn = JoinExpr(s"cid = $CustomerId") inner testDatasets(TransactionsDataset)
    val resultDataset = joinFn + Filter(s"$FirstName = 'Tony'") --> customerDatasetWithCidRenamed
    getDatasetRecordCntFunc(resultDataset) should be(1)
    getDatasetColsFunc(resultDataset) should contain theSameElementsAs (CustomerFields ++ TransactionFields :+ "cid")
      .toSet
    getDatasetRecordsFunc(resultDataset).head should contain theSameElementsAs
      Seq[Any](3, "Tony", "Stark", "Malibu Point", "ML", 3, 3, Date.valueOf("2020-02-01"))
  }

  def aggregateResult(): Unit = {
    val resultDataset = Aggregate("max_price" -> s"max($Price)",
      "sum_of_product_price" -> s"sum($Price)") --> testDatasets(ProductDataset)
    getDatasetRecordCntFunc(resultDataset) should be(1)
    getDatasetColsFunc(resultDataset) should contain theSameElementsAs Seq("max_price", "sum_of_product_price")
    getDatasetRecordsFunc(resultDataset).head should contain theSameElementsAs Seq(2000.0, 4121.0)
  }

  def groupResult(): Unit = {
    val resultDataset =
      GroupBy("trxn_id")
        .agg("total_quantity_purchased" -> s"count($Quantity)"
          , "max_quantity_item_purchased" -> s"max($Quantity)") +
        Filter(s"$TrxnId = 5") --> testDatasets(TransactionDetailsDataset)
    getDatasetRecordCntFunc(resultDataset) should be(1)
    getDatasetColsFunc(resultDataset) should contain theSameElementsAs Seq(TrxnId, "total_quantity_purchased",
      "max_quantity_item_purchased")
    getDatasetRecordsFunc(resultDataset).head should contain theSameElementsAs Seq(5, 3, 1000)
  }

  def unionResult(): Unit = {
    val customerDataset = testDatasets(CustomerDataset)
    val customer1 = Filter(s"$FirstName = 'Sherlock'") --> customerDataset
    val customer2 = Filter(s"$FirstName = 'John'") --> customerDataset
    val customer3 = Filter(s"$FirstName = 'Steve'") --> customerDataset
    val resultDataset = Union(customer1, customer2, customer2, customer3, customer3) --> customer1
    getDatasetRecordCntFunc(resultDataset) should be(3)
    getDatasetColsFunc(resultDataset) should contain theSameElementsAs CustomerFields
  }

  def intersectResult(likeFnTemplate: String = "%s like %s"): Unit = {
    val productDataset = testDatasets(ProductDataset)
    val startWithC = Filter(likeFnTemplate.format(ProductName, "'C%'")) --> productDataset
    val resultDataset = Intersect(startWithC) --> productDataset
    getDatasetRecordCntFunc(resultDataset) should be(5)
    getDatasetColsFunc(resultDataset) should contain theSameElementsAs ProductFields
  }

  def exceptResult(likeFnTemplate: String = "%s like %s"): Unit = {
    val productDataset = testDatasets(ProductDataset)
    val startWithC = Filter(likeFnTemplate.format(ProductName, "'C%'")) --> productDataset
    val resultDataset = Except(startWithC) --> productDataset
    getDatasetRecordCntFunc(resultDataset) should be(3)
    getDatasetColsFunc(resultDataset) should contain theSameElementsAs ProductFields
  }

  def orderByResult(descMarkerTemplate: String = "%s.desc"): Unit = {
    val resultDataset = OrderBy(descMarkerTemplate.format(Price),
      descMarkerTemplate.format(ProductName)) + Select(ProductId) --> testDatasets(ProductDataset)
    getDatasetRecordsFunc(resultDataset).map(_.head) should contain inOrder(3, 1, 2, 4, 5, 8, 7, 6)
    getDatasetRecordCntFunc(resultDataset) should be(8)
    getDatasetColsFunc(resultDataset).head should be theSameInstanceAs ProductId
  }

  def distinctResult(): Unit = {
    val resultDataset = Select(Price) + Distinct() --> testDatasets(ProductDataset)
    getDatasetRecordCntFunc(resultDataset) should be(6)
    getDatasetColsFunc(resultDataset).head should be theSameInstanceAs Price
  }
}
