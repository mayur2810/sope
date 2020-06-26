package com.sope.common.sql

import java.sql.Date

import com.sope.common.sql.dsl.model.{Customer, Product, TransactionDetails, Transactions}
import com.sope.common.sql.dsl.model.Customer

/**
 * @author mbadgujar
 */
trait TestInit[D] {

  val CustomerDataset = "CUSTOMERS"
  val ProductDataset = "PRODUCTS"
  val TransactionsDataset = "TRANSACTIONS"
  val TransactionDetailsDataset = "TRANSACTION_DETAILS"

  // Customer Fields
  val CustomerId = "cust_id"
  val FirstName = "first_name"
  val LastName = "last_name"
  val Address = "address"
  val City = "city"
  val CustomerFields = Seq(CustomerId, FirstName, LastName, Address, City)


  // Transactions Fields
  val TrxnId = "trxn_id"
  val TrxnDate = "trnx_date"
  val TransactionFields = Seq(TrxnId, CustomerId, TrxnDate)

  // Product Fields
  val ProductId = "product_id"
  val ProductName = "name"
  val Price = "price"
  val ProductFields = Seq(ProductId, ProductName, Price)

  // Transaction Details Fields
  val Quantity = "quantity"
  val TransactionDetailsFields = Seq(TrxnId, ProductId, Quantity)

  val customers = Seq(
    Customer(1, "Sherlock", "Holmes", "Baker Street", "LD"),
    Customer(2, "John", "Watson", "Opp. Baker Street", "LD"),
    Customer(3, "Tony", "Stark", "Malibu Point", "ML"),
    Customer(4, "Steve", "Rogers", "Brooklyn Heights", "NY"),
    Customer(5, "Chacha", "Chaudhary", "MG Street", "PN"),
    Customer(-1, "N.A.", "N.A.", "", "")
  )
  val products = Seq(
    Product(1, "Shield", 1000.00),
    Product(2, "PowerCells", 1000.00),
    Product(3, "Computer", 2000.00),
    Product(4, "Pipe", 100.00),
    Product(5, "Chips", 10.00),
    Product(6, "Candy", 1.00),
    Product(7, "Cola", 5.00),
    Product(8, "Cookies", 5.00)
  )

  val transactions = Seq(
    Transactions(1, 1, Date.valueOf("2020-01-01")),
    Transactions(2, 2, Date.valueOf("2020-01-01")),
    Transactions(3, 3, Date.valueOf("2020-02-01")),
    Transactions(4, 4, Date.valueOf("2020-02-01")),
    Transactions(5, 5, Date.valueOf("2020-03-01"))
  )

  val transactionDetails = Seq(
    TransactionDetails(1, 4, 2),
    TransactionDetails(1, 7, 4),
    TransactionDetails(1, 3, 1),

    TransactionDetails(2, 8, 5),
    TransactionDetails(2, 7, 3),

    TransactionDetails(3, 2, 500),
    TransactionDetails(3, 3, 30),

    TransactionDetails(4, 1, 1),
    TransactionDetails(4, 5, 1),

    TransactionDetails(5, 3, 4),
    TransactionDetails(5, 6, 1000),
    TransactionDetails(5, 8, 10)
  )

  def testDatasets: Map[String, D]
}
