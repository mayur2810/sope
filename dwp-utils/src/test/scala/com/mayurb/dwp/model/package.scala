package com.mayurb.dwp

/**
  * Test Model Classes
  *
  * @author mbadgujar
  */
package object model {

  case class Transactions(id: Int, loc: String, product: String, date: String)

  case class Product(product_id: Int, product_desc: String, size: Int, color: Int)

  case class Date(date_id: Int, date: String)

}
