package com.mayurb.dwp

/**
  * Test Model Classes
  *
  * @author mbadgujar
  */
package object model {

  case class Transactions(id: Int, loc: String, product: String, date: String)

  case class Product(product_id: Int, product_desc: String, size: Int, color: Int, derived_attr: String = "")

  case class ProductDim(product_key: Long, product_id: Int, product_desc: String, size: Int, color: Int, derived_attr: String,
                        last_updated_date: java.sql.Date)

  case class Date(date_id: Int, date: String)

}
