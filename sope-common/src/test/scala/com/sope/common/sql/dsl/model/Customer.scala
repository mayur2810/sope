package com.sope.common.sql.dsl.model

case class Customer(cust_id: Int, first_name: String, last_name: String, address: String, city: String) {
  def toSeq: Seq[Any] = Seq(cust_id, first_name, last_name, address, city)
}
