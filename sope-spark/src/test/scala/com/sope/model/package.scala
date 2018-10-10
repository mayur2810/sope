package com.sope

/**
  * Test model classes
  * @author mbadgujar
  */
package object model {

  case class Student(first_name: String, last_name: String, roll_no: Int, cls: Int)

  case class Class(key: Int, cls: Int, name: String)

  case class Person(first_name: String, last_name: String, address: String, email: String, phone: String)
}
