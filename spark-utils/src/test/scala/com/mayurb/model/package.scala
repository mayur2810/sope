package com.mayurb

/**
  * Test model classes
  * @author mbadgujar
  */
package object model {

  case class Student(first_name: String, last_name: String, roll_no: Int, cls: Int)

  case class Class(cls: Int, name: String)
}
