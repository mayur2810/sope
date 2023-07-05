package com.sope.etl

import com.sope.etl.utils.RedactUtil
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * @author mbadgujar
  */
class RedactTest extends AnyFlatSpec with Matchers {

  "Redact Utility" should "generate the redaction correctly" in {
    val data = Map("user" -> "user123", "url" -> "https://resrd", "username" -> "user123", "USERNAME" -> "user134",
    "password" -> "'sdds,dsd}sdsd'")
    val redacted = data.values.map(_.map(_ => '*'))
    data
      .map{case (k,v) => RedactUtil.redact(s"$k: $v")}
      .zip(redacted)
      .forall{
        case (result, masked) =>  result.contains(masked)
      } should be(true)
  }
}
