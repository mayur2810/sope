package com.sope.common.utils

/**
  * Redaction Utility
  *
  * @author mbadgujar
  */
object RedactUtil {

  // TODO: Move to config
  private val keys = Seq("username", "user", "password", "url")

  def redact(input: String): String = {
    keys.foldLeft(input) {
      case (redacted, key) =>
        val regex = s"(?i)$key:\\s+(.*?)(,\\B|\\}\\B|\\n|\\r|\\z)".r
        regex.replaceAllIn(redacted, m => s"$key: ${m.group(1).map(_ => '*')}")
    }
  }

}
