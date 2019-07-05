package com.sope.etl.utils

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
        // find all matches for the key
        val matches = regex.findAllMatchIn(redacted)
        // Redact all matches for the key
        matches
          .map(_.group(1))
          .foldLeft(redacted) {
            (str, replacement) => str.replaceAll(replacement, replacement.map(_ => '*'))
          }
    }
  }

}
