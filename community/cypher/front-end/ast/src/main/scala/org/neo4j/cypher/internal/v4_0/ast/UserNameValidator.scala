package org.neo4j.cypher.internal.v4_0.ast

import java.util.regex.Pattern

import org.neo4j.cypher.internal.v4_0.util.InvalidArgumentException

object UserNameValidator {
  // Allow all ascii from '!' to '~', apart from ',' and ':' which are used as separators in flat file
  private val usernamePattern = Pattern.compile("^[\\x21-\\x2B\\x2D-\\x39\\x3B-\\x7E]+$")

  def assertValidUsername(username: String): Unit = {
    if (username == null || username.isEmpty)
      throw new InvalidArgumentException("The provided username is empty.")
    if (!usernamePattern.matcher(username).matches)
      throw new InvalidArgumentException(
        s"""Username '$username' contains illegal characters.
           |Use ascii characters that are not ',', ':' or whitespaces.""".stripMargin)
  }

}
