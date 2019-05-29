/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v4_0.ast

import java.util.regex.Pattern

import org.neo4j.cypher.internal.v4_0.util.InvalidArgumentException

object NameValidator {
  // Allow all ascii from '!' to '~', apart from ',' and ':' which are used as separators in flat file
  private val usernamePattern = Pattern.compile("^[\\x21-\\x2B\\x2D-\\x39\\x3B-\\x7E]+$")

  // Allow only letters, numbers and underscore
  private val roleNamePattern = Pattern.compile("^[a-zA-Z0-9_]+$")

  private def isLowerCaseLetter(asciiCode: Int) = {
    asciiCode >= 97 && asciiCode <= 122
  }

  private def isDigitDotOrUnderscore(asciiCode: Int) = {
    (asciiCode >= 48 && asciiCode <= 57) || asciiCode == 46 || asciiCode == 95
  }

  def assertValidUsername(name: String): Unit = {
    if (name == null || name.isEmpty)
      throw new InvalidArgumentException("The provided username is empty.")
    if (!usernamePattern.matcher(name).matches)
      throw new InvalidArgumentException(
        s"""Username '$name' contains illegal characters.
           |Use ascii characters that are not ',', ':' or whitespaces.""".stripMargin)
  }

  def assertValidRoleName(name: String): Unit = {
    if (name == null || name.isEmpty)
      throw new InvalidArgumentException("The provided role name is empty.")
    if (!roleNamePattern.matcher(name).matches)
      throw new InvalidArgumentException(
        s"""Role name '$name' contains illegal characters.
           |Use simple ascii characters and numbers.""".stripMargin)
  }

  def assertValidDatabaseName(name: String): Unit = {
    // Assumes that `name` is in lowercase
    if (name == null || name.isEmpty)
      throw new InvalidArgumentException("The provided database name is empty.")
    if (name.length < 3 || name.length > 63)
      throw new InvalidArgumentException("The provided database name must have a length between 3 and 63 characters.")
    if (!isLowerCaseLetter(name(0).toInt))
      throw new InvalidArgumentException(s"Database name '$name' is not starting with an ASCII alphabetic character.")
    name.foreach(c => if (!(isLowerCaseLetter(c.toInt) || isDigitDotOrUnderscore(c.toInt)))
      throw new InvalidArgumentException(
        s"""Database name '$name' contains illegal characters.
           |Use simple ascii characters and numbers.""".stripMargin))
    if (name.startsWith("system"))
      throw new InvalidArgumentException(s"Database name '$name' is invalid, due to the prefix 'system'.")
  }
}
