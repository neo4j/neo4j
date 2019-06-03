/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.plans

import java.util.regex.Pattern

import org.neo4j.cypher.internal.v4_0.util.InvalidArgumentException
import org.neo4j.kernel.database.DatabaseId

object NameValidator {
  // Allow all ascii from '!' to '~', apart from ',' and ':' which are used as separators in flat file
  private val usernamePattern = Pattern.compile("^[\\x21-\\x2B\\x2D-\\x39\\x3B-\\x7E]+$")

  // Allow only letters, numbers and underscore
  private val roleNamePattern = Pattern.compile("^[a-zA-Z0-9_]+$")

  private def isLowerCaseLetter(asciiCode: Int) = {
    asciiCode >= 97 && asciiCode <= 122
  }

  private def isDigitDotOrDash(asciiCode: Int) = {
    (asciiCode >= 48 && asciiCode <= 57) || asciiCode == 46 || asciiCode == 45
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
           |Use simple ascii characters, numbers and underscores.""".stripMargin)
  }

  def assertValidDatabaseName(normalizedName: NormalizedDatabaseName): Unit = {
    if (normalizedName == null) throw new InvalidArgumentException("The provided database name is empty.")

    val name = normalizedName.name
    if (name.isEmpty)
      throw new InvalidArgumentException("The provided database name is empty.")
    if (name.length < 3 || name.length > 63)
      throw new InvalidArgumentException("The provided database name must have a length between 3 and 63 characters.")
    if (!isLowerCaseLetter(name(0).toInt))
      throw new InvalidArgumentException(s"Database name '$name' is not starting with an ASCII alphabetic character.")
    name.foreach(c => if (!(isLowerCaseLetter(c.toInt) || isDigitDotOrDash(c.toInt)))
      throw new InvalidArgumentException(
        s"""Database name '$name' contains illegal characters.
           |Use simple ascii characters, numbers, dots and dashes.""".stripMargin))
    if (name.startsWith("system"))
      throw new InvalidArgumentException(s"Database name '$name' is invalid, due to the prefix 'system'.")
  }
}
