/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.exceptions.InvalidArgumentException

object NameValidator {
  // Allow all ascii from '!' to '~', apart from ',' and ':' which are used as separators in flat file
  private val usernamePattern = Pattern.compile("^[\\x21-\\x2B\\x2D-\\x39\\x3B-\\x7E]+$")

  // Allow only letters, numbers and underscore
  private val roleNamePattern = Pattern.compile("^[a-zA-Z0-9_]+$")

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
}
