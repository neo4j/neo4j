/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.exceptions.InvalidArgumentException

import java.util.regex.Pattern

object NameValidator {
  // Allow all ascii from '!' to '~', apart from ',' and ':' which are used as separators in flat file
  private val usernamePattern = Pattern.compile("^[\\x21-\\x2B\\x2D-\\x39\\x3B-\\x7E]+$")

  // Allow only letters, numbers and underscore
  private val roleNamePattern = Pattern.compile("^[a-zA-Z0-9_]+$")

  // Do not allow create/drop/revoke on PUBLIC role
  private val reservedRoleName = "PUBLIC"

  def assertValidUsername(name: String): Boolean = {
    if (name == null || name.isEmpty)
      throw new InvalidArgumentException("The provided username is empty.")
    if (!usernamePattern.matcher(name).matches)
      throw new InvalidArgumentException(
        s"""Username '$name' contains illegal characters.
           |Use ascii characters that are not ',', ':' or whitespaces.""".stripMargin
      )
    true
  }

  def assertValidRoleName(name: String): Boolean = {
    if (name == null || name.isEmpty)
      throw new InvalidArgumentException("The provided role name is empty.")
    if (!roleNamePattern.matcher(name).matches)
      throw new InvalidArgumentException(
        s"""Role name '$name' contains illegal characters.
           |Use simple ascii characters, numbers and underscores.""".stripMargin
      )
    true
  }

  def assertValidAliasName(name: String): Boolean = {
    if (name == null || name.isEmpty)
      throw new InvalidArgumentException("The provided alias is empty.")
    if (name.length > 65534)
      throw new InvalidArgumentException("The provided alias is to long, maximum characters are 65534.")
    if (name.startsWith("system")) {
      throw new InvalidArgumentException("Alias name '" + name + "' is invalid, due to the prefix 'system'.")
    }
    true
  }

  def assertValidTargetName(name: String): Boolean = {
    if (name == null || name.isEmpty)
      throw new InvalidArgumentException("The provided target database name is empty.")
    if (name.length > 65534)
      throw new InvalidArgumentException("The provided target database name is to long, maximum characters are 65534.")
    if (name.startsWith("system")) {
      throw new InvalidArgumentException("Target database name '" + name + "' is invalid, due to the prefix 'system'.")
    }
    true
  }

  def assertUnreservedRoleName(verb: String, name: String, newName: Option[String] = None): Boolean =
    if (reservedRoleName.equals(name)) {
      throw new InvalidArgumentException(s"Failed to $verb the specified role '$name': '$name' is a reserved role.")
    } else if (newName.isDefined && reservedRoleName.equals(newName.get)) {
      throw new InvalidArgumentException(
        s"Failed to $verb the specified role '$name' to '${newName.get}': '${newName.get}' is a reserved role."
      )
    } else {
      true
    }
}
