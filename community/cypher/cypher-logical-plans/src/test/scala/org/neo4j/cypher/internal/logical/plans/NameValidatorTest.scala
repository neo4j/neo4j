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

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InvalidArgumentException

class NameValidatorTest extends CypherFunSuite {

  // username tests

  test("Should not get an error for a valid username") {
    NameValidator.assertValidUsername("myValidUser")
  }

  test("Should get an error for an empty username") {
    try {
      NameValidator.assertValidUsername("")

      fail("Expected exception \"The provided username is empty.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException => e.getMessage should be("The provided username is empty.")
    }

    try {
      NameValidator.assertValidUsername(null)

      fail("Expected exception \"The provided username is empty.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException => e.getMessage should be("The provided username is empty.")
    }
  }

  test("Should get an error for a username with invalid characters") {
    try {
      NameValidator.assertValidUsername("user:")

      fail("Expected exception \"Username 'user:' contains illegal characters.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException =>
        e.getMessage should be(
          """Username 'user:' contains illegal characters.
            |Use ascii characters that are not ',', ':' or whitespaces.""".stripMargin
        )
    }
  }

  // role name tests

  test("Should not get an error for a valid role name") {
    NameValidator.assertValidRoleName("my_ValidRole")
  }

  test("Should get an error for an empty role name") {
    try {
      NameValidator.assertValidRoleName("")

      fail("Expected exception \"The provided role name is empty.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException => e.getMessage should be("The provided role name is empty.")
    }

    try {
      NameValidator.assertValidRoleName(null)

      fail("Expected exception \"The provided role name is empty.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException => e.getMessage should be("The provided role name is empty.")
    }
  }

  test("Should get an error for a role name with invalid characters") {
    try {
      NameValidator.assertValidRoleName("role%")

      fail("Expected exception \"Role name 'role%' contains illegal characters.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException =>
        e.getMessage should be(
          """Role name 'role%' contains illegal characters.
            |Use simple ascii characters, numbers and underscores.""".stripMargin
        )
    }
  }
}
