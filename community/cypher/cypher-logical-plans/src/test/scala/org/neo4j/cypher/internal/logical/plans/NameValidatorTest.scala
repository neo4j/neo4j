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
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.gqlstatus.GqlStatusInfoCodes

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
      case e: InvalidArgumentException => e should (
          have message "The provided username is empty."
            and be(gqlStatus(
              GqlStatusInfoCodes.STATUS_22NB6,
              "error: data exception - input empty. Invalid input. Username is not allowed to be an empty string."
            ))
        )
    }

    try {
      NameValidator.assertValidUsername(null)

      fail("Expected exception \"The provided username is empty.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException => e should (
          have message "The provided username is empty."
            and be(gqlStatus(
              GqlStatusInfoCodes.STATUS_22NB6,
              "error: data exception - input empty. Invalid input. Username is not allowed to be an empty string."
            ))
        )
    }
  }

  test("Should get an error for a username with invalid characters") {
    try {
      NameValidator.assertValidUsername("user:")

      fail("Expected exception \"Username 'user:' contains illegal characters.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException =>
        e should have message (
          """Username 'user:' contains illegal characters.
            |Use ascii characters that are not ',', ':' or whitespaces.""".stripMargin
        )
        e.gqlStatus() should be("22N05")
        e.statusDescription() should include("Invalid input 'user:' for username.")
        e.cause() should not be empty
        e.cause().get().gqlStatus() should be("22N82")
        e.cause().get().statusDescription() should include(
          "Input 'user:' contains invalid characters for username. Special characters may require that the input is quoted using backticks."
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
      case e: InvalidArgumentException => e should (
          have message "The provided role name is empty."
            and be(gqlStatus(
              GqlStatusInfoCodes.STATUS_22NB6,
              "error: data exception - input empty. Invalid input. Role name is not allowed to be an empty string."
            ))
        )
    }

    try {
      NameValidator.assertValidRoleName(null)

      fail("Expected exception \"The provided role name is empty.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException => e should (
          have message "The provided role name is empty."
            and be(gqlStatus(
              GqlStatusInfoCodes.STATUS_22NB6,
              "error: data exception - input empty. Invalid input. Role name is not allowed to be an empty string."
            ))
        )
    }
  }

  test("Should get an error for a role name with invalid characters") {
    try {
      NameValidator.assertValidRoleName("role%")

      fail("Expected exception \"Role name 'role%' contains illegal characters.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException =>
        e should have message (
          """Role name 'role%' contains illegal characters.
            |Use simple ascii characters, numbers and underscores.""".stripMargin
        )
        e.gqlStatus() should be("22N05")
        e.statusDescription() should include("Invalid input 'role%' for role name.")
        e.cause() should not be empty
        e.cause().get().gqlStatus() should be("22N82")
        e.cause().get().statusDescription() should include(
          "Input 'role%' contains invalid characters for role name. Special characters may require that the input is quoted using backticks."
        )
    }
  }
}
