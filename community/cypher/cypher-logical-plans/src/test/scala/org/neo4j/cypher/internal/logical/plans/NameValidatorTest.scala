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

import org.neo4j.cypher.internal.v4_0.util.InvalidArgumentException
import org.neo4j.kernel.database.DatabaseId
import org.scalatest.{FunSuite, Matchers}

class NameValidatorTest extends FunSuite with Matchers {
  implicit private def stringToId(name: String): DatabaseId = new DatabaseId(name)

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
            |Use ascii characters that are not ',', ':' or whitespaces.""".stripMargin)
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
            |Use simple ascii characters, numbers and underscores.""".stripMargin)
    }
  }

  // database name tests

  test("Should not get an error for a valid database name") {
    NameValidator.assertValidDatabaseName("my.Vaild-Db")
  }

  test("Should get an error for an empty database name") {
    try {
      NameValidator.assertValidDatabaseName("")

      fail("Expected exception \"The provided database name is empty.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException => e.getMessage should be("The provided database name is empty.")
    }

    try {
      NameValidator.assertValidDatabaseName(null)

      fail("Expected exception \"The provided database name is empty.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException => e.getMessage should be("The provided database name is empty.")
    }
  }

  test("Should get an error for a database name with invalid characters") {
    try {
      NameValidator.assertValidDatabaseName("database%")

      fail("Expected exception \"Database name 'database%' contains illegal characters.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException =>
        e.getMessage should be(
          """Database name 'database%' contains illegal characters.
            |Use simple ascii characters, numbers, dots and dashes.""".stripMargin)
    }

    try {
      NameValidator.assertValidDatabaseName("data_base")

      fail("Expected exception \"Database name 'data_base' contains illegal characters.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException =>
        e.getMessage should be(
          """Database name 'data_base' contains illegal characters.
            |Use simple ascii characters, numbers, dots and dashes.""".stripMargin)
    }
  }

  test("Should get an error for a database name with 'system' prefix") {
    try {
      NameValidator.assertValidDatabaseName("systemdatabase")

      fail("Expected exception \"Database name 'systemdatabase' is invalid, due to the prefix 'system'.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException =>
        e.getMessage should be("Database name 'systemdatabase' is invalid, due to the prefix 'system'.")
    }
  }

  test("Should get an error for a database name with invalid first character") {
    try {
      NameValidator.assertValidDatabaseName("3database")

      fail("Expected exception \"Database name '3database' is not starting with an ASCII alphabetic character.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException =>
        e.getMessage should be("Database name '3database' is not starting with an ASCII alphabetic character.")
    }

    try {
      NameValidator.assertValidDatabaseName("_database")

      fail("Expected exception \"Database name '_database' is not starting with an ASCII alphabetic character.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException =>
        e.getMessage should be("Database name '_database' is not starting with an ASCII alphabetic character.")
    }
  }

  test("Should get an error for a database name with invalid length") {
    try {
      // Too short
      NameValidator.assertValidDatabaseName("me")

      fail("Expected exception \"The provided database name must have a length between 3 and 63 characters.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException =>
        e.getMessage should be("The provided database name must have a length between 3 and 63 characters.")
    }

    try {
      // Too long
      NameValidator.assertValidDatabaseName("ihaveallooootoflettersclearlymorethenishould_ihaveallooootoflettersclearlymorethenishould")

      fail("Expected exception \"The provided database name must have a length between 3 and 63 characters.\" but succeeded.")
    } catch {
      case e: InvalidArgumentException =>
        e.getMessage should be("The provided database name must have a length between 3 and 63 characters.")
    }
  }
}
