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

import org.neo4j.cypher.internal.v4_0.util.InvalidArgumentException
import org.scalatest.{FunSuite, Matchers}

class NameValidatorTest extends FunSuite with Matchers {

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

  test("Should not get an error for a valid role name") {
    NameValidator.assertValidRoleName("myValidRole")
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
            |Use simple ascii characters and numbers.""".stripMargin)
    }
  }

  test("Should not get an error for a valid database name") {
    NameValidator.assertValidDatabaseName("myvailddb")
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
            |Use simple ascii characters and numbers.""".stripMargin)
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
