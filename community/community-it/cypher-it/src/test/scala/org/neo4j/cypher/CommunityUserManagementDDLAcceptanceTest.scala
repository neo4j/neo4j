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
package org.neo4j.cypher

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.server.security.auth.SecurityTestUtils

import scala.collection.Map

class CommunityUserManagementDDLAcceptanceTest extends CommunityDDLAcceptanceTestBase {

  // Tests for showing users

  test("should show default user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW USERS")

    // THEN
    result.toList should be(List(user("neo4j")))
  }

  test("should show all users") {
    // GIVEN
    // User  : Roles
    // neo4j : admin
    // Bar   :
    // Baz   :
    // Zet   :
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE USER Zet SET PASSWORD 'NeX'")

    // WHEN
    val result = execute("SHOW USERS")

    // THEN
    result.toSet shouldBe Set(user("neo4j"), user("Bar"), user("Baz"), user("Zet"))
  }

  test("should fail when showing users when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("SHOW USERS")
      // THEN
    } should have message "Trying to run `SHOW USERS` against non-system database."
  }

  // Tests for creating users

  test("should create user with password as string") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))

    // WHEN
    execute("CREATE USER bar SET PASSWORD 'password'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("bar"))
    testUserLogin("bar", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("bar", "password", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with mixed password") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))

    // WHEN
    execute("CREATE USER bar SET PASSWORD 'p4s5W*rd'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("bar"))
    testUserLogin("bar", "p4s5w*rd", AuthenticationResult.FAILURE)
    testUserLogin("bar", "password", AuthenticationResult.FAILURE)
    testUserLogin("bar", "p4s5W*rd", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when creating user with empty password") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD ''")
      // THEN
    } should have message "A password cannot be empty."

    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should create user with password as parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    // WHEN
    execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> "bar"))

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo"))
    testUserLogin("foo", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when creating user with numeric password as parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    the[ParameterWrongTypeException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> 123))
      // THEN
    } should have message "Only string values are accepted as password, got: Integer"

    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should fail when creating user with password as missing parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    the[ParameterNotFoundException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED")
      // THEN
    } should have message "Expected parameter(s): password"

    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should fail when creating user with password as null parameter") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    the[ParameterNotFoundException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> null))
      // THEN
    } should have message "Expected parameter(s): password"

    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should create user with password change not required") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    // WHEN
    execute("CREATE USER foo SET PASSWORD 'password' CHANGE NOT REQUIRED")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))
    testUserLogin("foo", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("foo", "password", AuthenticationResult.SUCCESS)
  }

  test("should not be able to create user with explicit status active in community") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    // WHEN
    assertFailure("CREATE USER foo SET PASSWORD 'password' SET STATUS ACTIVE",
      "'SET STATUS' is not available in community edition.")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should not be able to create user with status suspended in community") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    // WHEN
    assertFailure("CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDED",
      "'SET STATUS' is not available in community edition.")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should fail when creating already existing user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE USER neo4j SET PASSWORD 'password'")
      // THEN
    } should have message "The specified user 'neo4j' already exists."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should fail when creating user with illegal username") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE USER `` SET PASSWORD 'password' SET PASSWORD CHANGE REQUIRED")
      // THEN
    } should have message "The provided username is empty."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE USER `neo:4j` SET PASSWORD 'password' SET PASSWORD CHANGE REQUIRED")
      // THEN
    } should have message
      """Username 'neo:4j' contains illegal characters.
        |Use ascii characters that are not ',', ':' or whitespaces.""".stripMargin

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    val exception = the[SyntaxException] thrownBy {
      // WHEN
      execute("CREATE USER `3neo4j` SET PASSWORD 'password'")
      execute("CREATE USER 4neo4j SET PASSWORD 'password'")
    }
    // THEN
    exception.getMessage should include("Invalid input '4'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("3neo4j"))
  }

  test("should fail when creating user when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD 'bar'")
      // THEN
    } should have message "Trying to run `CREATE USER` against non-system database."
  }

  test("should fail on dropping user from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DROP USER neo4j", "Unsupported management command: DROP USER neo4j")
  }

  test("should fail on dropping non-existing user with correct error message") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DROP USER foo", "Unsupported management command: DROP USER foo")
  }

  test("should fail on altering user from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("ALTER USER neo4j SET PASSWORD 'xxx'", "Unsupported management command: ALTER USER neo4j SET PASSWORD 'xxx'")
  }

  test("should fail on altering non-existing user with correct error message") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("ALTER USER foo SET PASSWORD 'xxx'", "Unsupported management command: ALTER USER foo SET PASSWORD 'xxx'")
  }

  // helper methods

  private def user(username: String, passwordChangeRequired: Boolean = true): Map[String, Any] = {
    Map("user" -> username, "passwordChangeRequired" -> passwordChangeRequired)
  }

  private def testUserLogin(username: String, password: String, expected: AuthenticationResult): Unit = {
    val login = authManager.login(SecurityTestUtils.authToken(username, password))
    val result = login.subject().getAuthenticationResult
    result should be(expected)
  }
}
