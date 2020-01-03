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
package org.neo4j.cypher

import java.util
import java.util.Collections

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.exceptions._
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.graphdb.{QueryExecutionException, Result}
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.server.security.auth.SecurityTestUtils
import org.scalatest.enablers.Messaging.messagingNatureOfThrowable

import scala.collection.Map

class CommunityUserAdministrationCommandAcceptanceTest extends CommunityAdministrationCommandAcceptanceTestBase {

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
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE USER Zet SET PASSWORD 'NeX'")

    // WHEN
    val result = execute("SHOW USERS")

    // THEN
    result.toSet shouldBe Set(user("neo4j"), user("Bar"), user("Baz"), user("Zet"))
  }

  test("should fail when showing users when not on system database") {
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("SHOW USERS")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: SHOW USERS"
  }

  // Tests for creating users

  test("should create user with password as string") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))

    // WHEN
    execute("CREATE USER bar SET PASSWORD 'password'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("bar"))
    testUserLogin("bar", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("bar", "password", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user using if not exists") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))

    // WHEN
    execute("CREATE USER bar IF NOT EXISTS SET PASSWORD 'password'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("bar"))
    testUserLogin("bar", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("bar", "password", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with mixed password") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    // WHEN
    assertFailure("CREATE USER foo SET PASSWORD 'password' SET STATUS ACTIVE",
      "Failed to create the specified user 'foo': 'SET STATUS' is not available in community edition.")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should not be able to create user with status suspended in community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    // WHEN
    assertFailure("CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDED",
      "Failed to create the specified user 'foo': 'SET STATUS' is not available in community edition.")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should fail when creating already existing user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE USER neo4j SET PASSWORD 'password'")
      // THEN
    } should have message "Failed to create the specified user 'neo4j': User already exists."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should do nothing when creating already existing user using if not exists") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    val existingUser = user("neo4j")
    execute("SHOW USERS").toSet shouldBe Set(existingUser)

    // WHEN
    execute("CREATE USER neo4j IF NOT EXISTS SET PASSWORD 'password' CHANGE NOT REQUIRED")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(existingUser)
  }

  test("should fail when creating user with illegal username") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
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

  test("should replace existing user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))

    // WHEN: creation
    execute("CREATE OR REPLACE USER bar SET PASSWORD 'firstPassword'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("bar"))
    testUserLogin("bar", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("bar", "firstPassword", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)

    // WHEN: replacing
    execute("CREATE OR REPLACE USER bar SET PASSWORD 'secondPassword'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("bar"))
    testUserLogin("bar", "firstPassword", AuthenticationResult.FAILURE)
    testUserLogin("bar", "secondPassword", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when replacing current user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    executeOnSystem("neo4j", "neo4j", "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'bar'")
    execute("SHOW USERS").toSet should be(Set(user("neo4j", passwordChangeRequired = false)))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      executeOnSystem("neo4j", "bar", "CREATE OR REPLACE USER neo4j SET PASSWORD 'baz'")
      // THEN
    } should have message "Failed to delete the specified user 'neo4j': Deleting yourself is not allowed."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j", passwordChangeRequired = false))
    testUserLogin("neo4j", "bar", AuthenticationResult.SUCCESS)
    testUserLogin("neo4j", "baz", AuthenticationResult.FAILURE)
  }

  test("should get syntax exception when using both replace and if not exists") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("CREATE OR REPLACE USER foo IF NOT EXISTS SET PASSWORD 'pass'")
    }
    // THEN
    exception.getMessage should include("Failed to create the specified user 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.")
  }

  test("should fail when creating user when not on system database") {
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD 'bar'")
      // THEN
    } should have message "This is an administration command and it should be executed against the system database: CREATE USER"
  }

  // Tests for dropping users

  test("should drop user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("DROP USER foo")

    // THEN
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))
  }

  test("should drop existing user using if exists") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")

    // WHEN
    execute("DROP USER foo IF EXISTS")

    // THEN
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))
  }

  test("should re-create dropped user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    prepareUser("foo", "bar")
    execute("DROP USER foo")
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))

    // WHEN
    execute("CREATE USER foo SET PASSWORD 'bar'")

    // THEN
    execute("SHOW USERS").toSet should be(Set(user("neo4j"), user("foo")))
  }

  test("should be able to drop the user that created you") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER alice SET PASSWORD 'abc' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("alice", "abc", "CREATE USER bob SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    // THEN
    execute("SHOW USERS").toSet should be(Set(
      user("neo4j"),
      user("alice", passwordChangeRequired = false),
      user("bob", passwordChangeRequired = false)
    ))

    // WHEN
    executeOnSystem("bob", "bar",  "DROP USER alice")

    // THEN
    execute("SHOW USERS").toSet should be(Set(user("neo4j"), user("bob", passwordChangeRequired = false)))
  }

  test("should fail when dropping current user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      executeOnSystem("foo", "bar", "DROP USER foo")
      // THEN
    } should have message "Failed to delete the specified user 'foo': Deleting yourself is not allowed."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      executeOnSystem("foo", "bar", "DROP USER foo IF EXISTS")
      // THEN
    } should have message "Failed to delete the specified user 'foo': Deleting yourself is not allowed."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))
  }

  test("should fail when dropping non-existing user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP USER foo")
      // THEN
    } should have message "Failed to delete the specified user 'foo': User does not exist."

    // THEN
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))

    // and an invalid (non-existing) one
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP USER `:foo`")
      // THEN
    } should have message "Failed to delete the specified user ':foo': User does not exist."

    // THEN
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))
  }

  test("should do nothing when dropping non-existing user using if exists") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))

    // WHEN
    execute("DROP USER foo IF EXISTS")

    // THEN
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))

    // and an invalid (non-existing) one

    // WHEN
    execute("DROP USER `:foo` IF EXISTS")

    // THEN
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))
  }

  test("should fail when dropping user when not on system database") {
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("DROP USER foo")
      // THEN
    } should have message "This is an administration command and it should be executed against the system database: DROP USER"
  }

  // Tests for altering users (not supported in community)

  test("should fail on altering user from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("ALTER USER neo4j SET PASSWORD 'xxx'", "Unsupported administration command: ALTER USER neo4j SET PASSWORD 'xxx'")
  }

  test("should fail on altering non-existing user with correct error message") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("ALTER USER foo SET PASSWORD 'xxx'", "Unsupported administration command: ALTER USER foo SET PASSWORD 'xxx'")
  }

  // Tests for changing own password

  test("should change own password") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    // WHEN
    executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO 'baz'")

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should change own password when password change is required") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"),
      user("foo", passwordChangeRequired = true))

    // WHEN
    executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO 'baz'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"),
      user("foo", passwordChangeRequired = false))
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should fail on changing own password from wrong password") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    the[QueryExecutionException] thrownBy { // the InvalidArgumentsException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'wrongPassword' TO 'baz'")
      // THEN
    } should have message "User 'foo' failed to alter their own password: Invalid principal or credentials."

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "baz", AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password to invalid password") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO ''")
      // THEN
    } should have message "A password cannot be empty."

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)

    val parameter = new util.HashMap[String, Object]()
    parameter.put("password", "bar")

    the[QueryExecutionException] thrownBy { // the InvalidArgumentsException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO $password", parameter)
      // THEN
    } should have message "User 'foo' failed to alter their own password: Old password and new password cannot be the same."

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should change own password to parameter") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    val parameter = new util.HashMap[String, Object]()
    parameter.put("password", "baz")

    // WHEN
    executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO $password", parameter)

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password to missing parameter") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    the[QueryExecutionException] thrownBy { // the ParameterNotFoundException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO $password")
      // THEN
    } should have message "Expected parameter(s): password"

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should change own password from parameter") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    val parameter = new util.HashMap[String, Object]()
    parameter.put("password", "bar")

    // WHEN
    executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $password TO 'baz'", parameter)

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password from integer parameter") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD '123' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    val parameter = new util.HashMap[String, Object]()
    parameter.put("password", Integer.valueOf(123))

    the[QueryExecutionException] thrownBy { // the ParameterWrongTypeException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "123", "ALTER CURRENT USER SET PASSWORD FROM $password TO 'bar'", parameter)
      // THEN
    } should have message "Only string values are accepted as password, got: Integer"

    // THEN
    testUserLogin("foo", "123", AuthenticationResult.SUCCESS)
  }

  test("should change own password from parameter to parameter") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    val parameter = new util.HashMap[String, Object]()
    parameter.put("currentPassword", "bar")
    parameter.put("newPassword", "baz")

    // WHEN
    executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword", parameter)

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password from existing parameter to missing parameter") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    val parameter = new util.HashMap[String, Object]()
    parameter.put("currentPassword", "bar")

    the[QueryExecutionException] thrownBy { // the ParameterNotFoundException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword", parameter)
      // THEN
    } should have message "Expected parameter(s): newPassword"

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password from missing parameter to existing parameter") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    val parameter = new util.HashMap[String, Object]()
    parameter.put("newPassword", "baz")

    the[QueryExecutionException] thrownBy { // the ParameterNotFoundException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword", parameter)
      // THEN
    } should have message "Expected parameter(s): currentPassword"

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password from parameter to parameter when both are missing") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    val e = the[QueryExecutionException] thrownBy { // the ParameterNotFoundException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword")
    }
    // THEN
    e.getMessage should (be("Expected parameter(s): newPassword") or be("Expected parameter(s): currentPassword"))

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password when AUTH DISABLED") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    the[IllegalStateException] thrownBy {
      // WHEN
      execute("ALTER CURRENT USER SET PASSWORD FROM 'old' TO 'new'")
      // THEN
    } should have message "User failed to alter their own password: Command not available with auth disabled."
  }

  test("should fail when changing own password when not on system database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    the[QueryExecutionException] thrownBy { // the DatabaseManagementException gets wrapped twice in this code path
      // WHEN
      executeOn(DEFAULT_DATABASE_NAME, "foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO 'baz'")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: ALTER CURRENT USER SET PASSWORD"
  }

  test("should not be able to run administration commands with password change required") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE REQUIRED")

    // WHEN
    val e = the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW USERS")
    }

    // THEN
    e.getMessage should startWith("Permission denied.")
  }

  test("should not be able to run database queries with password change required") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE REQUIRED")

    // WHEN
    val e = the[AuthorizationViolationException] thrownBy {
      executeOn( DEFAULT_DATABASE_NAME, "foo", "bar", "MATCH (n) RETURN n")
    }

    // THEN
    e.getMessage should startWith("Permission denied.")
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

  private def prepareUser(username: String, password: String): Unit = {
    execute(s"CREATE USER $username SET PASSWORD '$password'")
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user(username))
    testUserLogin(username, "wrong", AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  private def executeOnSystem(username: String, password: String, query: String,
                              params: util.Map[String, Object] = Collections.emptyMap(),
                              resultHandler: (Result.ResultRow, Int) => Unit = (_, _) => {}): Int = {
    executeOn(SYSTEM_DATABASE_NAME, username, password, query, params, resultHandler)
  }

  private def executeOn(database: String, username: String, password: String, query: String,
                        params: util.Map[String, Object] = Collections.emptyMap(),
                        resultHandler: (Result.ResultRow, Int) => Unit = (_, _) => {}): Int = {
    selectDatabase(database)
    val login = authManager.login(SecurityTestUtils.authToken(username, password))
    val tx = graph.beginTransaction(Type.explicit, login)
    try {
      var count = 0
      val result: Result = tx.execute(query, params)
      result.accept(row => {
        resultHandler(row, count)
        count = count + 1
        true
      })
      tx.commit()
      count
    } finally {
      tx.close()
    }
  }

}
