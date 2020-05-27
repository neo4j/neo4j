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

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.exceptions.DatabaseAdministrationException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.ParameterNotFoundException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.server.security.auth.SecurityTestUtils
import org.scalatest.enablers.Messaging.messagingNatureOfThrowable

import scala.collection.JavaConverters.mapAsJavaMapConverter

class CommunityUserAdministrationCommandAcceptanceTest extends CommunityAdministrationCommandAcceptanceTestBase {

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(GraphDatabaseSettings.auth_enabled -> java.lang.Boolean.TRUE)

  // Tests for showing users

  test("should show default user") {
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
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE USER Zet SET PASSWORD 'NeX'")

    // WHEN
    val result = execute("SHOW USERS")

    // THEN
    result.toSet shouldBe Set(user("neo4j"), user("Bar"), user("Baz"), user("Zet"))
  }

  test("should show users with yield") {
    // WHEN
    val result = execute("SHOW USERS YIELD user, passwordChangeRequired")

    // THEN
    result.toSet should be(Set(Map("user"->"neo4j", "passwordChangeRequired"-> true)))
  }

  test("should show users with yield and where") {

    // WHEN
    val result = execute("SHOW USERS YIELD user, passwordChangeRequired WHERE user = 'neo4j'")

    // THEN
    result.toSet should be(Set(Map("user"->"neo4j", "passwordChangeRequired"-> true )))
  }

  test("should show users with yield and where 2") {
    // GIVEN
    execute("CREATE USER bar SET PASSWORD 'password'")

    // WHEN
    val result = execute("SHOW USERS YIELD user, passwordChangeRequired WHERE user = 'bar'")

    // THEN
    result.toList should be(List(Map("user"->"bar", "passwordChangeRequired" -> true )))
  }

  test("should show users with yield and skip") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'password'")
    execute("CREATE USER bar SET PASSWORD 'password'")
    execute("CREATE USER zoo SET PASSWORD 'password'")

    // WHEN
    val result = execute("SHOW USERS YIELD user ORDER BY user SKIP 2")

    // THEN
    result.toList should be(List(Map("user" -> "neo4j"), Map("user" -> "zoo")))
  }

  test("should show users with yield and limit") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'password'")
    execute("CREATE USER bar SET PASSWORD 'password'")

    // WHEN
    val result = execute("SHOW USERS YIELD user ORDER BY user LIMIT 1")

    // THEN
    result.toList should be(List(Map("user" -> "bar")))
  }

  test("should show users with yield and order by asc") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'password'")
    execute("CREATE USER bar SET PASSWORD 'password'")

    // WHEN
    val result = execute("SHOW USERS YIELD user ORDER BY user ASC")

    // THEN
    result.toList should be(List(Map("user" -> "bar"),Map("user" -> "foo"),Map("user" -> "neo4j")))
  }

  test("should show users with yield and order by desc") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'password'")
    execute("CREATE USER bar SET PASSWORD 'password'")

    // WHEN
    val result = execute("SHOW USERS YIELD user ORDER BY user DESC")

    // THEN
    result.toList should be(List(Map("user" -> "neo4j"),Map("user" -> "foo"),Map("user" -> "bar")))
  }

  test("should not show users with invalid yield") {

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW USERS YIELD foo, bar, baz")
    }

    // THEN
    exception.getMessage should startWith("Variable `foo` not defined")
    exception.getMessage should include("(line 1, column 18 (offset: 17))")

  }

  test("should not show users with invalid where") {

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW USERS WHERE foo = 'bar'")
    }

    // THEN
    exception.getMessage should startWith("Variable `foo` not defined")
    exception.getMessage should include("(line 1, column 18 (offset: 17))")
  }

  test("should not show users with yield and invalid where") {

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW USERS YIELD user WHERE foo = 'bar'")
    }

    // THEN
    exception.getMessage should startWith("Variable `foo` not defined")
    exception.getMessage should include("(line 1, column 29 (offset: 28))")
  }

  test("should not show users with yield and invalid skip") {

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW USERS YIELD user ORDER BY user SKIP -1")
    }

    // THEN
    exception.getMessage should startWith("Invalid input. '-1' is not a valid value. Must be a non-negative integer")
    exception.getMessage should include("(line 1, column 42 (offset: 41))")
  }

  test("should not show users with yield and invalid limit") {

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW USERS YIELD user ORDER BY user LIMIT -2")
    }

    // THEN
    exception.getMessage should startWith("Invalid input. '-2' is not a valid value. Must be a non-negative integer")
    exception.getMessage should include("(line 1, column 43 (offset: 42))")
  }

  test("should not show users with invalid order by") {

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW USERS YIELD user ORDER BY bar")
    }

    // THEN
    exception.getMessage should startWith("Variable `bar` not defined")
    exception.getMessage should include("(line 1, column 32 (offset: 31))")
  }

  test("should fail when showing users when not on system database") {
    selectDatabase(DEFAULT_DATABASE_NAME)
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("SHOW USERS")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: SHOW USERS"
  }

  // Tests for creating users

  test("should create user with password as string") {
    // WHEN
    execute("CREATE USER bar SET PASSWORD 'password'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("bar"))
    testUserLogin("bar", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("bar", "password", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with parameter") {
    // WHEN
    execute("CREATE USER $user SET PASSWORD 'password'", Map("user" -> "bar"))

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("bar"))
    testUserLogin("bar", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("bar", "password", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user using if not exists") {
    // WHEN
    execute("CREATE USER bar IF NOT EXISTS SET PASSWORD 'password'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("bar"))
    testUserLogin("bar", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("bar", "password", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with mixed password") {
    // WHEN
    execute("CREATE USER bar SET PASSWORD 'p4s5W*rd'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("bar"))
    testUserLogin("bar", "p4s5w*rd", AuthenticationResult.FAILURE)
    testUserLogin("bar", "password", AuthenticationResult.FAILURE)
    testUserLogin("bar", "p4s5W*rd", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when creating user with empty password") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD ''")
      // THEN
    } should have message "A password cannot be empty."

    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should create user with password as parameter") {
    // WHEN
    execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> "bar"))

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo"))
    testUserLogin("foo", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when creating user with numeric password as parameter") {
    the[ParameterWrongTypeException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> 123))
      // THEN
    } should have message "Only string values are accepted as password, got: Integer"

    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should fail when creating user with password as missing parameter") {
    the[ParameterNotFoundException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED")
      // THEN
    } should have message "Expected parameter(s): password"

    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should fail when creating user with password as null parameter") {
    the[ParameterNotFoundException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> null))
      // THEN
    } should have message "Expected parameter(s): password"

    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should create user with password change not required") {
    // WHEN
    execute("CREATE USER foo SET PASSWORD 'password' CHANGE NOT REQUIRED")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))
    testUserLogin("foo", "wrong", AuthenticationResult.FAILURE)
    testUserLogin("foo", "password", AuthenticationResult.SUCCESS)
  }

  test("should not be able to create user with explicit status active in community") {
    // WHEN
    assertFailure("CREATE USER foo SET PASSWORD 'password' SET STATUS ACTIVE",
      "Failed to create the specified user 'foo': 'SET STATUS' is not available in community edition.")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should not be able to create user with status suspended in community") {
    // WHEN
    assertFailure("CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDED",
      "Failed to create the specified user 'foo': 'SET STATUS' is not available in community edition.")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should fail when creating already existing user") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE USER neo4j SET PASSWORD 'password'")
      // THEN
    } should have message "Failed to create the specified user 'neo4j': User already exists."

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE USER $user SET PASSWORD 'password'", Map("user" -> "neo4j"))
      // THEN
    } should have message "Failed to create the specified user 'neo4j': User already exists."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should do nothing when creating already existing user using if not exists") {
    // WHEN
    execute("CREATE USER neo4j IF NOT EXISTS SET PASSWORD 'password' CHANGE NOT REQUIRED")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"))
  }

  test("should fail when creating user with illegal username") {
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE USER `` SET PASSWORD 'password' SET PASSWORD CHANGE REQUIRED")
      // THEN
    } should have message "The provided username is empty."

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE USER $user SET PASSWORD 'password' SET PASSWORD CHANGE REQUIRED", Map("user" -> ""))
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

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE USER $user SET PASSWORD 'password' SET PASSWORD CHANGE REQUIRED", Map("user" -> "neo:4j"))
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
    executeOnSystem("neo4j", "neo4j", "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'bar'")
    execute("SHOW USERS").toSet should be(Set(user("neo4j", passwordChangeRequired = false)))

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem("neo4j", "bar", "CREATE OR REPLACE USER neo4j SET PASSWORD 'baz'")
      // THEN
    } should have message "Failed to replace the specified user 'neo4j': Deleting yourself is not allowed."

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem("neo4j", "bar", "CREATE OR REPLACE USER $user SET PASSWORD 'baz'", Map[String, Object]("user" -> "neo4j").asJava)
      // THEN
    } should have message "Failed to replace the specified user 'neo4j': Deleting yourself is not allowed."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j", passwordChangeRequired = false))
    testUserLogin("neo4j", "bar", AuthenticationResult.SUCCESS)
    testUserLogin("neo4j", "baz", AuthenticationResult.FAILURE)
  }

  test("should get syntax exception when using both replace and if not exists") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("CREATE OR REPLACE USER foo IF NOT EXISTS SET PASSWORD 'pass'")
    }
    // THEN
    exception.getMessage should include("Failed to create the specified user 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.")
    // WHEN
    val exception2 = the[SyntaxException] thrownBy {
      execute("CREATE OR REPLACE USER $user IF NOT EXISTS SET PASSWORD 'pass'", Map("user" -> "foo"))
    }
    // THEN
    exception2.getMessage should include("Failed to create the specified user '$user': cannot have both `OR REPLACE` and `IF NOT EXISTS`.")
  }

  test("should fail when creating user when not on system database") {
    selectDatabase(DEFAULT_DATABASE_NAME)
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD 'bar'")
      // THEN
    } should have message "This is an administration command and it should be executed against the system database: CREATE USER"
  }

  // Tests for dropping users

  test("should drop user") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("DROP USER foo")

    // THEN
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))
  }

  test("should drop user with parameter") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("DROP USER $user", Map("user" -> "foo"))

    // THEN
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))
  }

  test("should drop existing user using if exists") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("DROP USER foo IF EXISTS")

    // THEN
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))
  }

  test("should re-create dropped user") {
    // GIVEN
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
    executeOnSystem("bob", "bar", "DROP USER alice")

    // THEN
    execute("SHOW USERS").toSet should be(Set(user("neo4j"), user("bob", passwordChangeRequired = false)))
  }

  test("should fail when dropping current user") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem("foo", "bar", "DROP USER $user", Map[String, Object]("user" -> "foo").asJava)
      // THEN
    } should have message "Failed to delete the specified user 'foo': Deleting yourself is not allowed."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem("foo", "bar", "DROP USER foo IF EXISTS")
      // THEN
    } should have message "Failed to delete the specified user 'foo': Deleting yourself is not allowed."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j"), user("foo", passwordChangeRequired = false))
  }

  test("should fail when dropping non-existing user") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP USER foo")
      // THEN
    } should have message "Failed to delete the specified user 'foo': User does not exist."

    // using parameter
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP USER $user", Map("user" -> "foo"))
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

    // and an invalid (non-existing) one using parameter
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP USER $user", Map("user" -> ":foo"))
      // THEN
    } should have message "Failed to delete the specified user ':foo': User does not exist."

    // THEN
    execute("SHOW USERS").toSet should be(Set(user("neo4j")))
  }

  test("should do nothing when dropping non-existing user using if exists") {
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
    selectDatabase(DEFAULT_DATABASE_NAME)
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("DROP USER foo")
      // THEN
    } should have message "This is an administration command and it should be executed against the system database: DROP USER"
  }

  // Tests for altering users

  test("should alter user password") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD 'baz'")

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should alter user password as parameter") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password", Map("password" -> "baz"))

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should fail when alter user with invalid password") {
    // GIVEN
    prepareUser("foo", "bar")

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD ''")
      // THEN
    } should have message "A password cannot be empty."

    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when alter user with empty password parameter") {
    // GIVEN
    prepareUser("foo", "bar")

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER $user SET PASSWORD $password", Map("user" -> "foo", "password" -> ""))
      // THEN
    } should have message "A password cannot be empty."

    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when alter user with current password parameter") {
    // GIVEN
    prepareUser("foo", "bar")

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password", Map("password" -> "bar"))
      // THEN
    } should have message "Failed to alter the specified user 'foo': Old password and new password cannot be the same."

    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when alter user password as list parameter") {
    // GIVEN
    prepareUser("foo", "bar")

    the[ParameterWrongTypeException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD $password", Map("password" -> Seq("baz", "boo")))
      // THEN
    } should have message "Only string values are accepted as password, got: List"
  }

  test("should alter user password mode") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should alter user password mode to change required") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD CHANGE REQUIRED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should alter user password and mode") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD 'baz' CHANGE NOT REQUIRED")

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
  }

  test("should alter user password as parameter and password mode") {
    // GIVEN
    prepareUser("foo", "bar")

    // WHEN
    execute("ALTER USER foo SET PASSWORD $password CHANGE NOT REQUIRED", Map("password" -> "baz"))

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
  }

  test("should fail when altering a non-existing user") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER foo SET PASSWORD 'baz'")
      // THEN
    } should have message "Failed to alter the specified user 'foo': User does not exist."

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("ALTER USER $user SET PASSWORD 'baz'", Map("user" -> "foo"))
      // THEN
    } should have message "Failed to alter the specified user 'foo': User does not exist."
  }

  test("should fail on altering user status from community") {
    assertFailure("ALTER USER neo4j SET STATUS ACTIVE", "Failed to alter the specified user 'neo4j': 'SET STATUS' is not available in community edition.")
    assertFailure("ALTER USER neo4j SET PASSWORD 'xxx' SET STATUS SUSPENDED", "Failed to alter the specified user 'neo4j': 'SET STATUS' is not available in community edition.")
  }

  // Tests for changing own password

  test("should change own password") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO 'baz'")

    // THEN
    testUserLogin("foo", "baz", AuthenticationResult.SUCCESS)
    testUserLogin("foo", "bar", AuthenticationResult.FAILURE)
  }

  test("should change own password when password change is required") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE REQUIRED")

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
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

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
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO ''")
      // THEN
    } should have message "A password cannot be empty."

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)

    val parameter = new util.HashMap[String, Object]()
    parameter.put("password", "bar")

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO $password", parameter)
      // THEN
    } should have message "User 'foo' failed to alter their own password: Old password and new password cannot be the same."

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password to existing password and then succeed with a new password") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    val parameter = new util.HashMap[String, Object]()
    parameter.put("password", "bar")

    the[QueryExecutionException] thrownBy {
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO $password", parameter)
    } should have message "User 'foo' failed to alter their own password: Old password and new password cannot be the same."

    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)

    val parameter2 = new util.HashMap[String, Object]()
    parameter2.put("password", "badger")

    // WHEN
    executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO $password", parameter2)

    // THEN
    testUserLogin("foo", "badger", AuthenticationResult.SUCCESS)
  }

  test("should change own password to parameter") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

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
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

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
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

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
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

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
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

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
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    val e = the[QueryExecutionException] thrownBy { // the ParameterNotFoundException exception gets wrapped in this code path
      // WHEN
      executeOnSystem("foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword")
    }
    // THEN
    e.getMessage should (be("Expected parameter(s): newPassword, currentPassword"))

    // THEN
    testUserLogin("foo", "bar", AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password when AUTH DISABLED") {
    the[IllegalStateException] thrownBy {
      // WHEN
      execute("ALTER CURRENT USER SET PASSWORD FROM 'old' TO 'new'")
      // THEN
    } should have message "User failed to alter their own password: Command not available with auth disabled."
  }

  test("should fail when changing own password when not on system database") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    the[QueryExecutionException] thrownBy { // the DatabaseManagementException gets wrapped twice in this code path
      // WHEN
      executeOn(DEFAULT_DATABASE_NAME, "foo", "bar", "ALTER CURRENT USER SET PASSWORD FROM 'bar' TO 'baz'")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: ALTER CURRENT USER SET PASSWORD"
  }

  test("should not be able to run administration commands with password change required") {
    // GIVEN
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
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE REQUIRED")

    // WHEN
    val e = the[AuthorizationViolationException] thrownBy {
      executeOn(DEFAULT_DATABASE_NAME, "foo", "bar", "MATCH (n) RETURN n")
    }

    // THEN
    e.getMessage should startWith("Permission denied.")
  }

  // helper methods

  private def user(username: String, passwordChangeRequired: Boolean = true): Map[String, Any] = {
    Map("user" -> username, "roles" -> null, "passwordChangeRequired" -> passwordChangeRequired, "suspended" -> null)
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
    val tx = graph.beginTransaction(Type.EXPLICIT, login)
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
