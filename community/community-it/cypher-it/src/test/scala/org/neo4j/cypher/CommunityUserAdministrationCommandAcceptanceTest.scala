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
package org.neo4j.cypher

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.auth_enabled
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.ParameterNotFoundException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.server.security.SecureHasher
import org.neo4j.server.security.SystemGraphCredential
import org.neo4j.server.security.auth.SecurityTestUtils
import org.neo4j.server.security.systemgraph.SecurityGraphHelper.NATIVE_AUTH
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.AUTH_ID
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.AUTH_PROVIDER
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.HAS_AUTH
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_CREDENTIALS
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_EXPIRED
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_LABEL
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_NAME
import org.scalatest.enablers.Messaging.messagingNatureOfThrowable

import java.util
import java.util.Collections

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Using

//noinspection RedundantDefaultArgument
// uses default argument for clarification in some tests
class CommunityUserAdministrationCommandAcceptanceTest extends CommunityAdministrationCommandAcceptanceTestBase {
  private val defaultUsername = "neo4j"
  private val defaultUser = user(defaultUsername)
  private val defaultUserMap = Map("user" -> defaultUsername)
  private val username = "alexandra"
  private val newUsername = "oof"
  private val userAlexandra = user(username)
  private val userAlexandraMap = Map("user" -> username)
  private val password = "barpassword"
  private val newPassword = "newpassword"
  private val wrongPassword = "wrongpassword"
  private val alterDefaultUserQuery = s"ALTER USER $defaultUsername SET PASSWORD '$password' CHANGE NOT REQUIRED"

  override def databaseConfig(): Map[Setting[_], Object] =
    super.databaseConfig() ++ Map(
      auth_enabled -> java.lang.Boolean.TRUE
    )

  def authManager: AuthManager = graph.getDependencyResolver.resolveDependency(classOf[AuthManager])

  // Tests for showing users

  test("should show default user") {
    // WHEN
    val result = execute("SHOW USERS")

    // THEN
    result.toList should be(List(defaultUser))
  }

  test("should show all users") {
    // GIVEN
    // User  : Roles
    // neo4j : admin
    // Bar   :
    // Baz   :
    // Zet   :
    execute("CREATE USER Bar SET PASSWORD 'neopassword'")
    execute("CREATE USER Baz SET PASSWORD 'NEOPASSWORD'")
    execute("CREATE USER Zet SET PASSWORD 'NeXPassword'")

    // WHEN
    val result = execute("SHOW USERS")

    // THEN
    result.toSet shouldBe Set(defaultUser, user("Bar"), user("Baz"), user("Zet"))
  }

  test("should show users with yield") {
    // WHEN
    val result = execute("SHOW USERS YIELD user, passwordChangeRequired")

    // THEN
    result.toSet should be(Set(Map[String, Any]("user" -> defaultUsername, "passwordChangeRequired" -> true)))
  }

  test("should show users with yield and where, default user") {
    // WHEN
    val result = execute(s"SHOW USERS YIELD user, passwordChangeRequired WHERE user = '$defaultUsername'")

    // THEN
    result.toSet should be(Set(Map[String, Any]("user" -> defaultUsername, "passwordChangeRequired" -> true)))
  }

  test("should show users with yield and where, created user") {
    // GIVEN
    execute(s"CREATE USER $username SET PASSWORD '$password'")

    // WHEN
    val result = execute(s"SHOW USERS YIELD user, passwordChangeRequired WHERE user = '$username'")

    // THEN
    result.toList should be(List(Map[String, Any]("user" -> username, "passwordChangeRequired" -> true)))
  }

  test("should show users with yield and skip") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'password'")
    execute("CREATE USER bar SET PASSWORD 'password'")
    execute("CREATE USER zoo SET PASSWORD 'password'")

    // WHEN
    val result = execute("SHOW USERS YIELD user ORDER BY user SKIP 2")

    // THEN
    result.toList should be(List(defaultUserMap, Map("user" -> "zoo")))
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
    execute(s"CREATE USER $username SET PASSWORD 'password'")
    execute("CREATE USER bar SET PASSWORD 'password'")

    // WHEN
    val result = execute("SHOW USERS YIELD user ORDER BY user ASC")

    // THEN
    result.toList should be(List(userAlexandraMap, Map("user" -> "bar"), defaultUserMap))
  }

  test("should show users with yield and order by desc") {
    // GIVEN
    execute(s"CREATE USER $username SET PASSWORD 'password'")
    execute("CREATE USER bar SET PASSWORD 'password'")

    // WHEN
    val result = execute("SHOW USERS YIELD user ORDER BY user DESC")

    // THEN
    result.toList should be(List(defaultUserMap, Map("user" -> "bar"), userAlexandraMap))
  }

  test("should show users with yield and return") {
    // WHEN
    val result = execute("SHOW USERS YIELD user RETURN user")

    // THEN
    result.toSet should be(Set(defaultUserMap))
  }

  test("should count users with yield and return") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'password' CHANGE NOT REQUIRED")

    // WHEN
    val result =
      execute("SHOW USERS YIELD user, passwordChangeRequired RETURN count(user) as count, passwordChangeRequired")

    // THEN
    result.toSet should be(Set(
      Map[String, Any]("count" -> 1, "passwordChangeRequired" -> true),
      Map[String, Any]("count" -> 1, "passwordChangeRequired" -> false)
    ))
  }

  test("should show users with yield, return and skip") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'password'")
    execute("CREATE USER bar SET PASSWORD 'password'")
    execute("CREATE USER zoo SET PASSWORD 'password'")

    // WHEN
    val result = execute("SHOW USERS YIELD * RETURN user ORDER BY user SKIP 2")

    // THEN
    result.toList should be(List(defaultUserMap, Map("user" -> "zoo")))
  }

  test("should show users with yield, return and limit") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'password'")
    execute("CREATE USER bar SET PASSWORD 'password'")

    // WHEN
    val result = execute("SHOW USERS YIELD * RETURN user ORDER BY user LIMIT 1")

    // THEN
    result.toList should be(List(Map("user" -> "bar")))
  }

  test("should show users with yield and aliasing") {
    // GIVEN
    execute(s"CREATE USER $username SET PASSWORD 'password'")
    execute("CREATE USER bar SET PASSWORD 'password'")

    // WHEN
    val result = execute(s"SHOW USERS YIELD user AS foo WHERE foo = '$username' RETURN foo")

    // THEN
    result.toList should be(List(Map("foo" -> username)))
  }

  test("should show users with yield and return with aliasing") {
    // GIVEN
    execute(s"CREATE USER $username SET PASSWORD 'password'")
    execute("CREATE USER bar SET PASSWORD 'password'")

    // WHEN
    val result = execute(s"SHOW USERS YIELD user WHERE user = '$username' RETURN user as foo")

    // THEN
    result.toList should be(List(Map("foo" -> username)))
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
    val exceptionBar = the[SyntaxException] thrownBy {
      execute("SHOW USERS YIELD user ORDER BY bar")
    }

    // THEN
    exceptionBar.getMessage should startWith("Variable `bar` not defined")
    exceptionBar.getMessage should include("(line 1, column 32 (offset: 31))")

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      // 'passwordChangeRequired' is a valid column but not yielded
      execute("SHOW USERS YIELD user ORDER BY passwordChangeRequired")
    }

    // THEN
    exception.getMessage should startWith("Variable `passwordChangeRequired` not defined")
    exception.getMessage should include("(line 1, column 32 (offset: 31))")
  }

  test("should fail when showing users when not on system database") {
    assertFailWhenNotOnSystem("SHOW USERS", "SHOW USERS")
  }

  test("should show native only user with auth") {
    // WHEN
    val result = execute("SHOW USERS WITH AUTH")

    // THEN
    result.toSet should be(Set(addNativeAuthColumns(defaultUser, pwChangeRequired = true)))
  }

  test("should show native only users with auth and yield") {
    // GIVEN
    execute(alterDefaultUserQuery)

    // WHEN
    val result = execute(s"SHOW USERS WITH AUTH YIELD user, provider, auth")

    // THEN
    result.toSet should be(Set(addNativeAuthColumns(
      Map[String, Any]("user" -> defaultUsername),
      pwChangeRequired = false
    )))
  }

  test("should show native only users with auth and where") {
    // GIVEN
    execute(s"CREATE USER $username SET PASSWORD '$password'")

    // WHEN
    val result = execute(s"SHOW USERS WITH AUTH WHERE user = '$defaultUsername'")

    // THEN
    result.toSet should be(Set(addNativeAuthColumns(defaultUser, pwChangeRequired = true)))
  }

  test("should show users with auth with multiple users with mixed auth info") {
    // Community should only have native auth,
    // but if for some reason there is external auth it might be nice to show regardless
    // and therefore test even if we have to fake the external auth parts

    /** Takes in an existing user and adds external auths
     *
     * @param user username to add auths for
     * @param externalAuths List of maps with provider and id for each wanted external auth: Map("provider" -> "x", "id" ->"y")
     * @param keepNativeAuth if false, the native auth for the user is removed
     */
    def fakeExternalAuthForUser(
      user: String,
      externalAuths: List[Map[String, String]],
      keepNativeAuth: Boolean = true
    ): Unit = {
      Using.resource(graphOps.beginTx()) { tx =>
        val userNode = tx.findNode(USER_LABEL, USER_NAME, user)

        if (!keepNativeAuth) {
          // remove native auth
          userNode.removeProperty(USER_CREDENTIALS)
          userNode.removeProperty(USER_EXPIRED)

          userNode.getRelationships(HAS_AUTH)
            .stream()
            .filter(rel => rel.getOtherNode(userNode).getProperty(AUTH_PROVIDER).equals(NATIVE_AUTH))
            .forEach(rel => {
              val node = rel.getOtherNode(userNode)
              rel.delete()
              node.delete()
            })
        }

        externalAuths.foreach(auth => {
          val authNode = tx.createNode()
          authNode.setProperty(AUTH_PROVIDER, auth("provider"))
          authNode.setProperty(AUTH_ID, auth("id"))
          userNode.createRelationshipTo(authNode, HAS_AUTH)
        })

        tx.commit()
      }
    }

    def addExternalAuthColumns(userMap: Map[String, Any], provider: String, id: String): Map[String, Any] = {
      val authMap = if (id == null) null else Map("id" -> id)
      userMap ++ Map("provider" -> provider, "auth" -> authMap)
    }

    // GIVEN
    execute(s"CREATE USER $username SET PASSWORD '$password'")
    fakeExternalAuthForUser(
      username,
      List(
        Map("provider" -> "Foo", "id" -> s"$username.foo@example.com"),
        Map("provider" -> "Bar", "id" -> s"$username.bar@example.com")
      ),
      keepNativeAuth = false
    )
    execute(s"CREATE USER $newUsername SET PASSWORD '$password' CHANGE NOT REQUIRED")
    fakeExternalAuthForUser(
      newUsername,
      List(
        Map("provider" -> "Baz", "id" -> s"$newUsername.baz@example.com")
      )
    )

    // WHEN
    val resultWithAuth = execute("SHOW USERS WITH AUTH YIELD * ORDER BY user, provider")

    // THEN
    val user1Map = user(username) ++ Map("passwordChangeRequired" -> null)
    val user2Map = user(newUsername, passwordChangeRequired = false)
    resultWithAuth.toList should be(List(
      addNativeAuthColumns(defaultUser, pwChangeRequired = true),
      addExternalAuthColumns(user1Map, "Foo", s"$username.foo@example.com"),
      addExternalAuthColumns(user1Map, "Bar", s"$username.bar@example.com"),
      addNativeAuthColumns(user2Map, pwChangeRequired = false),
      addExternalAuthColumns(user2Map, "Baz", s"$newUsername.baz@example.com")
    ).sortBy(m => (m("user").asInstanceOf[String], m("provider").asInstanceOf[String])))

    // WHEN
    val resultWithoutAuth = execute("SHOW USERS YIELD * ORDER BY user")

    // THEN
    resultWithoutAuth.toList should be(List(
      defaultUser,
      user(username) ++ Map("passwordChangeRequired" -> null),
      user(newUsername, passwordChangeRequired = false)
    ).sortBy(m => m("user").asInstanceOf[String]))
  }

  // Tests for showing current user

  test("should show current user") {
    // GIVEN
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem(
      username,
      password,
      "SHOW CURRENT USER",
      resultHandler = (row, _) => {
        // THEN
        row.get("user") should be(username)
        row.get("roles") should be(null)
        row.get("passwordChangeRequired") shouldBe false
        row.get("suspended") shouldBe null
      }
    ) should be(1)
  }

  test("should show current user with yield, where and return") {
    // GIVEN
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem(
      username,
      password,
      "SHOW CURRENT USER YIELD * WHERE user = $name RETURN user, passwordChangeRequired",
      Collections.singletonMap("name", username),
      resultHandler = (row, _) => {
        // THEN
        row.get("user") should be(username)
        row.get("passwordChangeRequired") shouldBe false
      }
    ) should be(1)
  }

  test("should only show current user") {
    // GIVEN
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")
    execute(s"CREATE USER bar SET PASSWORD '$password' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem(
      username,
      password,
      "SHOW CURRENT USER",
      resultHandler = (row, _) => {
        // THEN
        row.get("user") should be(username)
        row.get("roles") should be(null)
        row.get("passwordChangeRequired") shouldBe false
        row.get("suspended") shouldBe null
      }
    ) should be(1)
  }

  test("should not return a user that is not the current user") {
    // GIVEN
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")
    execute(s"CREATE USER bar SET PASSWORD '$password' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem(username, password, "SHOW CURRENT USER WHERE user='bar'") should be(0)
  }

  test("should not return a user when not logged in") {
    // THEN
    execute("SHOW CURRENT USER").toList.size should be(0)
  }

  // Tests for creating users

  test("should create user with password as string") {
    // WHEN
    execute(s"CREATE USER $username SET PASSWORD '$password'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, userAlexandra)
    testUserLogin(username, wrongPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with username as parameter") {
    // WHEN
    execute(s"CREATE USER $$user SET PASSWORD '$password'", userAlexandraMap)

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, userAlexandra)
    testUserLogin(username, wrongPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with password using SET AUTH syntax") {
    // WHEN
    execute(s"CREATE USER $username SET AUTH 'native' { SET PASSWORD '$password' }")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, userAlexandra)
    testUserLogin(username, wrongPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create non-existing user using if not exists") {
    // WHEN
    execute(s"CREATE USER $username IF NOT EXISTS SET PASSWORD '$password'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, userAlexandra)
    testUserLogin(username, wrongPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with mixed password") {
    // WHEN
    execute(s"CREATE USER $username SET PASSWORD 'p4s5W*rd'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, userAlexandra)
    testUserLogin(username, "p4s5w*rd", AuthenticationResult.FAILURE) // w has wrong case
    testUserLogin(username, "password", AuthenticationResult.FAILURE)
    testUserLogin(username, "p4s5W*rd", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when creating user with empty password") {
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD ''")
      // THEN
    } should have message "A password cannot be empty."

    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should create user with password as parameter") {
    // WHEN
    execute(s"CREATE USER $username SET PASSWORD $$password CHANGE REQUIRED", Map("password" -> password))

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username))
    testUserLogin(username, wrongPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when creating user with password shorter than limit (default 8)") {
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute(s"CREATE USER $username SET PASSWORD '1234567'")
      // THEN
    } should have message "A password must be at least 8 characters."

    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should fail when creating user with password parameter shorter than limit (default 8)") {
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute(s"CREATE USER $username SET PASSWORD $param", Map(paramName -> "1234567"))
      // THEN
    } should have message "A password must be at least 8 characters."

    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should create user with username and password as same parameter") {
    // WHEN
    execute("CREATE USER $user SET PASSWORD $user CHANGE REQUIRED", Map("user" -> username))

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username))
    testUserLogin(username, username, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when creating user with numeric password as parameter") {
    the[ParameterWrongTypeException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> 123))
      // THEN
    } should have message "Expected password parameter $password to have type String but was Integer"

    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should fail when creating user with password as missing parameter") {
    the[ParameterNotFoundException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED")
      // THEN
    } should have message "Expected parameter(s): password"

    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should fail when creating user with password as null parameter") {
    the[ParameterNotFoundException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED", Map("password" -> null))
      // THEN
    } should have message "Expected parameter(s): password"

    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should create user with password change not required") {
    // WHEN
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username, passwordChangeRequired = false))
    testUserLogin(username, wrongPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.SUCCESS)
  }

  test("should create user with password change not required using SET AUTH syntax") {
    // WHEN
    execute(
      s"""CREATE USER $username
         |SET AUTH 'native' {
         |  SET PASSWORD '$password'
         |  SET PASSWORD CHANGE NOT REQUIRED
         |}""".stripMargin
    )

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username, passwordChangeRequired = false))
    testUserLogin(username, wrongPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.SUCCESS)
  }

  test("should not be able to create user with explicit status active in community") {
    // WHEN
    assertFailure(
      "CREATE USER foo SET PASSWORD 'password' SET STATUS ACTIVE",
      "Failed to create the specified user 'foo': 'SET STATUS' is not available in community edition."
    )

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should not be able to create user with status suspended in community") {
    // WHEN
    assertFailure(
      "CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDED",
      "Failed to create the specified user 'foo': 'SET STATUS' is not available in community edition."
    )

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should not be able to create user with a default database in community") {
    // WHEN
    assertFailure(
      "CREATE USER foo SET PASSWORD 'password' SET HOME DATABASE foo",
      "Failed to create the specified user 'foo': 'HOME DATABASE' is not available in community edition."
    )

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should not be able to create user with an external auth in community") {
    // WHEN
    assertFailure(
      "CREATE USER foo SET AUTH 'bar' { SET ID 'baz' }",
      "Failed to create the specified user 'foo': `SET AUTH 'bar'` is not available in community edition."
    )

    // WHEN
    assertFailure(
      "CREATE USER foo SET AUTH 'bar' { SET ID 'baz' } SET AUTH 'baz' { SET ID 'qux' }",
      "Failed to create the specified user 'foo': `SET AUTH 'bar'`, `SET AUTH 'baz'` are not available in community edition."
    )

    // WHEN
    assertFailure(
      "CREATE USER foo SET AUTH 'bar' { SET ID 'baz' } SET AUTH 'native' { SET PASSWORD 'password' }",
      "Failed to create the specified user 'foo': `SET AUTH 'bar'` is not available in community edition."
    )

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should fail when creating already existing user") {
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute(s"CREATE USER $defaultUsername SET PASSWORD 'password'")
      // THEN
    } should have message s"Failed to create the specified user '$defaultUsername': User already exists."

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE USER $user SET PASSWORD 'password'", defaultUserMap)
      // THEN
    } should have message s"Failed to create the specified user '$defaultUsername': User already exists."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should do nothing when creating already existing user using if not exists") {
    // WHEN
    val result =
      execute(s"CREATE USER $defaultUsername IF NOT EXISTS SET PLAINTEXT PASSWORD '$wrongPassword' CHANGE NOT REQUIRED")

    // THEN
    result.queryStatistics().systemUpdates should be(0)
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
    testUserLogin(defaultUsername, wrongPassword, AuthenticationResult.FAILURE)
    testUserLogin(defaultUsername, "neo4j", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
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
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)

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
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)

    val exception = the[SyntaxException] thrownBy {
      // WHEN
      execute("CREATE USER `3neo4j` SET PASSWORD 'password'")
      execute("CREATE USER 4neo4j SET PASSWORD 'password'")
    }
    // THEN
    exception.getMessage should (include("Invalid input '4neo4j'") or include("No viable alternative"))

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user("3neo4j"))
  }

  test("should replace existing user") {
    // WHEN: creation
    execute(s"CREATE OR REPLACE USER $username SET PASSWORD 'firstPassword'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, userAlexandra)
    testUserLogin(username, wrongPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, "firstPassword", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)

    // WHEN: replacing
    execute(s"CREATE OR REPLACE USER $username SET PASSWORD 'secondPassword'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, userAlexandra)
    testUserLogin(username, "firstPassword", AuthenticationResult.FAILURE)
    testUserLogin(username, "secondPassword", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when replacing current user") {
    // GIVEN
    executeOnSystem(defaultUsername, "neo4j", s"ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO '$password'")
    execute("SHOW USERS").toSet should be(Set(user(defaultUsername, passwordChangeRequired = false)))

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem(defaultUsername, password, s"CREATE OR REPLACE USER $defaultUsername SET PASSWORD 'wrong'")
      // THEN
    } should have message s"Failed to replace the specified user '$defaultUsername': Deleting yourself is not allowed."

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem(
        defaultUsername,
        password,
        "CREATE OR REPLACE USER $user SET PASSWORD 'wrong'",
        Map[String, Object]("user" -> defaultUsername).asJava
      )
      // THEN
    } should have message s"Failed to replace the specified user '$defaultUsername': Deleting yourself is not allowed."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user(defaultUsername, passwordChangeRequired = false))
    testUserLogin(defaultUsername, password, AuthenticationResult.SUCCESS)
    testUserLogin(defaultUsername, wrongPassword, AuthenticationResult.FAILURE)
  }

  test("should get syntax exception when using both replace and if not exists") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("CREATE OR REPLACE USER foo IF NOT EXISTS SET PASSWORD 'pass'")
    }
    // THEN
    exception.getMessage should include(
      "Failed to create the specified user 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`."
    )

    // WHEN
    val exception2 = the[SyntaxException] thrownBy {
      execute("CREATE OR REPLACE USER $user IF NOT EXISTS SET PASSWORD 'pass'", userAlexandraMap)
    }
    // THEN
    exception2.getMessage should include(
      "Failed to create the specified user '$user': cannot have both `OR REPLACE` and `IF NOT EXISTS`."
    )
  }

  test("should create user with encrypted password") {
    // GIVEN
    val encryptedPassword = getMaskedEncodedPassword(password)

    // WHEN
    execute(s"CREATE USER $username SET ENCRYPTED PASSWORD '$encryptedPassword'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username))
    testUserLogin(username, wrongPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, encryptedPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with old configuration encrypted password") {
    // GIVEN
    val version = "0"
    val encryptedPassword = getMaskedEncodedPassword(password, version)

    // WHEN
    execute(s"CREATE USER $username SET ENCRYPTED PASSWORD '$encryptedPassword'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username))
    testUserLogin(username, wrongPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, encryptedPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail to create user with unmasked encrypted password") {
    // GIVEN
    val unmaskedEncryptedPassword =
      "SHA-256,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab,1024"

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute(s"CREATE USER foo SET ENCRYPTED PASSWORD '$unmaskedEncryptedPassword'")
      // THEN
    } should have message "Incorrect format of encrypted password. Correct format is '<encryption-version>,<hash>,<salt>'."

    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should fail to create user with encrypted password and unsupported version number") {
    // GIVEN
    val incorrectlyEncryptedPassword =
      "8,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab"

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute(s"CREATE USER foo SET ENCRYPTED PASSWORD '$incorrectlyEncryptedPassword'")
      // THEN
    } should have message "The encryption version specified is not available."

    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should fail to create user with encrypted password and missing salt/hash") {
    // GIVEN
    val incorrectlyEncryptedPassword = "1,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab"

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute(s"CREATE USER foo SET ENCRYPTED PASSWORD '$incorrectlyEncryptedPassword'")
      // THEN
    } should have message "Incorrect format of encrypted password. Correct format is '<encryption-version>,<hash>,<salt>'."

    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should fail to create user with empty encrypted password") {
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE USER foo SET ENCRYPTED PASSWORD ''")
      // THEN
    } should have message "Incorrect format of encrypted password. Correct format is '<encryption-version>,<hash>,<salt>'."

    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should create user with encrypted password as parameter") {
    // GIVEN
    val encryptedPassword = getMaskedEncodedPassword(password)

    // WHEN
    execute(
      s"CREATE USER $username SET ENCRYPTED PASSWORD $$password CHANGE REQUIRED",
      Map("password" -> encryptedPassword)
    )

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username))
    testUserLogin(username, wrongPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, encryptedPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when creating user when not on system database") {
    assertFailWhenNotOnSystem("CREATE USER foo SET PASSWORD 'bar'", "CREATE USER")
  }

  // Test for renaming users

  test("should rename user") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"RENAME USER $username TO $newUsername")

    // THEN
    testUserLogin(username, password, AuthenticationResult.FAILURE)
    testUserLogin(newUsername, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should rename user with parameter for new username") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"RENAME USER $username TO $$to", Map("to" -> newUsername))

    // THEN
    testUserLogin(username, password, AuthenticationResult.FAILURE)
    testUserLogin(newUsername, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should rename user with parameter for old username") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"RENAME USER $$from TO $newUsername", Map("from" -> username))

    // THEN
    testUserLogin(username, password, AuthenticationResult.FAILURE)
    testUserLogin(newUsername, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should rename user with parameters for both inputs") {
    // GIVEN
    prepareUser()

    // WHEN
    execute("RENAME USER $from TO $to", Map("from" -> username, "to" -> newUsername))

    // THEN
    testUserLogin(username, password, AuthenticationResult.FAILURE)
    testUserLogin(newUsername, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should rename default user") {
    // GIVEN
    execute(alterDefaultUserQuery)

    // WHEN
    execute(s"RENAME USER $defaultUsername TO $newUsername")

    // THEN
    testUserLogin(defaultUsername, password, AuthenticationResult.FAILURE)
    testUserLogin(newUsername, password, AuthenticationResult.SUCCESS)
  }

  test("should rename existing user using if exists") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"RENAME USER $username IF EXISTS TO $newUsername")

    // THEN
    testUserLogin(username, password, AuthenticationResult.FAILURE)
    testUserLogin(newUsername, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when renaming non-existing user") {
    // WHEN
    val exception = the[InvalidArgumentException] thrownBy execute(s"RENAME USER iDontExist TO $newUsername")

    // THEN
    exception.getMessage should startWith(
      s"Failed to rename the specified user 'iDontExist' to '$newUsername': The user 'iDontExist' does not exist."
    )
    execute("SHOW USERS").toSet should be(Set(defaultUser))
  }

  test("should do nothing when renaming non-existing role if exists") {
    // WHEN
    execute(s"RENAME USER iDontExist IF EXISTS TO $newUsername")

    // THEN
    execute("SHOW USERS").toSet should be(Set(defaultUser))
  }

  test("should fail to rename to existing name") {
    // GIVEN
    prepareUser()
    execute(s"CREATE USER $newUsername SET PASSWORD '$newPassword'")

    // WHEN
    val exception = the[InvalidArgumentException] thrownBy execute(s"RENAME USER $username TO $newUsername")

    // THEN
    exception.getMessage should startWith(
      s"Failed to rename the specified user '$username' to '$newUsername': User '$newUsername' already exists."
    )

    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin(newUsername, password, AuthenticationResult.FAILURE)
    testUserLogin(newUsername, newPassword, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail to rename to existing name using if exists") {
    // GIVEN
    prepareUser()
    execute(s"CREATE USER $newUsername SET PASSWORD '$newPassword'")

    // WHEN
    val exception = the[InvalidArgumentException] thrownBy execute(s"RENAME USER $username IF EXISTS TO $newUsername")

    // THEN
    exception.getMessage should startWith(
      s"Failed to rename the specified user '$username' to '$newUsername': User '$newUsername' already exists."
    )

    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin(newUsername, password, AuthenticationResult.FAILURE)
    testUserLogin(newUsername, newPassword, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should create user with old name after rename") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"RENAME USER $username TO $newUsername")
    execute(s"CREATE USER $username SET PASSWORD '$newPassword'")

    // THEN
    testUserLogin(username, password, AuthenticationResult.FAILURE)
    testUserLogin(newUsername, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin(username, newPassword, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should rename user to old name after rename") {
    // GIVEN
    execute(s"CREATE USER alice SET PASSWORD '$password'")
    execute(s"CREATE USER charlie SET PASSWORD '$newPassword'")

    // WHEN
    execute("RENAME USER alice TO bob")
    execute("RENAME USER charlie TO alice")

    // THEN
    testUserLogin("alice", password, AuthenticationResult.FAILURE)
    testUserLogin("bob", password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)

    testUserLogin("charlie", newPassword, AuthenticationResult.FAILURE)
    testUserLogin("alice", newPassword, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should not create user with new name after rename") {
    // GIVEN
    prepareUser()
    execute(s"RENAME USER $username TO $newUsername")

    // WHEN .. THEN
    val exception =
      the[InvalidArgumentException] thrownBy execute(s"CREATE USER $newUsername SET PASSWORD '$newPassword'")
    exception.getMessage should startWith(s"Failed to create the specified user '$newUsername': User already exists.")

    testUserLogin(username, password, AuthenticationResult.FAILURE)
    testUserLogin(newUsername, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin(newUsername, newPassword, AuthenticationResult.FAILURE)
  }

  test("should rename current user") {
    // GIVEN
    execute(alterDefaultUserQuery)

    // WHEN
    executeOnSystem(defaultUsername, password, s"RENAME USER $defaultUsername TO $newUsername")

    // THEN
    an[AuthorizationViolationException] should be thrownBy {
      executeOn(DEFAULT_DATABASE_NAME, defaultUsername, password, "RETURN 1")
    }

    testUserLogin(defaultUsername, password, AuthenticationResult.FAILURE)
    testUserLogin(newUsername, password, AuthenticationResult.SUCCESS)
  }

  // Tests for dropping users

  test("should drop user") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"DROP USER $username")

    // THEN
    execute("SHOW USERS").toSet should be(Set(defaultUser))
  }

  test("should drop user with parameter") {
    // GIVEN
    prepareUser()

    // WHEN
    execute("DROP USER $user", userAlexandraMap)

    // THEN
    execute("SHOW USERS").toSet should be(Set(defaultUser))
  }

  test("should drop existing user using if exists") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"DROP USER $username IF EXISTS")

    // THEN
    execute("SHOW USERS").toSet should be(Set(defaultUser))
  }

  test("should re-create dropped user") {
    // GIVEN
    prepareUser()
    execute(s"DROP USER $username")
    execute("SHOW USERS").toSet should be(Set(defaultUser))

    // WHEN
    execute(s"CREATE USER $username SET PASSWORD '$password'")

    // THEN
    execute("SHOW USERS").toSet should be(Set(defaultUser, user(username)))
  }

  test("should be able to drop the user that created you") {
    // GIVEN
    execute(s"CREATE USER alice SET PASSWORD '$password' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("alice", password, s"CREATE USER bob SET PASSWORD '$password' CHANGE NOT REQUIRED")

    // THEN
    execute("SHOW USERS").toSet should be(Set(
      defaultUser,
      user("alice", passwordChangeRequired = false),
      user("bob", passwordChangeRequired = false)
    ))

    // WHEN
    executeOnSystem("bob", password, "DROP USER alice")

    // THEN
    execute("SHOW USERS").toSet should be(Set(defaultUser, user("bob", passwordChangeRequired = false)))
  }

  test("should fail when dropping current user") {
    // GIVEN
    prepareUser(changeRequired = false)

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem(username, password, "DROP USER $user", Map[String, Object]("user" -> username).asJava)
      // THEN
    } should have message s"Failed to delete the specified user '$username': Deleting yourself is not allowed."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username, passwordChangeRequired = false))

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem(username, password, s"DROP USER $username IF EXISTS")
      // THEN
    } should have message s"Failed to delete the specified user '$username': Deleting yourself is not allowed."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username, passwordChangeRequired = false))
  }

  test("should fail when dropping non-existing user") {
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute(s"DROP USER $username")
      // THEN
    } should have message s"Failed to delete the specified user '$username': User does not exist."

    // using parameter
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("DROP USER $user", userAlexandraMap)
      // THEN
    } should have message s"Failed to delete the specified user '$username': User does not exist."

    // THEN
    execute("SHOW USERS").toSet should be(Set(defaultUser))

    // and an invalid (non-existing) one
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("DROP USER `:foo`")
      // THEN
    } should have message "Failed to delete the specified user ':foo': User does not exist."

    // and an invalid (non-existing) one using parameter
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("DROP USER $user", Map("user" -> ":foo"))
      // THEN
    } should have message "Failed to delete the specified user ':foo': User does not exist."

    // THEN
    execute("SHOW USERS").toSet should be(Set(defaultUser))
  }

  test("should do nothing when dropping non-existing user using if exists") {
    // WHEN
    val result = execute("DROP USER foo IF EXISTS")

    // THEN
    result.queryStatistics().systemUpdates should be(0)
    execute("SHOW USERS").toSet should be(Set(defaultUser))

    // and an invalid (non-existing) one

    // WHEN
    val resultInvalid = execute("DROP USER `:foo` IF EXISTS")

    // THEN
    resultInvalid.queryStatistics().systemUpdates should be(0)
    execute("SHOW USERS").toSet should be(Set(defaultUser))
  }

  test("should fail when dropping user when not on system database") {
    assertFailWhenNotOnSystem("DROP USER foo", "DROP USER")
  }

  // Tests for altering users

  test("should alter user password") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"ALTER USER $username SET PASSWORD '$newPassword'")

    // THEN
    testUserLogin(username, newPassword, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin(username, password, AuthenticationResult.FAILURE)
  }

  test("should alter user password as parameter") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"ALTER USER $username SET PLAINTEXT PASSWORD $$password", Map("password" -> newPassword))

    // THEN
    testUserLogin(username, newPassword, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin(username, password, AuthenticationResult.FAILURE)
  }

  test("should alter user password using SET AUTH syntax") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"ALTER USER $username SET AUTH 'native' { SET PASSWORD '$newPassword' }")

    // THEN
    testUserLogin(username, newPassword, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin(username, password, AuthenticationResult.FAILURE)
  }

  test("should alter user password with same parameter as username") {
    // GIVEN
    prepareUser()

    // WHEN
    execute("ALTER USER $user SET PASSWORD $user", Map("user" -> username))

    // THEN
    testUserLogin(username, username, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin(username, password, AuthenticationResult.FAILURE)
  }

  test("should fail when alter user with invalid password") {
    // GIVEN
    prepareUser()

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute(s"ALTER USER $username SET PASSWORD '' CHANGE NOT REQUIRED")
      // THEN
    } should have message "A password cannot be empty."

    // THEN
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when alter user with empty password parameter") {
    // GIVEN
    prepareUser()

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("ALTER USER $user SET PASSWORD $password", Map("user" -> username, "password" -> ""))
      // THEN
    } should have message "A password cannot be empty."

    // THEN
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when alter user with current password parameter") {
    // GIVEN
    prepareUser()

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute(s"ALTER USER $username SET PASSWORD $$password CHANGE NOT REQUIRED", Map("password" -> password))
      // THEN
    } should have message s"Failed to alter the specified user '$username': Old password and new password cannot be the same."

    // THEN
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when alter user password as list parameter") {
    // GIVEN
    prepareUser()

    the[ParameterWrongTypeException] thrownBy {
      // WHEN
      execute(s"ALTER USER $username SET PASSWORD $$password", Map("password" -> Seq("bar", "boo")))
      // THEN
    } should have message "Expected password parameter $password to have type String but was List"

    // THEN
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should alter user password mode") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"ALTER USER $username SET PASSWORD CHANGE NOT REQUIRED")

    // THEN
    testUserLogin(username, password, AuthenticationResult.SUCCESS)
  }

  test("should alter user password mode to change required") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"ALTER USER $username SET PASSWORD CHANGE REQUIRED")

    // THEN
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should alter user password mode using SET AUTH syntax") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"ALTER USER $username SET AUTH 'native' { SET PASSWORD CHANGE NOT REQUIRED }")

    // THEN
    testUserLogin(username, password, AuthenticationResult.SUCCESS)
  }

  test("should alter user password and mode") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"ALTER USER $username SET PASSWORD '$newPassword' CHANGE NOT REQUIRED")

    // THEN
    testUserLogin(username, password, AuthenticationResult.FAILURE)
    testUserLogin(username, newPassword, AuthenticationResult.SUCCESS)
  }

  test("should alter user password as parameter and password mode") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"ALTER USER $username SET PASSWORD $$password CHANGE NOT REQUIRED", Map("password" -> newPassword))

    // THEN
    testUserLogin(username, password, AuthenticationResult.FAILURE)
    testUserLogin(username, newPassword, AuthenticationResult.SUCCESS)
  }

  test("should alter user password and mode using SET AUTH syntax") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(
      s"""ALTER USER $username
         |SET AUTH 'native' {
         |  SET PASSWORD '$newPassword'
         |  SET PASSWORD CHANGE NOT REQUIRED
         |}""".stripMargin
    )

    // THEN
    testUserLogin(username, password, AuthenticationResult.FAILURE)
    testUserLogin(username, newPassword, AuthenticationResult.SUCCESS)
  }

  test("should fail when altering a non-existing user") {
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute(s"ALTER USER $username SET PASSWORD '$password'")
      // THEN
    } should have message s"Failed to alter the specified user '$username': User does not exist."

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("ALTER USER $user SET PASSWORD 'bazPassword'", userAlexandraMap)
      // THEN
    } should have message s"Failed to alter the specified user '$username': User does not exist."
  }

  test("should alter user with encrypted password") {
    // GIVEN
    prepareUser()
    val encryptedPassword = getMaskedEncodedPassword(newPassword)

    // WHEN
    execute(s"ALTER USER $username SET ENCRYPTED PASSWORD '$encryptedPassword'")

    // THEN
    testUserLogin(username, password, AuthenticationResult.FAILURE)
    testUserLogin(username, encryptedPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, newPassword, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should alter user with old configuration encrypted password") {
    // GIVEN
    prepareUser()
    val version = "0"
    val encryptedPassword = getMaskedEncodedPassword(newPassword, version)

    // WHEN
    execute(s"ALTER USER $username SET ENCRYPTED PASSWORD '$encryptedPassword'")

    // THEN
    testUserLogin(username, password, AuthenticationResult.FAILURE)
    testUserLogin(username, encryptedPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, newPassword, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail to alter user with empty encrypted password") {
    // GIVEN
    prepareUser()

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute(s"ALTER USER $username SET ENCRYPTED PASSWORD ''")
      // THEN
    } should have message "Incorrect format of encrypted password. Correct format is '<encryption-version>,<hash>,<salt>'."

    // THEN
    testUserLogin(username, "", AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail to alter user with incorrectly encrypted password") {
    // GIVEN
    prepareUser()
    val incorrectlyEncryptedPassword = "0b1ca6d4016160782ab"

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute(s"ALTER USER $username SET ENCRYPTED PASSWORD '$incorrectlyEncryptedPassword'")
      // THEN
    } should have message "Incorrect format of encrypted password. Correct format is '<encryption-version>,<hash>,<salt>'."

    testUserLogin(username, incorrectlyEncryptedPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when alter user with set password shorter than limit (default 8)") {
    // GIVEN
    prepareUser()

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute(s"ALTER USER $username SET PASSWORD '1234567'")
      // THEN
    } should have message "A password must be at least 8 characters."

    testUserLogin(username, "1234567", AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should fail when alter user with set password parameter shorter than limit (default 8)") {
    // GIVEN
    prepareUser()

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute(s"ALTER USER $username SET PASSWORD $param", Map(paramName -> "1234567"))
      // THEN
    } should have message "A password must be at least 8 characters."

    testUserLogin(username, "1234567", AuthenticationResult.FAILURE)
    testUserLogin(username, password, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should alter user with encrypted password as parameter") {
    // GIVEN
    prepareUser()
    val encryptedPassword = getMaskedEncodedPassword(newPassword)

    // WHEN
    execute(
      s"ALTER USER $username SET ENCRYPTED PASSWORD $$password CHANGE REQUIRED",
      Map("password" -> encryptedPassword)
    )

    // THEN
    testUserLogin(username, password, AuthenticationResult.FAILURE)
    testUserLogin(username, encryptedPassword, AuthenticationResult.FAILURE)
    testUserLogin(username, newPassword, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should alter existing user using if exists") {
    // GIVEN
    prepareUser()

    // WHEN
    execute(s"ALTER USER $username IF EXISTS SET PASSWORD '$newPassword'")

    // THEN
    testUserLogin(username, newPassword, AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
    testUserLogin(username, password, AuthenticationResult.FAILURE)
  }

  test("should do nothing when altering non-existing user using if exists") {
    // WHEN
    execute("ALTER USER foo IF EXISTS SET PASSWORD CHANGE NOT REQUIRED")

    // THEN
    execute("SHOW USERS").toSet should be(Set(defaultUser))
  }

  test("should do nothing when altering an invalid (non-existing) user using if exists") {
    // WHEN
    execute("ALTER USER `:foo` IF EXISTS SET PASSWORD CHANGE REQUIRED")

    // THEN
    execute("SHOW USERS").toSet should be(Set(defaultUser))
  }

  test("should not be able to alter user status in community") {
    assertFailure(
      s"ALTER USER $defaultUsername SET STATUS ACTIVE",
      s"Failed to alter the specified user '$defaultUsername': 'SET STATUS' is not available in community edition."
    )
    assertFailure(
      s"ALTER USER $defaultUsername SET PASSWORD 'xxx' SET STATUS SUSPENDED",
      s"Failed to alter the specified user '$defaultUsername': 'SET STATUS' is not available in community edition."
    )
  }

  test("should not be able to alter a users home database in community") {
    // WHEN
    assertFailure(
      "ALTER USER foo SET HOME DATABASE foo",
      "Failed to alter the specified user 'foo': 'HOME DATABASE' is not available in community edition."
    )

    // WHEN
    assertFailure(
      "ALTER USER foo REMOVE HOME DATABASE",
      "Failed to alter the specified user 'foo': 'HOME DATABASE' is not available in community edition."
    )

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should not be able to alter external auths in community") {
    // WHEN
    assertFailure(
      "ALTER USER foo SET AUTH 'bar' { SET ID 'baz' }",
      "Failed to alter the specified user 'foo': `SET AUTH 'bar'` is not available in community edition."
    )

    // WHEN
    assertFailure(
      "ALTER USER foo SET AUTH 'bar' { SET ID 'baz' } SET AUTH 'baz' { SET ID 'qux' }",
      "Failed to alter the specified user 'foo': `SET AUTH 'bar'`, `SET AUTH 'baz'` are not available in community edition."
    )

    // WHEN
    assertFailure(
      "ALTER USER foo SET AUTH 'bar' { SET ID 'baz' } SET AUTH 'native' { SET PASSWORD 'password' }",
      "Failed to alter the specified user 'foo': `SET AUTH 'bar'` is not available in community edition."
    )

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should not be able to remove auths in community") {
    // WHEN
    assertFailure(
      "ALTER USER foo REMOVE AUTH 'bar'",
      "Failed to alter the specified user 'foo': `REMOVE AUTH` is not available in community edition."
    )
    // WHEN
    assertFailure(
      "ALTER USER foo REMOVE AUTH 'native'",
      "Failed to alter the specified user 'foo': `REMOVE AUTH` is not available in community edition."
    )

    // WHEN
    assertFailure(
      "ALTER USER foo REMOVE AUTH ['bar', 'baz']",
      "Failed to alter the specified user 'foo': `REMOVE AUTH` is not available in community edition."
    )

    // WHEN
    assertFailure(
      "ALTER USER foo REMOVE AUTH ['bar', 'native']",
      "Failed to alter the specified user 'foo': `REMOVE AUTH` is not available in community edition."
    )

    // WHEN
    assertFailure(
      "ALTER USER foo REMOVE ALL AUTH",
      "Failed to alter the specified user 'foo': `REMOVE AUTH` is not available in community edition."
    )

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  // Tests for changing own password

  test("should change own password") {
    // GIVEN
    prepareUser(changeRequired = false)

    // WHEN
    executeOnSystem(username, password, s"ALTER CURRENT USER SET PASSWORD FROM '$password' TO '$newPassword'")

    // THEN
    testUserLogin(username, newPassword, AuthenticationResult.SUCCESS)
    testUserLogin(username, password, AuthenticationResult.FAILURE)
  }

  test("should change own password when password change is required") {
    // GIVEN
    prepareUser(changeRequired = true)

    // WHEN
    executeOnSystem(username, password, s"ALTER CURRENT USER SET PASSWORD FROM '$password' TO '$newPassword'")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username, passwordChangeRequired = false))
    testUserLogin(username, newPassword, AuthenticationResult.SUCCESS)
    testUserLogin(username, password, AuthenticationResult.FAILURE)
  }

  test("should fail on changing own password from wrong password") {
    // GIVEN
    prepareUser(changeRequired = false)

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem(username, password, s"ALTER CURRENT USER SET PASSWORD FROM '$wrongPassword' TO '$newPassword'")
      // THEN
    } should have message s"User '$username' failed to alter their own password: Invalid principal or credentials."

    // THEN
    testUserLogin(username, password, AuthenticationResult.SUCCESS)
    testUserLogin(username, newPassword, AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password to invalid password") {
    // GIVEN
    prepareUser(changeRequired = false)

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem(username, password, s"ALTER CURRENT USER SET PASSWORD FROM '$password' TO ''")
      // THEN
    } should have message "A password cannot be empty."

    // THEN
    testUserLogin(username, password, AuthenticationResult.SUCCESS)

    the[QueryExecutionException] thrownBy {
      // WHEN
      val parameter = Map[String, Object]("password" -> password).asJava
      executeOnSystem(username, password, s"ALTER CURRENT USER SET PASSWORD FROM '$password' TO $$password", parameter)
      // THEN
    } should have message s"User '$username' failed to alter their own password: Old password and new password cannot be the same."

    // THEN
    testUserLogin(username, password, AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password to existing password and then succeed with a new password") {
    // GIVEN
    prepareUser(changeRequired = false)

    the[QueryExecutionException] thrownBy {
      // WHEN
      val parameter = Map[String, Object]("password" -> password).asJava
      executeOnSystem(username, password, s"ALTER CURRENT USER SET PASSWORD FROM '$password' TO $$password", parameter)
      // THEN
    } should have message s"User '$username' failed to alter their own password: Old password and new password cannot be the same."

    // THEN
    testUserLogin(username, password, AuthenticationResult.SUCCESS)

    // WHEN
    val parameter = Map[String, Object]("password" -> newPassword).asJava
    executeOnSystem(username, password, s"ALTER CURRENT USER SET PASSWORD FROM '$password' TO $$password", parameter)

    // THEN
    testUserLogin(username, newPassword, AuthenticationResult.SUCCESS)
  }

  test("should change own password to parameter") {
    // GIVEN
    prepareUser(changeRequired = false)
    val parameter = Map[String, Object]("password" -> newPassword).asJava

    // WHEN
    executeOnSystem(username, password, s"ALTER CURRENT USER SET PASSWORD FROM '$password' TO $$password", parameter)

    // THEN
    testUserLogin(username, newPassword, AuthenticationResult.SUCCESS)
    testUserLogin(username, password, AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password to missing parameter") {
    // GIVEN
    prepareUser(changeRequired = false)

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem(username, password, s"ALTER CURRENT USER SET PASSWORD FROM '$password' TO $$password")
      // THEN
    } should have message "Expected parameter(s): password"

    // THEN
    testUserLogin(username, password, AuthenticationResult.SUCCESS)
  }

  test("should change own password from parameter") {
    // GIVEN
    prepareUser(changeRequired = false)
    val parameter = Map[String, Object]("password" -> password).asJava

    // WHEN
    executeOnSystem(username, password, s"ALTER CURRENT USER SET PASSWORD FROM $$password TO '$newPassword'", parameter)

    // THEN
    testUserLogin(username, newPassword, AuthenticationResult.SUCCESS)
    testUserLogin(username, password, AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password from integer parameter") {
    // GIVEN
    execute(s"CREATE USER $username SET PASSWORD '12345678' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username, passwordChangeRequired = false))
    val parameter = Map[String, Object]("password" -> Integer.valueOf(12345678)).asJava

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem(
        username,
        "12345678",
        s"ALTER CURRENT USER SET PASSWORD FROM $$password TO '$newPassword'",
        parameter
      )
      // THEN
    } should have message "Expected password parameter $password to have type String but was Integer"

    // THEN
    testUserLogin(username, "12345678", AuthenticationResult.SUCCESS)
    testUserLogin(username, newPassword, AuthenticationResult.FAILURE)
  }

  test("should change own password from parameter to parameter") {
    // GIVEN
    prepareUser(changeRequired = false)
    val parameter = Map[String, Object]("currentPassword" -> password, "newPassword" -> newPassword).asJava

    // WHEN
    executeOnSystem(
      username,
      password,
      "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword",
      parameter
    )

    // THEN
    testUserLogin(username, newPassword, AuthenticationResult.SUCCESS)
    testUserLogin(username, password, AuthenticationResult.FAILURE)
  }

  test("should fail when changing own password from existing parameter to missing parameter") {
    // GIVEN
    prepareUser(changeRequired = false)
    val parameter = Map[String, Object]("currentPassword" -> password).asJava

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem(
        username,
        password,
        "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword",
        parameter
      )
      // THEN
    } should have message "Expected parameter(s): newPassword"

    // THEN
    testUserLogin(username, password, AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password from missing parameter to existing parameter") {
    // GIVEN
    prepareUser(changeRequired = false)
    val parameter = Map[String, Object]("newPassword" -> newPassword).asJava

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem(
        username,
        password,
        "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword",
        parameter
      )
      // THEN
    } should have message "Expected parameter(s): currentPassword"

    // THEN
    testUserLogin(username, password, AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password from parameter to parameter when both are missing") {
    // GIVEN
    prepareUser(changeRequired = false)

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOnSystem(username, password, "ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword")
      // THEN
    } should have message "Expected parameter(s): newPassword, currentPassword"

    // THEN
    testUserLogin(username, password, AuthenticationResult.SUCCESS)
  }

  test("should fail when changing own password when AUTH DISABLED") {
    the[IllegalStateException] thrownBy {
      // WHEN
      execute(s"ALTER CURRENT USER SET PASSWORD FROM '$password' TO '$newPassword'")
      // THEN
    } should have message "User failed to alter their own password: Command not available with auth disabled."
  }

  test("should fail when changing own password when not on system database") {
    // Can't use help method since we need to be logged in
    // GIVEN
    prepareUser(changeRequired = false)

    the[QueryExecutionException] thrownBy {
      // WHEN
      executeOn(
        DEFAULT_DATABASE_NAME,
        username,
        password,
        s"ALTER CURRENT USER SET PASSWORD FROM '$password' TO '$newPassword'"
      )
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: ALTER CURRENT USER SET PASSWORD"
  }

  // Run commands

  test("should not be able to run administration commands with password change required") {
    // GIVEN
    prepareUser(changeRequired = true)

    // WHEN
    val e = the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, "SHOW USERS")
    }

    // THEN
    e.getMessage should startWith("Permission denied.")
  }

  test("should not be able to run database queries with password change required") {
    // GIVEN
    prepareUser(changeRequired = true)

    // WHEN
    val e = the[AuthorizationViolationException] thrownBy {
      executeOn(DEFAULT_DATABASE_NAME, username, password, "MATCH (n) RETURN n")
    }

    // THEN
    e.getMessage should startWith("ACCESS on database 'neo4j' is not allowed.")
  }

  // Helper methods

  private def user(username: String, passwordChangeRequired: Boolean = true): Map[String, Any] =
    Map(
      "user" -> username,
      "roles" -> null,
      "passwordChangeRequired" -> passwordChangeRequired,
      "suspended" -> null,
      "home" -> null
    )

  private def addNativeAuthColumns(userMap: Map[String, Any], pwChangeRequired: Boolean): Map[String, Any] = {
    val authMap = Map[String, Any]("password" -> "***", "changeRequired" -> pwChangeRequired)
    userMap ++ Map("provider" -> NATIVE_AUTH, "auth" -> authMap)
  }

  private def testUserLogin(username: String, password: String, expected: AuthenticationResult): Unit = {
    val login = authManager.login(SecurityTestUtils.authToken(username, password), EMBEDDED_CONNECTION)
    val result = login.subject().getAuthenticationResult
    result should be(expected)
  }

  private def prepareUser(changeRequired: Boolean = true): Unit = {
    val changeRequiredString = if (changeRequired) "CHANGE REQUIRED" else "CHANGE NOT REQUIRED"
    execute(s"CREATE USER $username SET PASSWORD '$password' $changeRequiredString")
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username, changeRequired))
    testUserLogin(username, wrongPassword, AuthenticationResult.FAILURE)
    testUserLogin(
      username,
      password,
      if (changeRequired) AuthenticationResult.PASSWORD_CHANGE_REQUIRED else AuthenticationResult.SUCCESS
    )
  }

  private def getMaskedEncodedPassword(password: String): String = {
    val hasher = new SecureHasher()
    val credential = SystemGraphCredential.createCredentialForPassword(password.getBytes, hasher)
    SystemGraphCredential.maskSerialized(credential.serialize())
  }

  private def getMaskedEncodedPassword(password: String, version: String): String = {
    val hasher = new SecureHasher(version)
    val credential = SystemGraphCredential.createCredentialForPassword(password.getBytes, hasher)
    SystemGraphCredential.maskSerialized(credential.serialize())
  }

  private def executeOnSystem(
    username: String,
    password: String,
    query: String,
    params: util.Map[String, Object] = Collections.emptyMap(),
    resultHandler: (Result.ResultRow, Int) => Unit = (_, _) => {}
  ): Int = {
    executeOn(SYSTEM_DATABASE_NAME, username, password, query, params, resultHandler)
  }

  private def executeOn(
    database: String,
    username: String,
    password: String,
    query: String,
    params: util.Map[String, Object] = Collections.emptyMap(),
    resultHandler: (Result.ResultRow, Int) => Unit = (_, _) => {}
  ): Int = {
    selectDatabase(database)
    val login = authManager.login(SecurityTestUtils.authToken(username, password), EMBEDDED_CONNECTION)
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
