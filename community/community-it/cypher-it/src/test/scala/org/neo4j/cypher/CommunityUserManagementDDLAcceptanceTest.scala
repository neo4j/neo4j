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

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME

import scala.collection.Map

class CommunityUserManagementDDLAcceptanceTest extends CommunityDDLAcceptanceTestBase {
  // Tests for showing users

  test("should show default user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW USERS")

    // THEN
    result.toList should be(List(Map("user" -> "neo4j")))
  }

  test("should fail when showing users when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("SHOW USERS")
      // THEN
    } should have message "Trying to run `SHOW USERS` against non-system database."
  }

  test("should fail on creating user from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("CREATE USER foo SET PASSWORD 'xxx'", "Unsupported management command: CREATE USER foo SET PASSWORD 'xxx'")
  }

  test("should fail on creating already existing user with correct error message") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("CREATE USER neo4j SET PASSWORD 'xxx'", "Unsupported management command: CREATE USER neo4j SET PASSWORD 'xxx'")
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

}
