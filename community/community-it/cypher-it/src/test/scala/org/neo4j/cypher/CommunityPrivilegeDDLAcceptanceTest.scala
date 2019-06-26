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

class CommunityPrivilegeDDLAcceptanceTest extends CommunityDDLAcceptanceTestBase {

  // Tests for showing privileges

  test("should fail on showing privileges from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("SHOW ALL PRIVILEGES", "Unsupported management command: SHOW ALL PRIVILEGES")
  }

  test("should fail on showing role privileges from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("SHOW ROLE reader PRIVILEGES", "Unsupported management command: SHOW ROLE reader PRIVILEGES")
  }

  test("should fail on showing user privileges for non-existing user with correct error message") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("SHOW USER foo PRIVILEGES", "Unsupported management command: SHOW USER foo PRIVILEGES")
  }

  // Tests for granting privileges

  test("should fail on granting traverse privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom", "Unsupported management command: GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
  }

  test("should fail on granting read privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT READ (*) ON GRAPH * NODES * (*) TO custom", "Unsupported management command: GRANT READ (*) ON GRAPH * NODES * (*) TO custom")
  }

  test("should fail on granting MATCH privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT MATCH (*) ON GRAPH * NODES * (*) TO custom", "Unsupported management command: GRANT MATCH (*) ON GRAPH * NODES * (*) TO custom")
  }

  test("should fail on granting write privileges from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT WRITE (*) ON GRAPH * NODES * (*) TO custom", "Unsupported management command: GRANT WRITE (*) ON GRAPH * NODES * (*) TO custom")
  }

  // Tests for revoking privileges

  test("should fail on revoking traverse privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM custom", "Unsupported management command: REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM custom")
  }

  test("should fail on revoking read privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE READ (*) ON GRAPH * NODES * (*) FROM custom", "Unsupported management command: REVOKE READ (*) ON GRAPH * NODES * (*) FROM custom")
  }

  test("should fail on revoking MATCH privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE MATCH (*) ON GRAPH * NODES * (*) FROM custom", "Unsupported management command: REVOKE MATCH (*) ON GRAPH * NODES * (*) FROM custom")
  }

  test("should fail on revoking write privileges from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE WRITE (*) ON GRAPH * NODES * (*) FROM custom", "Unsupported management command: REVOKE WRITE (*) ON GRAPH * NODES * (*) FROM custom")
  }

  // Tests for granting and revoking roles to users

  test("should fail on granting role to user from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT ROLE reader TO neo4j", "Unsupported management command: GRANT ROLE reader TO neo4j")
  }

  test("should fail on revoking non-existing role to user with correct error message") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE ROLE custom FROM neo4j", "Unsupported management command: REVOKE ROLE custom FROM neo4j")
  }
}
