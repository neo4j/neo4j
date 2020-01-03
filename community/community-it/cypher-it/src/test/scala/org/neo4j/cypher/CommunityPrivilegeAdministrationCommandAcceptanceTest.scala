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

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME

class CommunityPrivilegeAdministrationCommandAcceptanceTest extends CommunityAdministrationCommandAcceptanceTestBase {

  // Tests for showing privileges

  test("should fail on showing privileges from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("SHOW ALL PRIVILEGES", "Unsupported administration command: SHOW ALL PRIVILEGES")
  }

  test("should fail on showing role privileges from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("SHOW ROLE reader PRIVILEGES", "Unsupported administration command: SHOW ROLE reader PRIVILEGES")
  }

  test("should fail on showing user privileges for non-existing user with correct error message") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("SHOW USER foo PRIVILEGES", "Unsupported administration command: SHOW USER foo PRIVILEGES")
  }

  // Tests for granting privileges

  test("should fail on granting traverse privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom", "Unsupported administration command: GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
  }

  test("should fail on granting read privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT READ {*} ON GRAPH * NODES * (*) TO custom", "Unsupported administration command: GRANT READ {*} ON GRAPH * NODES * (*) TO custom")
  }

  test("should fail on granting MATCH privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT MATCH {*} ON GRAPH * NODES * (*) TO custom", "Unsupported administration command: GRANT MATCH {*} ON GRAPH * NODES * (*) TO custom")
  }

  // Tests for denying privileges

  test("should fail on denying traverse privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY TRAVERSE ON GRAPH * NODES * (*) TO custom", "Unsupported administration command: DENY TRAVERSE ON GRAPH * NODES * (*) TO custom")
  }

  test("should fail on denying read privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY READ {*} ON GRAPH * NODES * (*) TO custom", "Unsupported administration command: DENY READ {*} ON GRAPH * NODES * (*) TO custom")
  }

  test("should fail on denying MATCH privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY MATCH {*} ON GRAPH * NODES * (*) TO custom", "Unsupported administration command: DENY MATCH {*} ON GRAPH * NODES * (*) TO custom")
  }

  // Tests for revoking grant privileges

  test("should fail on revoking grant traverse privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE GRANT TRAVERSE ON GRAPH * NODES * (*) FROM custom", "Unsupported administration command: REVOKE GRANT TRAVERSE ON GRAPH * NODES * (*) FROM custom")
  }

  test("should fail on revoking grant read privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE GRANT READ {*} ON GRAPH * NODES * (*) FROM custom", "Unsupported administration command: REVOKE GRANT READ {*} ON GRAPH * NODES * (*) FROM custom")
  }

  test("should fail on revoking grant MATCH privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailureWithPartialMessage("REVOKE GRANT MATCH {*} ON GRAPH * NODES * (*) FROM custom",
      "REVOKE GRANT MATCH is not a valid command, use REVOKE GRANT READ and REVOKE GRANT TRAVERSE instead.")
  }
  
  // Tests for revoking deny privileges
  test("should fail on revoking deny traverse privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE DENY TRAVERSE ON GRAPH * NODES * (*) FROM custom", "Unsupported administration command: REVOKE DENY TRAVERSE ON GRAPH * NODES * (*) FROM custom")
  }

  test("should fail on revoking deny read privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE DENY READ {*} ON GRAPH * NODES * (*) FROM custom", "Unsupported administration command: REVOKE DENY READ {*} ON GRAPH * NODES * (*) FROM custom")
  }

  test("should fail on revoking deny MATCH privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailureWithPartialMessage("REVOKE DENY MATCH {*} ON GRAPH * NODES * (*) FROM custom",
      "REVOKE DENY MATCH is not a valid command, use REVOKE DENY READ and REVOKE DENY TRAVERSE instead.")
  }

  // Tests for revoking privileges
  test("should fail on revoking traverse privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM custom", "Unsupported administration command: REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM custom")
  }

  test("should fail on revoking read privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE READ {*} ON GRAPH * NODES * (*) FROM custom", "Unsupported administration command: REVOKE READ {*} ON GRAPH * NODES * (*) FROM custom")
  }

  test("should fail on revoking MATCH privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailureWithPartialMessage("REVOKE MATCH {*} ON GRAPH * NODES * (*) FROM custom",
      "REVOKE MATCH is not a valid command, use REVOKE READ and REVOKE TRAVERSE instead.")
  }

  def assertFailureWithPartialMessage(command: String, errorMsg: String): Unit = {
    // WHEN
    val exception = the[Exception] thrownBy {
      execute(command)
    }

    // THEN
    exception.getMessage should include(errorMsg)
  }

}
