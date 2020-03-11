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

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.graphdb.config.Setting

class CommunityPrivilegeAdministrationCommandAcceptanceTest extends CommunityAdministrationCommandAcceptanceTestBase {

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(GraphDatabaseSettings.auth_enabled -> java.lang.Boolean.TRUE)

  private val enterpriseCommands = Seq(
    ("TRAVERSE", "TRAVERSE ON GRAPH * NODES * (*)"),
    ("READ", "READ {*} ON GRAPH * NODES * (*)"),
    ("MATCH", "MATCH {*} ON GRAPH * NODES * (*)"),
    ("ROLE MANAGEMENT", "ROLE MANAGEMENT ON DBMS"),
    ("CREATE ROLE", "CREATE ROLE ON DBMS"),
    ("DROP ROLE", "DROP ROLE ON DBMS"),
    ("ASSIGN ROLE", "ASSIGN ROLE ON DBMS"),
    ("REMOVE ROLE", "REMOVE ROLE ON DBMS"),
    ("SHOW ROLE", "SHOW ROLE ON DBMS"),
    ("USER MANAGEMENT", "USER MANAGEMENT ON DBMS"),
    ("CREATE USER", "CREATE USER ON DBMS"),
    ("DROP USER", "DROP USER ON DBMS"),
    ("ALTER USER", "ALTER USER ON DBMS"),
    ("SHOW USER", "SHOW USER ON DBMS"),
    ("DATABASE MANAGEMENT", "DATABASE MANAGEMENT ON DBMS"),
    ("CREATE DATABASE", "CREATE DATABASE ON DBMS"),
    ("DROP DATABASE", "DROP DATABASE ON DBMS"),
    ("PRIVILEGE MANAGEMENT", "PRIVILEGE MANAGEMENT ON DBMS"),
    ("SHOW PRIVILEGE", "SHOW PRIVILEGE ON DBMS"),
    ("ASSIGN PRIVILEGE", "ASSIGN PRIVILEGE ON DBMS"),
    ("REMOVE PRIVILEGE", "REMOVE PRIVILEGE ON DBMS")
  )

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
  enterpriseCommands.foreach {
    case (privilege, command) =>
      test(s"should fail on granting $privilege privilege from community") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)

        // THEN
        assertFailure(s"GRANT $command TO custom", s"Unsupported administration command: GRANT $command TO custom")
      }
  }

  // Tests for denying privileges
  enterpriseCommands.foreach {
    case (privilege, command) =>
      test(s"should fail on denying $privilege privilege from community") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)

        // THEN
        assertFailure(s"DENY $command TO custom", s"Unsupported administration command: DENY $command TO custom")
      }
  }

  // Tests for revoking grant privileges
  enterpriseCommands.foreach {
    case (privilege, command) =>
      test(s"should fail on revoking grant $privilege privilege from community") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)

        // THEN
        assertFailure(s"REVOKE GRANT $command FROM custom", s"Unsupported administration command: REVOKE GRANT $command FROM custom")
      }
  }

  // Tests for revoking deny privileges
  enterpriseCommands.foreach {
    case (privilege, command) =>
      test(s"should fail on revoking deny $privilege privilege from community") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)

        // THEN
        assertFailure(s"REVOKE DENY $command FROM custom", s"Unsupported administration command: REVOKE DENY $command FROM custom")
      }
  }

  // Tests for revoking privileges
  enterpriseCommands.foreach {
    case (privilege, command) =>
      test(s"should fail on revoking $privilege privilege from community") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)

        // THEN
        assertFailure(s"REVOKE $command FROM custom", s"Unsupported administration command: REVOKE $command FROM custom")
      }
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
