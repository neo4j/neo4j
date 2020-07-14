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
import org.neo4j.graphdb.config.Setting

class CommunityPrivilegeAdministrationCommandAcceptanceTest extends CommunityAdministrationCommandAcceptanceTestBase {

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(GraphDatabaseSettings.auth_enabled -> java.lang.Boolean.TRUE)

  // Tests for showing privileges

  test("should fail on showing privileges from community") {
    assertFailure("SHOW ALL PRIVILEGES", "Unsupported administration command: SHOW ALL PRIVILEGES")
  }

  test("should fail on showing role privileges from community") {
    assertFailure("SHOW ROLE reader PRIVILEGES", "Unsupported administration command: SHOW ROLE reader PRIVILEGES")
    assertFailure("SHOW ROLE $role PRIVILEGES", "Unsupported administration command: SHOW ROLE $role PRIVILEGES")
    assertFailure("SHOW ROLES role1, $role2 PRIVILEGES", "Unsupported administration command: SHOW ROLES role1, $role2 PRIVILEGES")
  }

  test("should fail on showing user privileges for non-existing user with correct error message") {
    assertFailure("SHOW USER foo PRIVILEGES", "Unsupported administration command: SHOW USER foo PRIVILEGES")
    assertFailure("SHOW USER $foo PRIVILEGES", "Unsupported administration command: SHOW USER $foo PRIVILEGES")
    assertFailure("SHOW USERS $foo, bar PRIVILEGES", "Unsupported administration command: SHOW USERS $foo, bar PRIVILEGES")
  }

  private val enterprisePrivileges = Seq(
    // Graph privileges
    "TRAVERSE ON GRAPH * NODES * (*)",
    "READ {*} ON GRAPH * NODES * (*)",
    "MATCH {*} ON DEFAULT GRAPH NODES * (*)",
    "WRITE ON GRAPH *",
    "SET LABEL foo ON GRAPH *",
    "REMOVE LABEL foo ON GRAPH *",
    "CREATE ON DEFAULT GRAPH NODE A",
    "DELETE ON GRAPH * RELATIONSHIP B",
    "SET PROPERTY {prop} ON GRAPH *",
    "ALL GRAPH PRIVILEGES ON GRAPH *",

    // Database privileges
    "ACCESS ON DATABASE *",
    "START ON DATABASE $foo",
    "STOP ON DATABASE *",
    "CREATE INDEX ON DATABASE *",
    "DROP INDEX ON DATABASE $foo",
    "INDEX MANAGEMENT ON DATABASE *",
    "CREATE CONSTRAINT ON DATABASE *",
    "DROP CONSTRAINT ON DATABASE *",
    "CONSTRAINT MANAGEMENT ON DATABASE $foo",
    "CREATE NEW NODE LABEL ON DATABASE *",
    "CREATE NEW RELATIONSHIP TYPE ON DATABASE $foo",
    "CREATE NEW PROPERTY NAME ON DATABASE *",
    "NAME MANAGEMENT ON DATABASE *",
    "ALL DATABASE PRIVILEGES ON DATABASE *",
    "SHOW TRANSACTION (*) ON DATABASE *",
    "TERMINATE TRANSACTION ($user) ON DATABASE $foo",
    "TRANSACTION MANAGEMENT ON DATABASE *",

    // Dbms privileges
    "ROLE MANAGEMENT ON DBMS",
    "CREATE ROLE ON DBMS",
    "DROP ROLE ON DBMS",
    "ASSIGN ROLE ON DBMS",
    "REMOVE ROLE ON DBMS",
    "SHOW ROLE ON DBMS",
    "USER MANAGEMENT ON DBMS",
    "CREATE USER ON DBMS",
    "DROP USER ON DBMS",
    "SET USER STATUS ON DBMS",
    "SET PASSWORDS ON DBMS",
    "ALTER USER ON DBMS",
    "SHOW USER ON DBMS",
    "DATABASE MANAGEMENT ON DBMS",
    "CREATE DATABASE ON DBMS",
    "DROP DATABASE ON DBMS",
    "PRIVILEGE MANAGEMENT ON DBMS",
    "SHOW PRIVILEGE ON DBMS",
    "ASSIGN PRIVILEGE ON DBMS",
    "REMOVE PRIVILEGE ON DBMS",
    "ALL ON DBMS",
    "ALL PRIVILEGES ON DBMS",
    "ALL DBMS PRIVILEGES ON DBMS"
  )

  private val grantPrivilegeTypes = Seq(
    ("GRANT", "TO"),
    ("REVOKE", "FROM"),
    ("REVOKE GRANT", "FROM"),

  )

  private val denyPrivilegeTypes = Seq(
    ("DENY", "TO"),
    ("REVOKE DENY", "FROM")
  )

  enterprisePrivileges.foreach {
    privilege =>
      (grantPrivilegeTypes ++ denyPrivilegeTypes).foreach {
        case (privilegeType, preposition) =>
          test(s"should fail on $privilegeType $privilege from community") {
            val command = s"$privilegeType $privilege $preposition custom"
            assertFailure(command, s"Unsupported administration command: $command")
          }
      }
  }

  (grantPrivilegeTypes).foreach {
    case (privilegeType, preposition) =>
      test(s"should fail on $privilegeType MERGE {*} ON GRAPH * from community") {
        val command = s"$privilegeType MERGE {*} ON GRAPH * $preposition custom"
        assertFailure(command, s"Unsupported administration command: $command")
      }
  }

}
