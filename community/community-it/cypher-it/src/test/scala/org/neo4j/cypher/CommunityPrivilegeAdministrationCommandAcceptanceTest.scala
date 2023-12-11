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

import org.neo4j.configuration.GraphDatabaseSettings.auth_enabled
import org.neo4j.graphdb.config.Setting

class CommunityPrivilegeAdministrationCommandAcceptanceTest extends CommunityAdministrationCommandAcceptanceTestBase {

  override def databaseConfig(): Map[Setting[_], Object] =
    super.databaseConfig() ++ Map(auth_enabled -> java.lang.Boolean.TRUE)

  test("should fail on showing all privileges from community") {
    assertFailure("SHOW ALL PRIVILEGES", "Unsupported administration command: SHOW ALL PRIVILEGES")
  }

  test("should fail on showing role privileges from community") {
    assertFailure("SHOW ROLE reader PRIVILEGES", "Unsupported administration command: SHOW ROLE reader PRIVILEGES")
    assertFailure("SHOW ROLE $role PRIVILEGES", "Unsupported administration command: SHOW ROLE $role PRIVILEGES")
    assertFailure(
      "SHOW ROLES role1, $role2 PRIVILEGES",
      "Unsupported administration command: SHOW ROLES role1, $role2 PRIVILEGES"
    )
  }

  test("should fail on showing user privileges from community") {
    assertFailure("SHOW USER foo PRIVILEGES", "Unsupported administration command: SHOW USER foo PRIVILEGES")
    assertFailure("SHOW USER $foo PRIVILEGES", "Unsupported administration command: SHOW USER $foo PRIVILEGES")
    assertFailure(
      "SHOW USERS $foo, bar PRIVILEGES",
      "Unsupported administration command: SHOW USERS $foo, bar PRIVILEGES"
    )
  }

  test("should fail on showing all privileges as (revoke) commands from community") {
    assertFailure(
      "SHOW ALL PRIVILEGES AS COMMANDS",
      "Unsupported administration command: SHOW ALL PRIVILEGES AS COMMANDS"
    )
    assertFailure(
      "SHOW ALL PRIVILEGES AS REVOKE COMMAND",
      "Unsupported administration command: SHOW ALL PRIVILEGES AS REVOKE COMMAND"
    )
  }

  test("should fail on showing role privileges as (revoke) commands from community") {
    assertFailure(
      "SHOW ROLE reader PRIVILEGES AS COMMANDS",
      "Unsupported administration command: SHOW ROLE reader PRIVILEGES AS COMMANDS"
    )
    assertFailure(
      "SHOW ROLE $role PRIVILEGES AS REVOKE COMMANDS",
      "Unsupported administration command: SHOW ROLE $role PRIVILEGES AS REVOKE COMMANDS"
    )
    assertFailure(
      "SHOW ROLES role1, $role2 PRIVILEGES AS COMMAND",
      "Unsupported administration command: SHOW ROLES role1, $role2 PRIVILEGES AS COMMAND"
    )
  }

  test("should fail on showing user privileges as (revoke) commands from community") {
    assertFailure(
      "SHOW USER foo PRIVILEGES AS COMMAND",
      "Unsupported administration command: SHOW USER foo PRIVILEGES AS COMMAND"
    )
    assertFailure(
      "SHOW USER $foo PRIVILEGES AS REVOKE COMMANDS",
      "Unsupported administration command: SHOW USER $foo PRIVILEGES AS REVOKE COMMANDS"
    )
    assertFailure(
      "SHOW USERS $foo, bar PRIVILEGES AS REVOKE COMMANDS",
      "Unsupported administration command: SHOW USERS $foo, bar PRIVILEGES AS REVOKE COMMANDS"
    )
  }

  private val privilegeTypes = Seq(
    ("GRANT", "TO"),
    ("REVOKE", "FROM"),
    ("REVOKE GRANT", "FROM"),
    ("DENY", "TO"),
    ("REVOKE DENY", "FROM")
  )

  privilegeTypes.foreach {
    case (privilegeType, preposition) =>
      test(s"should fail on $privilegeType graph privilege from community") {
        val command = s"$privilegeType TRAVERSE ON GRAPH * NODES * (*) $preposition custom"
        assertFailure(command, s"Unsupported administration command: $command")
      }

      test(s"should fail on $privilegeType database privilege from community") {
        val command = s"$privilegeType ACCESS ON HOME DATABASE $preposition custom"
        assertFailure(command, s"Unsupported administration command: $command")
      }

      test(s"should fail on $privilegeType dbms privilege from community") {
        val command = s"$privilegeType ROLE MANAGEMENT ON DBMS $preposition custom"
        assertFailure(command, s"Unsupported administration command: $command")
      }

      test(s"should fail on $privilegeType load privilege from community") {
        val command = s"$privilegeType LOAD ON ALL DATA $preposition custom"
        assertFailure(command, s"Unsupported administration command: $command")
      }
  }
}
