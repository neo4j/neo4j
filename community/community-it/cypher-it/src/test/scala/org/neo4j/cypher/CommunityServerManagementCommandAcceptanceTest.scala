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

class CommunityServerManagementCommandAcceptanceTest extends CommunityAdministrationCommandAcceptanceTestBase {

  override def databaseConfig(): Map[Setting[_], Object] =
    super.databaseConfig() ++ Map(auth_enabled -> java.lang.Boolean.TRUE)

  test("should fail on enabling server from community") {
    assertFailure("ENABLE SERVER $name", "Unsupported administration command: ENABLE SERVER $name")
    assertFailure(
      "ENABLE SERVER 'name' OPTIONS {}",
      "Unsupported administration command: ENABLE SERVER 'name' OPTIONS {}"
    )
  }

  test("should fail on altering server from community") {
    assertFailure(
      "ALTER SERVER 'name' SET OPTIONS $options",
      "Unsupported administration command: ALTER SERVER 'name' SET OPTIONS $options"
    )
    assertFailure(
      "ALTER SERVER $name SET OPTIONS {badger: 'snake'}",
      "Unsupported administration command: ALTER SERVER $name SET OPTIONS {badger: 'snake'}"
    )
  }

  test("should fail on renaming server from community") {
    assertFailure("RENAME SERVER 'name' TO $name", "Unsupported administration command: RENAME SERVER 'name' TO $name")
    assertFailure(
      "RENAME SERVER $name TO 'badger'",
      "Unsupported administration command: RENAME SERVER $name TO 'badger'"
    )
  }

  test("should fail on reallocating databases from community") {
    assertFailure("REALLOCATE DATABASES", "Unsupported administration command: REALLOCATE DATABASES")
  }

  test("should fail on dryrun reallocating databases from community") {
    assertFailure("DRYRUN REALLOCATE DATABASES", "Unsupported administration command: DRYRUN REALLOCATE DATABASES")
  }

  test("should fail on deallocating server from community") {
    assertFailure(
      "DEALLOCATE DATABASES FROM SERVER 'name'",
      "Unsupported administration command: DEALLOCATE DATABASES FROM SERVER 'name'"
    )
    assertFailure(
      "DEALLOCATE DATABASES FROM SERVERS 'name', $badger",
      "Unsupported administration command: DEALLOCATE DATABASES FROM SERVERS 'name', $badger"
    )
  }

  test("should fail on dryrun deallocating server from community") {
    assertFailure(
      "DRYRUN DEALLOCATE DATABASES FROM SERVER 'name'",
      "Unsupported administration command: DRYRUN DEALLOCATE DATABASES FROM SERVER 'name'"
    )
    assertFailure(
      "DRYRUN DEALLOCATE DATABASES FROM SERVERS 'name', $badger",
      "Unsupported administration command: DRYRUN DEALLOCATE DATABASES FROM SERVERS 'name', $badger"
    )
  }

  test("should fail on drop server from community") {
    assertFailure("DROP SERVER 'name'", "Unsupported administration command: DROP SERVER 'name'")
    assertFailure("DROP SERVER $badger", "Unsupported administration command: DROP SERVER $badger")
  }

  test("should fail on show server from community") {
    assertFailure("SHOW SERVERS", "Unsupported administration command: SHOW SERVERS")
  }
}
