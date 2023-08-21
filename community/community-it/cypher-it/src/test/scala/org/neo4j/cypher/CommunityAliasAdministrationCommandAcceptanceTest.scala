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

class CommunityAliasAdministrationCommandAcceptanceTest extends CommunityAdministrationCommandAcceptanceTestBase {

  test("should fail on creating local alias from community") {
    assertFailure(
      "CREATE ALIAS alias FOR DATABASE foo",
      "Unsupported administration command: CREATE ALIAS alias FOR DATABASE foo"
    )
  }

  test("should fail on creating local alias (in composite) from community") {
    assertFailure(
      "CREATE ALIAS composite.alias FOR DATABASE foo",
      "Unsupported administration command: CREATE ALIAS composite.alias FOR DATABASE foo"
    )
  }

  test("should fail on creating remote alias from community") {
    assertFailure(
      "CREATE ALIAS alias FOR DATABASE foo AT 'url' USER user PASSWORD 'password'",
      "Unsupported administration command: CREATE ALIAS alias FOR DATABASE foo AT 'url' USER user PASSWORD 'password'"
    )
  }

  test("should fail on creating remote alias (in composite) from community") {
    assertFailure(
      "CREATE ALIAS composite.alias FOR DATABASE foo AT 'url' USER user PASSWORD 'password'",
      "Unsupported administration command: CREATE ALIAS composite.alias FOR DATABASE foo AT 'url' USER user PASSWORD 'password'"
    )
  }

  test("should fail on altering local alias from community") {
    assertFailure(
      "ALTER ALIAS alias SET DATABASE TARGET foo",
      "Unsupported administration command: ALTER ALIAS alias SET DATABASE TARGET foo"
    )
  }

  test("should fail on altering local alias (in composite) from community") {
    assertFailure(
      "ALTER ALIAS composite.alias SET DATABASE TARGET foo",
      "Unsupported administration command: ALTER ALIAS composite.alias SET DATABASE TARGET foo"
    )
  }

  test("should fail on altering remote alias from community") {
    assertFailure(
      "ALTER ALIAS alias SET DATABASE TARGET foo AT 'url' USER user PASSWORD 'password'",
      "Unsupported administration command: ALTER ALIAS alias SET DATABASE TARGET foo AT 'url' USER user PASSWORD 'password'"
    )
  }

  test("should fail on altering remote alias (in composite) from community") {
    assertFailure(
      "ALTER ALIAS composite.alias SET DATABASE TARGET foo AT 'url' USER user PASSWORD 'password'",
      "Unsupported administration command: ALTER ALIAS composite.alias SET DATABASE TARGET foo AT 'url' USER user PASSWORD 'password'"
    )
  }

  test("should fail on drop alias from community") {
    assertFailure("DROP ALIAS alias FOR DATABASE", "Unsupported administration command: DROP ALIAS alias FOR DATABASE")
  }

  test("should fail on drop alias (in composite) from community") {
    assertFailure(
      "DROP ALIAS composite.alias FOR DATABASE",
      "Unsupported administration command: DROP ALIAS composite.alias FOR DATABASE"
    )
  }

  test("should fail on show aliases from community") {
    assertFailure("SHOW ALIASES FOR DATABASE", "Unsupported administration command: SHOW ALIASES FOR DATABASE")
  }

  test("should fail on show alias by name from community") {
    assertFailure(
      "SHOW ALIAS alias FOR DATABASES",
      "Unsupported administration command: SHOW ALIAS alias FOR DATABASES"
    )
  }

  test("should fail on show alias by name (in composite) from community") {
    assertFailure(
      "SHOW ALIAS composite.alias FOR DATABASES",
      "Unsupported administration command: SHOW ALIAS composite.alias FOR DATABASES"
    )
  }
}
