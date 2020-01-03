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

class CommunityDatabasePrivilegeAdministrationCommandAcceptanceTest extends CommunityAdministrationCommandAcceptanceTestBase {

  // Access, Start and Stop privilege

  test("should fail on granting access privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT ACCESS ON DATABASE * TO custom", "Unsupported administration command: GRANT ACCESS ON DATABASE * TO custom")
  }

  test("should fail on denying access privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY ACCESS ON DATABASE * TO custom", "Unsupported administration command: DENY ACCESS ON DATABASE * TO custom")
  }

  test("should fail on revoking access privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE ACCESS ON DATABASE * FROM custom", "Unsupported administration command: REVOKE ACCESS ON DATABASE * FROM custom")
  }

  test("should fail on granting start privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT START ON DATABASE * TO custom", "Unsupported administration command: GRANT START ON DATABASE * TO custom")
  }

  test("should fail on denying start privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY START ON DATABASE * TO custom", "Unsupported administration command: DENY START ON DATABASE * TO custom")
  }

  test("should fail on revoking start privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE START ON DATABASE * FROM custom", "Unsupported administration command: REVOKE START ON DATABASE * FROM custom")
  }

  test("should fail on granting stop privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT STOP ON DATABASE * TO custom", "Unsupported administration command: GRANT STOP ON DATABASE * TO custom")
  }

  test("should fail on denying stop privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY STOP ON DATABASE * TO custom", "Unsupported administration command: DENY STOP ON DATABASE * TO custom")
  }

  test("should fail on revoking stop privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE STOP ON DATABASE * FROM custom", "Unsupported administration command: REVOKE STOP ON DATABASE * FROM custom")
  }

  // Index privileges

  test("should fail on granting create index privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT CREATE INDEX ON DATABASE * TO custom", "Unsupported administration command: GRANT CREATE INDEX ON DATABASE * TO custom")
  }

  test("should fail on denying create index privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY CREATE INDEX ON DATABASE * TO custom", "Unsupported administration command: DENY CREATE INDEX ON DATABASE * TO custom")
  }

  test("should fail on revoking create index privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE CREATE INDEX ON DATABASE * FROM custom", "Unsupported administration command: REVOKE CREATE INDEX ON DATABASE * FROM custom")
  }

  test("should fail on granting drop index privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT DROP INDEX ON DATABASE * TO custom", "Unsupported administration command: GRANT DROP INDEX ON DATABASE * TO custom")
  }

  test("should fail on denying drop index privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY DROP INDEX ON DATABASE * TO custom", "Unsupported administration command: DENY DROP INDEX ON DATABASE * TO custom")
  }

  test("should fail on revoking drop index privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE DROP INDEX ON DATABASE * FROM custom", "Unsupported administration command: REVOKE DROP INDEX ON DATABASE * FROM custom")
  }

  test("should fail on granting index management privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT INDEX MANAGEMENT ON DATABASE * TO custom", "Unsupported administration command: GRANT INDEX MANAGEMENT ON DATABASE * TO custom")
  }

  test("should fail on denying index management privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY INDEX MANAGEMENT ON DATABASE * TO custom", "Unsupported administration command: DENY INDEX MANAGEMENT ON DATABASE * TO custom")
  }

  test("should fail on revoking index management privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE INDEX MANAGEMENT ON DATABASE * FROM custom", "Unsupported administration command: REVOKE INDEX MANAGEMENT ON DATABASE * FROM custom")
  }

  // Constraint privileges

  test("should fail on granting create constraint privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT CREATE CONSTRAINT ON DATABASE * TO custom", "Unsupported administration command: GRANT CREATE CONSTRAINT ON DATABASE * TO custom")
  }

  test("should fail on denying create constraint privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY CREATE CONSTRAINT ON DATABASE * TO custom", "Unsupported administration command: DENY CREATE CONSTRAINT ON DATABASE * TO custom")
  }

  test("should fail on revoking create constraint privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE CREATE CONSTRAINT ON DATABASE * FROM custom", "Unsupported administration command: REVOKE CREATE CONSTRAINT ON DATABASE * FROM custom")
  }

  test("should fail on granting drop constraint privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT DROP CONSTRAINT ON DATABASE * TO custom", "Unsupported administration command: GRANT DROP CONSTRAINT ON DATABASE * TO custom")
  }

  test("should fail on denying drop constraint privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY DROP CONSTRAINT ON DATABASE * TO custom", "Unsupported administration command: DENY DROP CONSTRAINT ON DATABASE * TO custom")
  }

  test("should fail on revoking drop constraint privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE DROP CONSTRAINT ON DATABASE * FROM custom", "Unsupported administration command: REVOKE DROP CONSTRAINT ON DATABASE * FROM custom")
  }

  test("should fail on granting constraint management privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT CONSTRAINT MANAGEMENT ON DATABASE * TO custom", "Unsupported administration command: GRANT CONSTRAINT MANAGEMENT ON DATABASE * TO custom")
  }

  test("should fail on denying constraint management privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY CONSTRAINT MANAGEMENT ON DATABASE * TO custom", "Unsupported administration command: DENY CONSTRAINT MANAGEMENT ON DATABASE * TO custom")
  }

  test("should fail on revoking constraint management privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE CONSTRAINT MANAGEMENT ON DATABASE * FROM custom", "Unsupported administration command: REVOKE CONSTRAINT MANAGEMENT ON DATABASE * FROM custom")
  }

  // Token privileges

  test("should fail on granting create label privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT CREATE NEW LABEL ON DATABASE * TO custom", "Unsupported administration command: GRANT CREATE NEW LABEL ON DATABASE * TO custom")
  }

  test("should fail on denying create label privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY CREATE NEW LABEL ON DATABASE * TO custom", "Unsupported administration command: DENY CREATE NEW LABEL ON DATABASE * TO custom")
  }

  test("should fail on revoking create label privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE CREATE NEW LABEL ON DATABASE * FROM custom", "Unsupported administration command: REVOKE CREATE NEW LABEL ON DATABASE * FROM custom")
  }

  test("should fail on granting create reltype privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT CREATE NEW TYPE ON DATABASE * TO custom", "Unsupported administration command: GRANT CREATE NEW TYPE ON DATABASE * TO custom")
  }

  test("should fail on denying create reltype privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY CREATE NEW TYPE ON DATABASE * TO custom", "Unsupported administration command: DENY CREATE NEW TYPE ON DATABASE * TO custom")
  }

  test("should fail on revoking create reltype privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE CREATE NEW TYPE ON DATABASE * FROM custom", "Unsupported administration command: REVOKE CREATE NEW TYPE ON DATABASE * FROM custom")
  }

  test("should fail on granting property name privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT CREATE NEW NAME ON DATABASE * TO custom", "Unsupported administration command: GRANT CREATE NEW NAME ON DATABASE * TO custom")
  }

  test("should fail on denying property name privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY CREATE NEW NAME ON DATABASE * TO custom", "Unsupported administration command: DENY CREATE NEW NAME ON DATABASE * TO custom")
  }

  test("should fail on revoking property name privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE CREATE NEW NAME ON DATABASE * FROM custom", "Unsupported administration command: REVOKE CREATE NEW NAME ON DATABASE * FROM custom")
  }

  test("should fail on granting name management privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT NAME MANAGEMENT ON DATABASE * TO custom", "Unsupported administration command: GRANT NAME MANAGEMENT ON DATABASE * TO custom")
  }

  test("should fail on denying name management privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY NAME MANAGEMENT ON DATABASE * TO custom", "Unsupported administration command: DENY NAME MANAGEMENT ON DATABASE * TO custom")
  }

  test("should fail on revoking name management privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE NAME MANAGEMENT ON DATABASE * FROM custom", "Unsupported administration command: REVOKE NAME MANAGEMENT ON DATABASE * FROM custom")
  }

  // All database privileges

  test("should fail on granting all database privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("GRANT ALL ON DATABASE * TO custom", "Unsupported administration command: GRANT ALL ON DATABASE * TO custom")
  }

  test("should fail on denying all database privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("DENY ALL ON DATABASE * TO custom", "Unsupported administration command: DENY ALL ON DATABASE * TO custom")
  }

  test("should fail on revoking all database privilege from community") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // THEN
    assertFailure("REVOKE ALL ON DATABASE * FROM custom", "Unsupported administration command: REVOKE ALL ON DATABASE * FROM custom")
  }
}
