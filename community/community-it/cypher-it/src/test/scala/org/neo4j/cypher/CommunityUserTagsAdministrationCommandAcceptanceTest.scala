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

import org.neo4j.configuration.GraphDatabaseSettings.CypherVersion
import org.neo4j.configuration.GraphDatabaseSettings.default_language
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.graphdb.config.Setting

// Tags syntax is Cypher 25-only. Without the language override the command fails at the
// semantic layer (wrong error), so we need a standalone class rather than adding to
// CommunityUserAdministrationCommandAcceptanceTest.
class CommunityUserTagsAdministrationCommandAcceptanceTest extends CommunityAdministrationCommandAcceptanceTestBase {

  override def databaseConfig(): Map[Setting[?], Object] =
    super.databaseConfig() ++ Map(
      default_language -> CypherVersion.Cypher25
    )

  private def defaultUser: Map[String, Any] = Map(
    "user" -> "neo4j",
    "roles" -> null,
    "passwordChangeRequired" -> true,
    "suspended" -> null,
    "home" -> null
  )

  test("should not be able to alter user with tags in community") {
    assertFailureWithGQLStatus(
      "ALTER USER foo ADD TAGS 'engineering'",
      "Failed to alter the specified user 'foo': 'SET TAGS', 'ADD TAGS', 'REMOVE TAGS', 'REMOVE ALL TAGS' are not available in community edition.",
      GqlStatusInfoCodes.STATUS_51N27,
      "error: system configuration or operation exception - not supported in this edition. 'SET/ADD/REMOVE TAGS' is not supported in community edition."
    )
    assertFailureWithGQLStatus(
      "ALTER USER foo SET TAGS 'engineering'",
      "Failed to alter the specified user 'foo': 'SET TAGS', 'ADD TAGS', 'REMOVE TAGS', 'REMOVE ALL TAGS' are not available in community edition.",
      GqlStatusInfoCodes.STATUS_51N27,
      "error: system configuration or operation exception - not supported in this edition. 'SET/ADD/REMOVE TAGS' is not supported in community edition."
    )
    assertFailureWithGQLStatus(
      "ALTER USER foo REMOVE TAGS 'engineering'",
      "Failed to alter the specified user 'foo': 'SET TAGS', 'ADD TAGS', 'REMOVE TAGS', 'REMOVE ALL TAGS' are not available in community edition.",
      GqlStatusInfoCodes.STATUS_51N27,
      "error: system configuration or operation exception - not supported in this edition. 'SET/ADD/REMOVE TAGS' is not supported in community edition."
    )
    assertFailureWithGQLStatus(
      "ALTER USER foo REMOVE ALL TAGS",
      "Failed to alter the specified user 'foo': 'SET TAGS', 'ADD TAGS', 'REMOVE TAGS', 'REMOVE ALL TAGS' are not available in community edition.",
      GqlStatusInfoCodes.STATUS_51N27,
      "error: system configuration or operation exception - not supported in this edition. 'SET/ADD/REMOVE TAGS' is not supported in community edition."
    )
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should not be able to create user with SET TAGS in community") {
    assertFailureWithGQLStatus(
      "CREATE USER foo SET PASSWORD 'password' SET TAGS 'engineering'",
      "Failed to create the specified user 'foo': 'SET TAGS' is not available in community edition.",
      GqlStatusInfoCodes.STATUS_51N27,
      "error: system configuration or operation exception - not supported in this edition. 'SET TAGS' is not supported in community edition."
    )
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should not be able to alter users with tags in community") {
    assertFailureWithGQLStatus(
      "ALTER USERS neo4j ADD TAGS 'engineering'",
      "51N27: 'ALTER USERS' is not supported in community edition.",
      GqlStatusInfoCodes.STATUS_51N27,
      "error: system configuration or operation exception - not supported in this edition. 'ALTER USERS' is not supported in community edition."
    )
  }
}
