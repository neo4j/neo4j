/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AdditiveProjection
import org.neo4j.cypher.internal.ast.DefaultYield
import org.neo4j.cypher.internal.ast.ReadAdministrationCommand
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.ShowAllPrivileges
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.YieldAddedInRewrite
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.ExpandShowWhere
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ExpandShowWhereTest extends CypherFunSuite with RewriteTest {
  val rewriterUnderTest: Rewriter = ExpandShowWhere.instance

  test("SHOW ALIASES FOR DATABASE") {
    val originalQuery = "SHOW ALIASES FOR DATABASE YIELD * WHERE name STARTS WITH 's'"
    val original = parseForRewriting(originalQuery)
    val result = rewrite(original)

    result match {
      case ShowAliases(
          None,
          Some(Left((
            Yield(
              ReturnItems(AdditiveProjection, _, Some(columns)),
              None,
              None,
              None,
              Some(Where(StartsWith(Variable("name"), StringLiteral("s")))),
              DefaultYield
            ),
            None
          ))),
          _
        ) =>
        columns shouldBe List(
          "name",
          "composite",
          "database",
          "location",
          "url",
          "user",
          "driver",
          "defaultLanguage",
          "properties"
        )
      case _ => fail(
          s"\n$originalQuery\nshould be rewritten to:\nSHOW ALIASES FOR DATABASE YIELD * WHERE name STARTS WITH 's'\nbut was rewritten to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
        )
    }
  }

  test("SHOW ROLES") {
    val originalQuery = "SHOW ROLES WHERE name STARTS WITH 's'"
    val original = parseForRewriting(originalQuery)
    val result = rewrite(original)

    // SHOW ROLES has brief (List(role)) and verbose (List(role, immutable))
    result match {
      case ShowRoles(
          false,
          false,
          true,
          false,
          Some(Left((
            Yield(
              ReturnItems(AdditiveProjection, _, Some(columns)),
              None,
              None,
              None,
              Some(Where(StartsWith(Variable("name"), StringLiteral("s")))),
              YieldAddedInRewrite
            ),
            None
          ))),
          _
        ) =>
        columns shouldBe List(
          "role"
        )
      case _ => fail(
          s"\n$originalQuery\nshould be rewritten to:\nSHOW ALL ROLES YIELD * WHERE role STARTS WITH 's'\nbut was rewritten to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
        )
    }
  }

  test("SHOW PRIVILEGES") {
    assertRewrite(
      "SHOW PRIVILEGES WHERE scope STARTS WITH 's'",
      "SHOW PRIVILEGES YIELD * WHERE scope STARTS WITH 's'",
      List("access", "action", "resource", "graph", "segment", "role", "immutable")
    )
  }

  test("SHOW PRIVILEGES AS COMMANDS") {
    val originalQuery = "SHOW PRIVILEGES AS COMMANDS WHERE command CONTAINS 'MATCH'"
    val original = parseForRewriting(originalQuery)
    val result = rewrite(original)

    // SHOW PRIVILEGES AS COMMANDS has brief (List(command)) and verbose (List(command, immutable))
    result match {
      case ShowPrivilegeCommands(
          ShowAllPrivileges(),
          false,
          Some(Left((
            Yield(
              ReturnItems(AdditiveProjection, _, Some(columns)),
              None,
              None,
              None,
              Some(Where(Contains(Variable("command"), StringLiteral("MATCH")))),
              YieldAddedInRewrite
            ),
            None
          ))),
          _
        ) =>
        columns shouldBe List(
          "command"
        )
      case _ => fail(
          s"\n$originalQuery\nshould be rewritten to:\nSHOW ALL PRIVILEGES AS COMMANDS YIELD * WHERE command CONTAINS \"MATCH\"\nbut was rewritten to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
        )
    }
  }

  test("SHOW USERS") {
    assertRewrite(
      "SHOW USERS WHERE name STARTS WITH 'g'",
      "SHOW USERS YIELD * WHERE name STARTS WITH 'g'",
      List("user", "roles", "passwordChangeRequired", "suspended", "home")
    )
  }

  test("SHOW USERS WITH AUTH") {
    assertRewrite(
      "SHOW USERS WITH AUTH WHERE name STARTS WITH 'g'",
      "SHOW USERS WITH AUTH YIELD * WHERE name STARTS WITH 'g'",
      List("user", "roles", "passwordChangeRequired", "suspended", "home", "provider", "auth")
    )
  }

  test("SHOW CURRENT USER") {
    assertRewrite(
      "SHOW CURRENT USER WHERE name STARTS WITH 'g'",
      "SHOW CURRENT USER YIELD * WHERE name STARTS WITH 'g'",
      List("user", "roles", "passwordChangeRequired", "suspended", "home")
    )
  }

  test("SHOW AUTH RULES") {
    assertRewrite(
      "SHOW AUTH RULES WHERE name STARTS WITH 'g'",
      "SHOW AUTH RULES YIELD * WHERE name STARTS WITH 'g'",
      List("name", "condition", "enabled", "roles"),
      Some(CypherVersion.Cypher25)
    )
  }

  private def assertRewrite(
    originalQuery: String,
    expectedQuery: String,
    expectedDefaultColumns: List[String],
    cypherVersion: Option[CypherVersion] = None
  ): Unit = {
    val (expected, result) =
      if (cypherVersion.nonEmpty)
        getRewrite(cypherVersion.get, originalQuery, expectedQuery)
      else
        getRewrite(originalQuery, expectedQuery)

    val updatedYield = expected.asInstanceOf[ReadAdministrationCommand].yieldOrWhere.map {
      case Left((y, r)) if y.returnItems.defaultOrderOnColumns.isEmpty =>
        Left((
          y
            .withReturnItems(y.returnItems.withDefaultOrderOnColumns(expectedDefaultColumns))
            .withYieldType(YieldAddedInRewrite),
          r
        ))
      case o => o
    }
    val withYield = expected.asInstanceOf[ReadAdministrationCommand].withYieldOrWhere(updatedYield)
    // For commands with non-default columns (e.g. ShowUsers.tags), apply() inflates
    // defaultColumnSet under YIELD *, but the rewriter preserves the original WHERE-form
    // defaultColumnSet via copy(). Align expected to the rewriter's view.
    val updatedExpected = (withYield, result) match {
      case (e: ShowUsers, r: ShowUsers) =>
        r.defaultColumnSet.map(_.name) shouldBe expectedDefaultColumns
        e.copy(defaultColumnSet = r.defaultColumnSet)(e.position)
      case (e: ShowCurrentUser, r: ShowCurrentUser) =>
        r.defaultColumnSet.map(_.name) shouldBe expectedDefaultColumns
        e.copy(defaultColumnSet = r.defaultColumnSet)(e.position)
      case _ => withYield
    }

    assert(
      result === updatedExpected,
      s"\n$originalQuery\nshould be rewritten to:\n$expectedQuery\nbut was rewritten to:\n${prettifier.asString(result.asInstanceOf[Statement])}"
    )
  }
}
