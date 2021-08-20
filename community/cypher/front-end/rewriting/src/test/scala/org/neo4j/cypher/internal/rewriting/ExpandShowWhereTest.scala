/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.internal.ast.ReadAdministrationCommand
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.rewriting.rewriters.expandShowWhere
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ExpandShowWhereTest extends CypherFunSuite with RewriteTest {
  val rewriterUnderTest: Rewriter = expandShowWhere

  test("SHOW DATABASES") {
    assertRewrite(
      "SHOW DATABASES WHERE name STARTS WITH 's'",
      "SHOW DATABASES YIELD * WHERE name STARTS WITH 's'",
      List("name", "address", "role", "requestedStatus", "currentStatus", "error", "default", "home")
    )
  }

  test("SHOW ROLES") {
    assertRewrite(
      "SHOW ROLES WHERE name STARTS WITH 's'",
      "SHOW ROLES YIELD * WHERE name STARTS WITH 's'",
      List("role")
    )
  }

  test("SHOW PRIVILEGES") {
    assertRewrite(
      "SHOW PRIVILEGES WHERE scope STARTS WITH 's'",
      "SHOW PRIVILEGES YIELD * WHERE scope STARTS WITH 's'",
      List("access", "action", "resource", "graph", "segment", "role")
    )
  }

  test("SHOW PRIVILEGES AS COMMANDS") {
    assertRewrite(
      "SHOW PRIVILEGES AS COMMANDS WHERE command CONTAINS 'MATCH'",
      "SHOW PRIVILEGES AS COMMANDS YIELD * WHERE command CONTAINS 'MATCH'",
      List("command")
    )
  }

  test("SHOW USERS") {
    assertRewrite(
      "SHOW USERS WHERE name STARTS WITH 'g'",
      "SHOW USERS YIELD * WHERE name STARTS WITH 'g'",
      List("user", "roles", "passwordChangeRequired", "suspended", "home")
    )
  }

  test("SHOW CURRENT USER") {
    assertRewrite(
      "SHOW CURRENT USER WHERE name STARTS WITH 'g'",
      "SHOW CURRENT USER YIELD * WHERE name STARTS WITH 'g'",
      List("user", "roles", "passwordChangeRequired", "suspended", "home")
    )
  }

  private def assertRewrite(originalQuery: String, expectedQuery: String, expectedDefaultColumns: List[String]) {
    val (expected, result) = getRewrite(originalQuery, expectedQuery)

    val updatedYield = expected.asInstanceOf[ReadAdministrationCommand].yieldOrWhere.map {
      case Left((y, r)) if y.returnItems.defaultOrderOnColumns.isEmpty =>
        Left((y.withReturnItems(y.returnItems.withDefaultOrderOnColumns(expectedDefaultColumns)), r))
      case o => o
    }
    val updatedExpected = expected.asInstanceOf[ReadAdministrationCommand].withYieldOrWhere(updatedYield)

    assert(result === updatedExpected, s"\n$originalQuery\nshould be rewritten to:\n$expectedQuery\nbut was rewritten to:\n${prettifier.asString(result.asInstanceOf[Statement])}")
  }
}
