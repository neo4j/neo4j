/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2._

class ProjectFreshSortExpressionsTest extends CypherFunSuite with RewriteTest {
  val rewriterUnderTest: Rewriter = projectFreshSortExpressions

  test("dont adjust WITH without ORDER BY or WHERE") {
    assertRewrite(
      """MATCH n
        |WITH n AS n
        |RETURN n
      """.stripMargin,
      """MATCH n
        |WITH n AS n
        |RETURN n
      """.stripMargin)
  }

  test("duplicate WITH containing ORDER BY") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop ORDER BY prop
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop
        |WITH prop AS prop ORDER BY prop
        |RETURN prop
      """.stripMargin)
  }

  test("duplicate WITH containing WHERE") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop WHERE prop
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop
        |WITH prop AS prop WHERE prop
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("preserve DISTINCT on first WITH") {
    assertRewrite(
      """MATCH n
        |WITH DISTINCT n.prop AS prop ORDER BY prop
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH DISTINCT n.prop AS prop
        |WITH prop AS prop ORDER BY prop
        |RETURN prop
      """.stripMargin)

    assertRewrite(
      """MATCH n
        |WITH DISTINCT n.prop AS prop WHERE prop
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH DISTINCT n.prop AS prop
        |WITH prop AS prop WHERE prop
        |RETURN prop
      """.stripMargin)
  }

  test("carry SKIP and LIMIT with ORDER BY") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop ORDER BY prop SKIP 2 LIMIT 5
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop
        |WITH prop AS prop ORDER BY prop SKIP 2 LIMIT 5
        |RETURN prop
      """.stripMargin)
  }

  test("carry SKIP and LIMIT with WHERE") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop SKIP 2 LIMIT 5 WHERE prop
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop
        |WITH prop AS prop SKIP 2 LIMIT 5 WHERE prop
        |RETURN prop
      """.stripMargin)
  }

  test("keep WHERE with ORDER BY") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop ORDER BY prop WHERE prop
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop
        |WITH prop AS prop ORDER BY prop WHERE prop
        |RETURN prop
      """.stripMargin)
  }

  protected override def assertRewrite(originalQuery: String, expectedQuery: String) {
    val original = ast(originalQuery)
    val expected = ast(expectedQuery)
    val result = endoRewrite(original)
    assert(result === expected, "\n" + originalQuery)
  }

  private def ast(queryText: String) = {
    val parsed = parseForRewriting(queryText)
    val normalized = parsed.endoRewrite(inSequence(normalizeReturnClauses, normalizeWithClauses))
    val checkResult = normalized.semanticCheck(SemanticState.clean)
    normalized.endoRewrite(inSequence(expandStar(checkResult.state)))
  }
}
