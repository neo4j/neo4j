/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.frontend.v2_3.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{Rewriter, SemanticState, inSequence}

class ProjectFreshSortExpressionsTest extends CypherFunSuite with RewriteTest with AstConstructionTestSupport {
  val rewriterUnderTest: Rewriter = projectFreshSortExpressions

  test("don't adjust WITH without ORDER BY or WHERE") {
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

  test("duplicate WITH containing ORDER BY that refers to previous identifier") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop ORDER BY prop + n.x
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n AS n, n.prop AS prop
        |WITH prop AS prop, prop + n.x AS `  FRESHID42`
        |WITH prop AS prop, `  FRESHID42` AS `  FRESHID42` ORDER BY `  FRESHID42`
        |WITH prop AS prop
        |RETURN prop AS prop
      """.stripMargin)
  }

  test("duplicate RETURN containing ORDER BY after WITH") {
    assertRewrite(
      """WITH 1 AS p, count(*) AS rng
        |RETURN p ORDER BY rng
      """.stripMargin,
      """WITH 1 AS p, count(*) AS rng
        |WITH p AS `  FRESHID36`, rng AS rng
        |WITH `  FRESHID36` AS `  FRESHID36` ORDER BY rng
        |RETURN `  FRESHID36` AS p
      """.stripMargin
    )
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

  test("handle RETURN * ORDERBY property") {
    assertRewrite(
      """MATCH n
        |RETURN * ORDER BY n.prop
      """.stripMargin,
      """MATCH n
        |WITH n AS n
        |WITH n AS n, n.prop AS `  FRESHID28`
        |WITH n AS n, `  FRESHID28` AS `  FRESHID28` ORDER BY `  FRESHID28`
        |WITH n AS n
        |RETURN n AS n
      """.stripMargin)
  }

  test("Does not introduce WITH for ORDER BY over preserved identifier") {
    assertIsNotRewritten(
    """MATCH n
      |WITH n AS n, n.prop AS prop
      |WITH n AS n, prop AS prop ORDER BY prop
      |RETURN n AS n
    """.stripMargin
    )
  }

  test("Does not introduce WITH for WHERE over preserved identifier") {
    assertIsNotRewritten(
      """MATCH n
        |WITH n AS n, n.prop AS prop
        |WITH n AS n, prop AS prop WHERE prop
        |RETURN n AS n
      """.stripMargin
    )
  }

  protected override def assertRewrite(originalQuery: String, expectedQuery: String) {
    val original = ast(originalQuery)
    val expected = ast(expectedQuery)
    val result = endoRewrite(original)
    assert(result === expected, "\n" + originalQuery)
  }

  private def ast(queryText: String) = {
    val parsed = parseForRewriting(queryText)
    val mkException = new SyntaxExceptionCreator(queryText, Some(pos))
    val normalized = parsed.endoRewrite(inSequence(normalizeReturnClauses(mkException), normalizeWithClauses(mkException)))
    val checkResult = normalized.semanticCheck(SemanticState.clean)
    normalized.endoRewrite(inSequence(expandStar(checkResult.state)))
  }
}
