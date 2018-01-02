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
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticState, inSequence}

class ExpandStarTest extends CypherFunSuite with AstConstructionTestSupport {
  import parser.ParserFixture.parser

  test("rewrites * in return") {
    assertRewrite(
      "match n return *",
      "match n return n")

    assertRewrite(
      "match n,c return *",
      "match n,c return c,n")

    assertRewrite(
      "match n-->c return *",
      "match n-->c return c,n")

    assertRewrite(
      "match n-[r]->c return *",
      "match n-[r]->c return c,n,r")

    assertRewrite(
      "create (n) return *",
      "create (n) return n")

    assertRewrite(
      "match p = shortestPath((a)-[r*]->(x)) return *",
      "match p = shortestPath((a)-[r*]->(x)) return a,p,r,x")

    assertRewrite(
      "match p=(a:Start)-->b return *",
      "match p=(a:Start)-->b return a, b, p")
  }

  test("rewrites * in with") {
    assertRewrite(
      "match n with * return n",
      "match n with n return n")

    assertRewrite(
      "match n,c with * return n",
      "match n,c with c,n return n")

    assertRewrite(
      "match n-->c with * return n",
      "match n-->c with c,n return n")

    assertRewrite(
      "match n-[r]->c with * return n",
      "match n-[r]->c with c,n,r return n")

    assertRewrite(
      "match n-[r]->c with *, r.pi as x return n",
      "match n-[r]->c with c, n, r, r.pi as x return n")

    assertRewrite(
      "create (n) with * return n",
      "create (n) with n return n")

    assertRewrite(
      "match p = shortestPath((a)-[r*]->(x)) with * return p",
      "match p = shortestPath((a)-[r*]->(x)) with a,p,r,x return p")
  }

  test("symbol shadowing should be taken into account") {
    assertRewrite(
      "match a,x,y with a match b return *",
      "match a,x,y with a match b return a, b")
  }

  test("expands _PRAGMA WITHOUT") {
    assertRewrite(
      "MATCH a,x,y _PRAGMA WITHOUT a MATCH b RETURN *",
      "MATCH a,x,y WITH x, y MATCH b RETURN b, x, y")
  }

  test("keeps listed items during expand") {
    assertRewrite(
      "MATCH (n) WITH *, 1 AS b RETURN *",
      "MATCH (n) WITH n, 1 AS b RETURN b, n"
    )
  }

  private def assertRewrite(originalQuery: String, expectedQuery: String) {
    val mkException = new SyntaxExceptionCreator(originalQuery, Some(pos))
    val original = parser.parse(originalQuery).endoRewrite(inSequence(normalizeReturnClauses(mkException), normalizeWithClauses(mkException)))
    val expected = parser.parse(expectedQuery).endoRewrite(inSequence(normalizeReturnClauses(mkException), normalizeWithClauses(mkException)))

    val checkResult = original.semanticCheck(SemanticState.clean)
    val rewriter = expandStar(checkResult.state)

    val result = original.rewrite(rewriter)
    assert(result === expected)
  }
}
