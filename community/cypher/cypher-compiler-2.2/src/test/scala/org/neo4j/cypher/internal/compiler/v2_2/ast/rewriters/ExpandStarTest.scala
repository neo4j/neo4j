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
import org.neo4j.cypher.internal.compiler.v2_2.parser.ParserFixture._

class ExpandStarTest extends CypherFunSuite {
  import org.neo4j.cypher.internal.compiler.v2_2.parser.ParserFixture._

  test("rewrites * in return") {
    assertRewrite(
      "match n return *",
      "match n return n as n")

    assertRewrite(
      "match n,c return *",
      "match n,c return c as c, n as n")

    assertRewrite(
      "match n-->c return *",
      "match n-->c return c as c, n as n")

    assertRewrite(
      "match n-[r]->c return *",
      "match n-[r]->c return c as c, n as n, r as r")

    assertRewrite(
      "create (n) return *",
      "create (n) return n as n")

    assertRewrite(
      "match p = shortestPath((a)-[r*]->(x)) return *",
      "match p = shortestPath((a)-[r*]->(x)) return a as a, p as p, r as r, x as x")
  }

  test("rewrites * in with") {
    assertRewrite(
      "match n with * return n",
      "match n with n as n return n")

    assertRewrite(
      "match n,c with * return n",
      "match n,c with c as c, n as n return n")

    assertRewrite(
      "match n-->c with * return n",
      "match n-->c with c as c, n as n return n")

    assertRewrite(
      "match n-[r]->c with * return n",
      "match n-[r]->c with c as c, n as n, r as r return n")

    assertRewrite(
      "create (n) with * return n",
      "create (n) with n as n return n")

    assertRewrite(
      "match p = shortestPath((a)-[r*]->(x)) with * return p",
      "match p = shortestPath((a)-[r*]->(x)) with a as a, p as p, r as r, x as x return p")
  }

  test("symbol shadowing should be taken into account") {
    assertRewrite(
      "match a,x,y with a match b return *",
      "match a,x,y with a match b return a as a, b as b")
  }

  val semantickChecker = new SemanticChecker(mock[SemanticCheckMonitor])

  def assertRewrite(originalQuery: String, expectedQuery: String) {
    val original = parser.parse(originalQuery)
    val expected = parser.parse(expectedQuery)
    val table = semantickChecker.check(originalQuery, original)

    val result = expandStar(table)(original).getOrElse(fail("Rewriter did not accept query"))
    assert(result === expected, s"\n$originalQuery")
  }
}
