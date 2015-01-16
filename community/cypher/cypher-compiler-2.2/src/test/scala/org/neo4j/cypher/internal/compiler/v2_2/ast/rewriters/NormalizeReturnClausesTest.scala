/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

class NormalizeReturnClausesTest extends CypherFunSuite with RewriteTest {
  val rewriterUnderTest: Rewriter = normalizeReturnClauses

  test("alias RETURN clause items") {
    assertRewrite(
      """MATCH n
        |RETURN n, n.foo AS foo, n.bar
      """.stripMargin,
      """MATCH n
        |RETURN n AS `n`, n.foo AS foo, n.bar AS `n.bar`
      """.stripMargin)
  }

  test("introduce WITH clause for ORDER BY") {
    assertRewrite(
      """MATCH n
        |RETURN n.foo AS foo, n.bar ORDER BY foo SKIP 2 LIMIT 5""".stripMargin,
      """MATCH n
        |WITH n.foo AS `  FRESHID17`, n.bar AS `  FRESHID31` ORDER BY `  FRESHID17` SKIP 2 LIMIT 5
        |RETURN `  FRESHID17` AS foo, `  FRESHID31` AS `n.bar`""".stripMargin)
  }

  test("introduce WITH clause for ORDER BY where returning all IDs") {
    assertRewrite(
      """MATCH n
        |RETURN * ORDER BY n.foo SKIP 2 LIMIT 5""".stripMargin,
      """MATCH n
        |WITH * ORDER BY n.foo SKIP 2 LIMIT 5
        |RETURN *""".stripMargin)
  }

  test("introduce WITH clause for ORDER BY where returning all IDs and additional columns") {
    assertRewrite(
      """MATCH n
        |RETURN *, n.foo AS bar ORDER BY n.foo SKIP 2 LIMIT 5""".stripMargin,
      """MATCH n
        |WITH *, n.foo AS `  FRESHID20` ORDER BY `  FRESHID20` SKIP 2 LIMIT 5
        |RETURN *, `  FRESHID20` AS bar""".stripMargin)
  }

  test("match n return n, count(*) as c order by c") {
    assertRewrite(
      "match n return n, count(*) as c order by c",
      """match n
        |with n as `  FRESHID15`, count(*) as `  FRESHID18` order by `  FRESHID18`
        |return `  FRESHID15` as n, `  FRESHID18` as c""".stripMargin)
  }

  protected override def assertRewrite(originalQuery: String, expectedQuery: String) {
    val original = parseForRewriting(originalQuery)
    val expected = parseForRewriting(expectedQuery)
    val result = endoRewrite(original)
    assert(result === expected, "\n" + originalQuery)
  }

}
