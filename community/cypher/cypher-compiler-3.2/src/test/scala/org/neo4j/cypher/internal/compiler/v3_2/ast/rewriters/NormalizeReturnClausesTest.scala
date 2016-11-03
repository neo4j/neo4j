/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.frontend.v3_2.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, SyntaxException}

class NormalizeReturnClausesTest extends CypherFunSuite with RewriteTest with AstConstructionTestSupport {
  val mkException = new SyntaxExceptionCreator("<Query>", Some(pos))
  val rewriterUnderTest: Rewriter = normalizeReturnClauses(mkException)

  test("alias RETURN clause items") {
    assertRewrite(
      """MATCH (n)
        |RETURN n, n.foo AS foo, n.bar
      """.stripMargin,
      """MATCH (n)
        |RETURN n AS `n`, n.foo AS foo, n.bar AS `n.bar`
      """.stripMargin)
  }

  test("introduce WITH clause for ORDER BY") {
    assertRewrite(
      """MATCH (n)
        |RETURN n.foo AS foo, n.bar ORDER BY foo SKIP 2 LIMIT 5""".stripMargin,
      """MATCH (n)
        |WITH n.foo AS `  FRESHID19`, n.bar AS `  FRESHID33` ORDER BY `  FRESHID19` SKIP 2 LIMIT 5
        |RETURN `  FRESHID19` AS foo, `  FRESHID33` AS `n.bar`""".stripMargin)
  }

  test("introduce WITH clause for ORDER BY where returning all IDs") {
    assertRewrite(
      """MATCH (n)
        |RETURN * ORDER BY n.foo SKIP 2 LIMIT 5""".stripMargin,
      """MATCH (n)
        |WITH * ORDER BY n.foo SKIP 2 LIMIT 5
        |RETURN *""".stripMargin)
  }

  test("introduce WITH clause for ORDER BY where returning all IDs and additional columns") {
    assertRewrite(
      """MATCH (n)
        |RETURN *, n.foo AS bar ORDER BY n.foo SKIP 2 LIMIT 5""".stripMargin,
      """MATCH (n)
        |WITH *, n.foo AS `  FRESHID22` ORDER BY `  FRESHID22` SKIP 2 LIMIT 5
        |RETURN *, `  FRESHID22` AS bar""".stripMargin)
  }

  test("match (n) return n, count(*) as c order by c") {
    assertRewrite(
      "match (n) return n, count(*) as c order by c",
      """match (n)
        |with n as `  FRESHID17`, count(*) as `  FRESHID20` order by `  FRESHID20`
        |return `  FRESHID17` as n, `  FRESHID20` as c""".stripMargin)
  }

  test("rejects use of aggregation in ORDER BY if aggregation is not used in associated RETURN") {
    // Note: aggregations in ORDER BY that don't also appear in WITH are invalid
    try {
      rewrite(parseForRewriting(
        """MATCH (n)
          |RETURN n.prop AS prop ORDER BY max(n.foo)
        """.stripMargin))
      fail("We shouldn't get here")
    } catch {
      case (e: SyntaxException) =>
        e.getMessage should equal("Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding RETURN (line 2, column 1 (offset: 10))")
    }
  }

  test("accepts use of aggregation in ORDER BY if aggregation is used in associated RETURN") {
    assertRewrite(
      """MATCH (n)
        |RETURN n.prop AS prop, max(n.foo) AS m ORDER BY max(n.foo)
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS `  FRESHID19`, max(n.foo) AS `  FRESHID33` ORDER BY `  FRESHID33`
        |RETURN  `  FRESHID19` AS prop,  `  FRESHID33` AS m
      """.stripMargin
    )
  }

  test("should replace the aggregation function in the order by") {
    assertRewrite(
      """MATCH (n)
        |RETURN n as n, count(n) as count ORDER BY count(n)""".stripMargin,
      """MATCH (n)
        |WITH n AS `  FRESHID17`, count(n) AS `  FRESHID25` ORDER BY `  FRESHID25`
        |RETURN `  FRESHID17` AS n, `  FRESHID25` as count""".stripMargin)
  }

  protected override def assertRewrite(originalQuery: String, expectedQuery: String) {
    val original = parseForRewriting(originalQuery)
    val expected = parseForRewriting(expectedQuery)
    val result = endoRewrite(original)
    assert(result === expected, "\n" + originalQuery)
  }

  protected def rewriting(queryText: String): Unit = {
    endoRewrite(parseForRewriting(queryText))
  }
}
