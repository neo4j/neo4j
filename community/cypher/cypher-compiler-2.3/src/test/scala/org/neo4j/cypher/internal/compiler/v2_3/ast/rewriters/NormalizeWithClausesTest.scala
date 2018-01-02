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
import org.neo4j.cypher.internal.frontend.v2_3.{Rewriter, SemanticState, SyntaxException}

class NormalizeWithClausesTest extends CypherFunSuite with RewriteTest with AstConstructionTestSupport {
  val mkException = new SyntaxExceptionCreator("<Query>", Some(pos))
  val rewriterUnderTest: Rewriter = normalizeWithClauses(mkException)

  test("ensure identifiers are aliased") {
    assertRewrite(
      """MATCH n
        |WITH n
        |RETURN n
      """.stripMargin,
      """MATCH n
        |WITH n AS n
        |RETURN n
      """.stripMargin)
  }

  test("attach ORDER BY expressions to existing aliases") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop ORDER BY n.prop
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop ORDER BY prop
        |RETURN prop
      """.stripMargin)
  }

  test("attach WHERE expression to existing aliases") {
    assertRewrite(
      """MATCH n
        |WITH length(n.prop) > 10 AS result WHERE length(n.prop) > 10
        |RETURN result
      """.stripMargin,
      """MATCH n
        |WITH length(n.prop) > 10 AS result WHERE result
        |RETURN result
      """.stripMargin)
  }

  test("does not introduce aliases for ORDER BY with existing alias") {
    assertIsNotRewritten(
      """MATCH n
        |WITH n.prop AS prop ORDER BY prop
        |RETURN prop
      """.stripMargin)
  }

  test("does not introduce aliases for WHERE with existing alias") {
    assertIsNotRewritten(
      """MATCH n
        |WITH n.prop AS prop WHERE prop
        |RETURN prop
      """.stripMargin)
  }

  test("introduces aliases for ORDER BY expressions the depend on existing identifiers") {
    assertRewrite(
      """MATCH n
        |WITH n ORDER BY length(n.prop)
        |RETURN n
      """.stripMargin,
      """MATCH n
        |WITH n AS n
        |WITH n AS n, length(n.prop) AS `  FRESHID24` ORDER BY `  FRESHID24`
        |_PRAGMA WITHOUT `  FRESHID24`
        |RETURN n
      """.stripMargin)
  }

  test("introduces aliases for WHERE expression the depends on existing identifiers") {
    assertRewrite(
      """MATCH n
        |WITH n WHERE length(n.prop) > 10
        |RETURN n
      """.stripMargin,
      """MATCH n
        |WITH n AS n
        |WITH n AS n, length(n.prop) > 10 AS `  FRESHID36` WHERE `  FRESHID36`
        |_PRAGMA WITHOUT `  FRESHID36`
        |RETURN n
      """.stripMargin)
  }

  test("introduces aliases for ORDER BY expressions that depend on existing aliases") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop ORDER BY length(n.prop)
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop
        |WITH prop AS prop, length(prop) AS `  FRESHID37` ORDER BY `  FRESHID37`
        |_PRAGMA WITHOUT `  FRESHID37`
        |RETURN prop
      """.stripMargin)
  }

  test("introduces aliases for WHERE expression that depends on existing aliases") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop WHERE length(n.prop) > 10
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop
        |WITH prop AS prop, length(prop) > 10 AS `  FRESHID49` WHERE `  FRESHID49`
        |_PRAGMA WITHOUT `  FRESHID49`
        |RETURN prop
      """.stripMargin)
  }

  test("introduces identifiers for ORDER BY expressions that depend on non-aliased identifiers") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop ORDER BY n.foo DESC
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n AS n, n.prop AS prop
        |WITH prop AS prop, n.foo AS `  FRESHID39` ORDER BY `  FRESHID39` DESC
        |_PRAGMA WITHOUT `  FRESHID39`
        |RETURN prop
      """.stripMargin)
  }

  test("introduces identifiers for WHERE expression that depend on non-aliased identifiers") {
    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop WHERE n.foo > 10
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n AS n, n.prop AS prop
        |WITH prop AS prop, n.foo > 10 AS `  FRESHID40` WHERE `  FRESHID40`
        |_PRAGMA WITHOUT `  FRESHID40`
        |RETURN prop
      """.stripMargin)
  }

  test("does not introduce identifiers for ORDER BY expressions in WITH *") {
    assertRewrite(
      """MATCH n
        |WITH *, n.prop AS prop ORDER BY n.foo DESC
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH *, n.prop AS prop
        |WITH *, n.foo AS `  FRESHID42` ORDER BY `  FRESHID42` DESC
        |_PRAGMA WITHOUT `  FRESHID42`
        |RETURN prop
      """.stripMargin)
  }

  test("does not introduce identifiers for WHERE expression in WITH *") {
    assertRewrite(
      """MATCH n
        |WITH *, n.prop AS prop WHERE n.foo > 10
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH *, n.prop AS prop
        |WITH *, n.foo > 10 AS `  FRESHID43` WHERE `  FRESHID43`
        |_PRAGMA WITHOUT `  FRESHID43`
        |RETURN prop
      """.stripMargin)
  }

  test("does not attach ORDER BY expressions to unaliased items") {
    // Note: unaliased items in WITH are invalid, and will be caught during semantic check
    assertRewriteAndSemanticErrors(
      """MATCH n
        |WITH n.prop ORDER BY n.prop
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n AS n, n.prop
        |WITH n.prop, n.prop AS `  FRESHID31` ORDER BY `  FRESHID31`
        |_PRAGMA WITHOUT `  FRESHID31`
        |RETURN prop
      """.stripMargin,
      "Expression in WITH must be aliased (use AS) (line 2, column 6 (offset: 13))"
    )
  }

  test("does not attach WHERE expression to unaliased items") {
    // Note: unaliased items in WITH are invalid, and will be caught during semantic check
    assertRewriteAndSemanticErrors(
      """MATCH n
        |WITH n.prop WHERE n.prop
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n AS n, n.prop
        |WITH n.prop, n.prop AS `  FRESHID28` WHERE `  FRESHID28`
        |_PRAGMA WITHOUT `  FRESHID28`
        |RETURN prop
      """.stripMargin,
      "Expression in WITH must be aliased (use AS) (line 2, column 6 (offset: 13))"
    )
  }

  test("rejects use of aggregation in ORDER BY if aggregation is not used in associated WITH") {
    // Note: aggregations in ORDER BY that don't also appear in WITH are invalid
    try {
      rewrite(parseForRewriting(
        """MATCH n
          |WITH n.prop AS prop ORDER BY max(n.foo)
          |RETURN prop
        """.stripMargin))
      fail("We shouldn't get here")
    } catch {
      case (e: SyntaxException) =>
        e.getMessage should equal("Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding WITH (line 2, column 1 (offset: 8))")
    }
  }

  test("accepts use of aggregation in ORDER BY if aggregation is used in associated WITH") {
    assertRewrite(
      """MATCH n
          |WITH n.prop AS prop, max(n.foo) AS m ORDER BY max(n.foo)
        |RETURN prop AS prop, m AS m
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop, max(n.foo) AS m ORDER BY m
        |RETURN  prop AS prop, m AS m
      """.stripMargin
    )
  }

  test("does not introduce alias for WHERE containing aggregate") {
    // Note: aggregations in WHERE are invalid, and will be caught during semantic check
    assertRewriteAndSemanticErrors(
      """MATCH n
        |WITH n.prop AS prop WHERE max(n.foo)
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop WHERE max(n.foo)
        |RETURN prop
      """.stripMargin,
      "Invalid use of aggregating function max(...) in this context (line 2, column 27 (offset: 34))"
    )
  }

  test("preserves SKIP and LIMIT") {
    assertRewrite(
      """MATCH n
        |WITH n SKIP 5 LIMIT 2
        |RETURN n
      """.stripMargin,
      """MATCH n
        |WITH n AS n SKIP 5 LIMIT 2
        |RETURN n
      """.stripMargin)
  }

  test("preserves SKIP and LIMIT when introducing aliases for ORDER BY expressions") {
    assertRewrite(
      """MATCH n
        |WITH n ORDER BY n.prop SKIP 5 LIMIT 2
        |RETURN n
      """.stripMargin,
      """MATCH n
        |WITH n AS n
        |WITH n AS n, n.prop AS `  FRESHID26` ORDER BY `  FRESHID26` SKIP 5 LIMIT 2
        |_PRAGMA WITHOUT `  FRESHID26`
        |RETURN n
      """.stripMargin)
  }

  test("preserves SKIP and LIMIT when introducing aliases for WHERE expression") {
    assertRewrite(
      """MATCH n
        |WITH n SKIP 5 LIMIT 2 WHERE n.prop > 10
        |RETURN n
      """.stripMargin,
      """MATCH n
        |WITH n AS n
        |WITH n AS n, n.prop > 10 AS `  FRESHID43` SKIP 5 LIMIT 2 WHERE `  FRESHID43`
        |_PRAGMA WITHOUT `  FRESHID43`
        |RETURN n
      """.stripMargin)
  }

  test("aggregating: preserves DISTINCT when replacing ORDER BY expressions") {
    assertRewrite(
      """MATCH n
        |WITH DISTINCT n.prop AS prop ORDER BY n.prop
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH DISTINCT n.prop AS prop ORDER BY prop
        |RETURN prop
      """.stripMargin)
  }

  test("aggregating: preserves DISTINCT when replacing WHERE expressions") {
    assertRewrite(
      """MATCH n
        |WITH DISTINCT n.prop AS prop WHERE n.prop
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH DISTINCT n.prop AS prop WHERE prop
        |RETURN prop
      """.stripMargin)
  }

  test("aggregating: does not change grouping set when introducing aliases for ORDER BY") {
    assertRewrite(
      """MATCH n
        |WITH DISTINCT n.prop AS prop ORDER BY length(n.prop)
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH DISTINCT n.prop AS prop
        |WITH prop AS prop, length(prop) AS `  FRESHID46` ORDER BY `  FRESHID46`
        |_PRAGMA WITHOUT `  FRESHID46`
        |RETURN prop
      """.stripMargin)

    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop, count(*) AS count ORDER BY length(n.prop)
        |RETURN prop, count
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop, count(*) AS count
        |WITH prop AS prop, count AS count, length(prop) AS `  FRESHID56` ORDER BY `  FRESHID56`
        |_PRAGMA WITHOUT `  FRESHID56`
        |RETURN prop, count
      """.stripMargin)
  }

  test("aggregating: does not change grouping set when introducing aliases for WHERE") {
    assertRewrite(
      """MATCH n
        |WITH DISTINCT n.prop AS prop WHERE length(n.prop) = 1
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH DISTINCT n.prop AS prop
        |WITH prop AS prop, length(prop) = 1 AS `  FRESHID58` WHERE `  FRESHID58`
        |_PRAGMA WITHOUT `  FRESHID58`
        |RETURN prop
      """.stripMargin)

    assertRewrite(
      """MATCH n
        |WITH n.prop AS prop, count(*) AS count WHERE length(n.prop) = 1
        |RETURN prop, count
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop, count(*) AS count
        |WITH prop AS prop, count AS count, length(prop) = 1 AS `  FRESHID68` WHERE `  FRESHID68`
        |_PRAGMA WITHOUT `  FRESHID68`
        |RETURN prop, count
      """.stripMargin)
  }

  test("aggregating: does not change grouping set when introducing aliases for ORDER BY with non-grouping expression") {
    // Note: using a non-grouping expression for ORDER BY when aggregating is invalid, and will be caught during semantic check
    assertRewriteAndSemanticErrors(
      """MATCH n
        |WITH DISTINCT n.prop AS prop ORDER BY n.foo
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH DISTINCT n.prop AS prop
        |WITH prop AS prop, n.foo AS `  FRESHID48` ORDER BY `  FRESHID48`
        |_PRAGMA WITHOUT `  FRESHID48`
        |RETURN prop
      """.stripMargin,
      "n not defined (line 2, column 39 (offset: 46))")

    assertRewriteAndSemanticErrors(
      """MATCH n
        |WITH n.prop AS prop, collect(n.foo) AS foos ORDER BY n.foo
        |RETURN prop, foos
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop, collect(n.foo) AS foos
        |WITH prop AS prop, foos AS foos, n.foo AS `  FRESHID63` ORDER BY `  FRESHID63`
        |_PRAGMA WITHOUT `  FRESHID63`
        |RETURN prop, foos
      """.stripMargin,
      "n not defined (line 2, column 54 (offset: 61))")
  }

  test("aggregating: does not change grouping set when introducing aliases for WHERE with non-grouping expression") {
    // Note: using a non-grouping expression for ORDER BY when aggregating is invalid, and will be caught during semantic check
    assertRewriteAndSemanticErrors(
      """MATCH n
        |WITH DISTINCT n.prop AS prop WHERE n.foo
        |RETURN prop
      """.stripMargin,
      """MATCH n
        |WITH DISTINCT n.prop AS prop
        |WITH prop AS prop, n.foo AS `  FRESHID45` WHERE `  FRESHID45`
        |_PRAGMA WITHOUT `  FRESHID45`
        |RETURN prop
      """.stripMargin,
      "n not defined (line 2, column 36 (offset: 43))")


    assertRewriteAndSemanticErrors(
      """MATCH n
        |WITH n.prop AS prop, collect(n.foo) AS foos WHERE n.foo
        |RETURN prop, foos
      """.stripMargin,
      """MATCH n
        |WITH n.prop AS prop, collect(n.foo) AS foos
        |WITH prop AS prop, foos AS foos, n.foo AS `  FRESHID60` WHERE `  FRESHID60`
        |_PRAGMA WITHOUT `  FRESHID60`
        |RETURN prop, foos
      """.stripMargin,
      "n not defined (line 2, column 51 (offset: 58))")
  }


  // Below: unordered, exploratory tests

  test("MATCH (u)-[r1]->(v) WITH r1 AS r2, rand() AS c ORDER BY c MATCH (a)-[r2]->(b) RETURN r2 AS rel") {
    assertIsNotRewritten(
      """MATCH (u)-[r1]->(v)
        |WITH r1 AS r2, rand() AS c ORDER BY c
        |MATCH (a)-[r2]->(b)
        |RETURN r2 AS rel
      """.stripMargin)
  }

  test("MATCH foo WITH {meh} AS x ORDER BY x.prop DESC LIMIT 4 RETURN x") {
    assertRewrite(
      """MATCH foo
        |WITH {meh} AS x ORDER BY x.prop DESC LIMIT 4
        |RETURN x
      """.stripMargin,
      """MATCH foo
        |WITH {meh} AS x
        |WITH x AS x, x.prop AS `  FRESHID37` ORDER BY `  FRESHID37` DESC LIMIT 4
        |_PRAGMA WITHOUT `  FRESHID37`
        |RETURN x
      """.stripMargin)
  }

  test("match n with n order by n.name ASC skip 2 return n") {
    assertRewrite(
      """match n
        |with n order by n.name ASC skip 2
        |return n
      """.stripMargin,
      """match n
        |with n AS n
        |with n AS n, n.name AS `  FRESHID26` order by `  FRESHID26` ASC skip 2
        |_PRAGMA WITHOUT `  FRESHID26`
        |return n
      """.stripMargin)
  }

  test("match x WITH DISTINCT x as otherName ORDER BY x.name RETURN otherName") {
    assertRewrite(
      """match x
        |WITH DISTINCT x as otherName ORDER BY x.name
        |RETURN otherName
      """.stripMargin,
      """match x
        |WITH DISTINCT x as otherName
        |WITH otherName AS otherName, otherName.name AS `  FRESHID48` ORDER BY `  FRESHID48`
        |_PRAGMA WITHOUT `  FRESHID48`
        |RETURN otherName
      """.stripMargin)
  }

  test("match x WITH x as otherName ORDER BY x.name + otherName.name RETURN otherName") {
    assertRewrite(
      """match x
        |WITH x as otherName ORDER BY x.name + otherName.name
        |RETURN otherName
      """.stripMargin,
      """match x
        |WITH x as otherName
        |WITH otherName as otherName, otherName.name + otherName.name AS `  FRESHID44` ORDER BY `  FRESHID44`
        |_PRAGMA WITHOUT `  FRESHID44`
        |RETURN otherName
      """.stripMargin)
  }

  test("MATCH (a)-[r]->(b) WITH a, r, b, rand() AS c ORDER BY c MATCH (a)-[r]->(b) RETURN r AS rel") {
    assertRewrite(
      """MATCH (a)-[r]->(b)
        |WITH a, r, b, rand() AS c ORDER BY c
        |MATCH (a)-[r]->(b)
        |RETURN r AS rel
      """.stripMargin,
      """MATCH (a)-[r]->(b)
        |WITH a AS a, r AS r, b AS b, rand() AS c ORDER BY c
        |MATCH (a)-[r]->(b)
        |RETURN r AS rel
      """.stripMargin
    )
  }

  test("match n where id(n) IN [0,1,2,3] WITH n.division AS div, max(n.age) AS age order by max(n.age) RETURN div, age") {
    assertRewrite(
      """match n where id(n) IN [0,1,2,3]
        |WITH n.division AS div, max(n.age) AS age order by max(n.age)
        |RETURN div, age
      """.stripMargin,
      """match n where id(n) IN [0,1,2,3]
        |WITH n.division AS div, max(n.age) AS age order by age
        |RETURN div, age
      """.stripMargin
    )
  }

  test("match (a) with a where true return a") {
    assertRewrite(
      """match (a)
        |with a where true
        |return a
      """.stripMargin,
      """match (a)
        |with a as a
        |with a as a, true as `  FRESHID23` where `  FRESHID23`
        |_PRAGMA WITHOUT `  FRESHID23`
        |return a
      """.stripMargin
    )
  }

  test("match (n) return * order by id(n)") {
    assertRewrite(
      """MATCH (n)
        |WITH * ORDER BY id(n)
        |RETURN *
      """.stripMargin,
      """match (n)
        |WITH *
        |WITH *, id(n) AS `  FRESHID26` ORDER BY `  FRESHID26`
        |_PRAGMA WITHOUT `  FRESHID26`
        |RETURN *
      """.stripMargin
    )
  }

  test("match (n) with n, 0 as foo with n as n order by foo, n.bar return n") {
    assertRewrite(
      """MATCH (n)
        |WITH n, 0 AS foo
        |WITH n AS n ORDER BY foo, n.bar
        |RETURN n
      """.stripMargin,
    """MATCH (n)
      |WITH n AS n, 0 AS foo
      |WITH foo AS foo, n AS n
      |WITH n AS n, foo AS foo, n.bar AS `  FRESHID55` ORDER BY foo, `  FRESHID55`
      |_PRAGMA WITHOUT `  FRESHID55`
      |RETURN n
    """.stripMargin)
  }

  test("match n with n as n order by max(n) return n") {
    evaluating { rewriting("match n with n as n order by max(n) return n") } should produce[SyntaxException]
  }

  protected override def assertRewrite(originalQuery: String, expectedQuery: String) {
    val original = parseForRewriting(originalQuery.replace("\r\n", "\n"))
    val expected = parseForRewriting(expectedQuery.replace("\r\n", "\n"))
    val result = endoRewrite(original)
    assert(result === expected, "\n" + originalQuery)

    val checkResult = result.semanticCheck(SemanticState.clean)
    assert(checkResult.errors === Seq())
  }

  protected def assertRewriteAndSemanticErrors(originalQuery: String, expectedQuery: String, semanticErrors: String*) {
    val original = parseForRewriting(originalQuery)
    val expected = parseForRewriting(expectedQuery)
    val result = endoRewrite(original)
    assert(result === expected, "\n" + originalQuery)

    val checkResult = result.semanticCheck(SemanticState.clean)
    val errors = checkResult.errors.map(error => s"${error.msg} (${error.position})").toSet
    semanticErrors.foreach(msg =>
      assert(errors contains msg, s"Error '$msg' not produced (errors: $errors)}")
    )
  }

  protected def rewriting(queryText: String): Unit = {
    endoRewrite(parseForRewriting(queryText))
  }
}
