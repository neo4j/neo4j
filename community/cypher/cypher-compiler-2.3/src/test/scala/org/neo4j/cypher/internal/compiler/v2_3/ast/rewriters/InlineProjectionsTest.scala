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

import org.apache.commons.lang3.SystemUtils
import org.neo4j.cypher.internal.compiler.v2_3.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.v2_3.planner.{AstRewritingTestSupport, CantHandleQueryException}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticState, inSequence}

class InlineProjectionsTest extends CypherFunSuite with AstRewritingTestSupport {

  test("should inline: MATCH a, b, c WITH c AS c, b AS a RETURN c") {
    val result = projectionInlinedAst(
      """MATCH a, b, c
        |WITH c AS c, b AS d
        |RETURN c
      """.stripMargin)

    result should equal(ast(
      """MATCH a, b, c
        |WITH c AS c, b AS b
        |RETURN c AS c
      """.stripMargin))
  }

  test("should inline: WITH {b} AS tmp, {r} AS r WITH {a} AS b AS a, r LIMIT 1 MATCH (a)-[r]->(b) RETURN a, r, b") {
    val result = projectionInlinedAst(
      """WITH {a} AS b, {b} AS tmp, {r} AS r
        |WITH b AS a, r LIMIT 1
        |MATCH (a)-[r]->(b)
        |RETURN a, r, b
      """.stripMargin)

    result should equal(ast( """
                               |WITH {a} AS b, {r} AS r
                               |WITH b AS a, r LIMIT 1
                               |MATCH (a)-[r]->(b)
                               |RETURN a, r, b
                             """.stripMargin))
  }

  test("should inline: MATCH a, b, c WITH c AS d, b AS a RETURN d") {
    val result = projectionInlinedAst(
      """MATCH a, b, c
        |WITH c AS d, b AS e
        |RETURN d
      """.stripMargin)

    result should equal(ast(
      """MATCH a, b, c
        |WITH c AS c, b AS b
        |RETURN c AS d
      """.stripMargin))
  }

  test("should  inline: MATCH n WITH n AS m RETURN m => MATCH n RETURN n") {
    val result = projectionInlinedAst(
      """MATCH n
        |WITH n AS m
        |RETURN m
      """.stripMargin)

    result should equal(ast(
      """MATCH n
        |WITH n
        |RETURN n AS m
      """.stripMargin))
  }

  test("should  inline: MATCH (a:Start) WITH a.prop AS property LIMIT 1 MATCH (b) WHERE id(b) = property RETURN b") {
    val result = projectionInlinedAst(
      """MATCH (a:Start)
        |WITH a.prop AS property LIMIT 1
        |MATCH (b) WHERE id(b) = property
        |RETURN b, property
      """.stripMargin)

    result should equal(ast(
      """MATCH (a:Start) WITH a LIMIT 1
        |MATCH (b) WHERE id(b) = a.prop
        |RETURN b, a.prop AS property
      """.stripMargin))
  }

  test("should inline: MATCH (a) WITH a WHERE TRUE RETURN a") {
    val result = projectionInlinedAst(
      """MATCH (a)
        |WITH a WHERE TRUE
        |RETURN a
      """.stripMargin)

    result should equal(parser.parse(
      """MATCH (a)
        |WITH a AS a
        |WITH a AS a WHERE true
        |WITH a AS a
        |RETURN a AS a
      """.stripMargin))
  }

  test("should inline pattern identifiers when possible") {
    val result = projectionInlinedAst(
      """MATCH n
        |WITH n
        |MATCH n-->x
        |RETURN x
      """.stripMargin)

    result should equal(ast(
      """MATCH n
        |WITH n
        |MATCH n-->x
        |RETURN x""".stripMargin))
  }

  test("should inline: WITH 1 AS x RETURN 1 + x => _PRAGMA WITH NONE RETURN 1 + 1") {
    val result = projectionInlinedAst(
      """WITH 1 AS x
        |RETURN 1 + x
      """.stripMargin)

    result should equal(ast(
      """_PRAGMA WITH NONE
        |RETURN 1 + 1 AS `1 + x`
      """.stripMargin))
  }

  test("should inline: WITH 1 as b RETURN b => RETURN 1 AS `b`") {
    val result = projectionInlinedAst(
      """WITH 1 as b
        |RETURN b
      """.stripMargin)

    result should equal(ast(
      """_PRAGMA WITH NONE
        |RETURN 1 AS `b`
      """.stripMargin))
  }

  test("should not inline aggregations: WITH 1 as b WITH DISTINCT b AS c RETURN c => WITH DISTINCT 1 AS c RETURN c AS c") {
    val result = projectionInlinedAst(
      """WITH 1 as b
        |WITH DISTINCT b AS c
        |RETURN c
      """.stripMargin)

    result should equal(ast(
      """_PRAGMA WITH NONE
        |WITH DISTINCT 1 AS `c`
        |RETURN c AS `c`
      """.stripMargin))
  }

  test("should not inline identifiers into patterns: WITH {node} as a MATCH (a) RETURN a => WITH {node} as a MATCH (a) RETURN a AS `a`") {
    val result = projectionInlinedAst(
      """WITH {node} as a
        |MATCH (a)
        |RETURN a
      """.stripMargin)

    result should equal(ast(
      """WITH {node} as a
        |MATCH (a)
        |RETURN a AS `a`
      """.stripMargin))
  }

  test("should inline multiple identifiers across multiple WITH clauses: WITH 1 as n WITH n+1 AS m RETURN m => RETURN 1+1 as m") {
    val result = projectionInlinedAst(
      """WITH 1 as n
        |WITH n + 1 AS m
        |RETURN m
      """.stripMargin)

    result should equal(ast(
      """_PRAGMA WITH NONE
        |_PRAGMA WITH NONE
        |RETURN 1+1 as `m`
      """.stripMargin))
  }

  test("should inline node patterns: MATCH (a) WITH a as b MATCH (b) RETURN b => MATCH (a) _PRAGMA WITH NONE MATCH (a) RETURN a as `b`") {
    val result = projectionInlinedAst(
      """MATCH (a)
        |WITH a as b
        |MATCH (b)
        |RETURN b
      """.stripMargin)

    result should equal(ast(
      """MATCH (a)
        |WITH a AS a
        |MATCH (a)
        |RETURN a as `b`
      """.stripMargin))
  }

  test("should inline relationship patterns: MATCH ()-[a]->() WITH a as b MATCH ()-[b]->() RETURN b => MATCH ()-[a]->() _PRAGMA WITH NONE MATCH ()-[a]->() RETURN a as `b`") {
    val result = projectionInlinedAst(
      """MATCH ()-[a]->()
        |WITH a as b
        |MATCH ()-[b]->()
        |RETURN b
      """.stripMargin)

    result should equal(ast(
      """MATCH ()-[a]->()
        |WITH a AS a
        |MATCH ()-[a]->()
        |RETURN a as `b`
      """.stripMargin))
  }

  test("should not inline aggregations: MATCH (a)-[r]->() WITH a, count(r) as b RETURN b as `b`") {
    val result = projectionInlinedAst(
      """MATCH (a)-[r]->()
        |WITH a, count(r) as b
        |RETURN b as `b`
      """.stripMargin)

    result should equal(ast(
      """MATCH (a)-[r]->()
        |WITH a, count(r) as b
        |RETURN b as `b`
      """.stripMargin))
  }

  test("should not inline aggregations: MATCH (a)-[r]->() RETURN a, count(r) as `b`") {
    val result = projectionInlinedAst(
      """MATCH (a)-[r]->()
        |RETURN a, count(r) as `b`
      """.stripMargin)

    result should equal(ast(
      """MATCH (a)-[r]->()
        |RETURN a, count(r) as `b`
      """.stripMargin))
  }

  test("should not inline identifiers which are reused multiple times: WITH 1 as n WITH 2 AS n RETURN n") {
    intercept[AssertionError](projectionInlinedAst(
      """WITH 1 as n
        |WITH 2 AS n
        |RETURN n
      """.stripMargin))
  }

  test("should inline same identifier across multiple WITH clauses, case #1: WITH 1 as n WITH n+1 AS n RETURN n => RETURN 1+1 as n") {
    intercept[AssertionError](projectionInlinedAst(
      """WITH 1 as n
        |WITH n+1 AS n
        |RETURN n
      """.stripMargin))
  }

  test("should inline same identifier across multiple WITH clauses, case #2: WITH 1 as n WITH n+2 AS m WITH n + m as n RETURN n => RETURN 1+1+2 as n") {
    intercept[AssertionError](projectionInlinedAst(
      """WITH 1 as n
        |WITH n+2 AS m
        |WITH n + m as n
        |RETURN n
      """.stripMargin))
  }

  test("should not inline identifiers which cannot be inlined when they are shadowed later on: WITH 1 as n MATCH (n) WITH 2 AS n RETURN n => WITH 1 as n MATCH (n) RETURN 2 as n") {
    intercept[AssertionError](projectionInlinedAst(
      """WITH 1 as n
        |MATCH (n)
        |WITH 2 AS n
        |RETURN n
      """.stripMargin))
  }

  // FIXME: 2014-4-30 Stefan: This is not yet supported by the inline rewriter
  test("should refuse to inline queries containing update clauses by throwing CantHandleQueryException") {
    evaluating {
      projectionInlinedAst(
        """CREATE (n)
          |RETURN n
        """.stripMargin)
    } should produce[CantHandleQueryException]
  }

  test("MATCH n WITH n.prop AS x WITH x LIMIT 10 RETURN x") {
    val result = projectionInlinedAst(
      """MATCH n
        |WITH n.prop AS x
        |WITH x LIMIT 10
        |RETURN x
      """.stripMargin)

    result should equal(ast(
      """MATCH n
        |WITH n AS n
        |WITH n AS n LIMIT 10
        |RETURN n.prop AS x
      """.stripMargin))
  }

  test("MATCH (a:Start) WITH a.prop AS property, count(*) AS count MATCH (b) WHERE id(b) = property RETURN b") {
    val result = projectionInlinedAst(
      """MATCH (a:Start)
        |WITH a.prop AS property, count(*) AS count
        |MATCH (b) WHERE id(b) = property
        |RETURN b
      """.stripMargin)

    result should equal(ast(
      """MATCH (a:Start)
        |WITH a.prop AS property, count(*) AS `count`
        |MATCH (b) WHERE id(b) = property
        |RETURN b AS `b`
      """.stripMargin))
  }

  test("removes unneeded projection") {
    val query =
      """MATCH (owner)
        |WITH owner, COUNT(*) AS xyz
        |WITH owner, xyz > 0 as collection
        |WHERE (owner)--()
        |RETURN owner
      """.stripMargin
    val result = projectionInlinedAst(query)

    result should equal(parser.parse(
      """MATCH (owner)
        |WITH owner AS `owner`, COUNT(*) AS xyz
        |WITH owner AS `owner`, xyz AS `xyz`
        |WITH owner AS `owner`, xyz AS `xyz`, owner AS `owner`
        |WHERE (owner)--()
        |WITH xyz AS `xyz`, owner AS `owner`
        |RETURN owner AS `owner`
      """.stripMargin))
  }

  test("WITH 1 as b RETURN b") {
    val result = projectionInlinedAst(
      """WITH 1 as b
        |RETURN b
      """.stripMargin)

    result should equal(ast(
      """_PRAGMA WITH NONE
        |RETURN 1 AS b
      """.stripMargin))
  }

  test("match n where id(n) IN [0,1,2,3] with n.division AS `n.division`, max(n.age) AS `max(n.age)` with `n.division` AS `n.division`, `max(n.age)` AS `max(n.age)` RETURN `n.division` AS `n.division`, `max(n.age)` AS `max(n.age)` order by `max(n.age)`") {
    val result = projectionInlinedAst(
      """match n where id(n) IN [0,1,2,3]
        |with n.division AS `n.division`, max(n.age) AS `max(n.age)`
        |with `n.division` AS `n.division`, `max(n.age)` AS `max(n.age)`
        |RETURN `n.division` AS `n.division`, `max(n.age)` AS `max(n.age)` order by `max(n.age)`
      """.stripMargin)

    // TODO: this is a temporary solution we should rethink how to generated fresh ids on windows
    val freshIdName = if (SystemUtils.IS_OS_WINDOWS) "`  FRESHID197`" else "`  FRESHID194`"
    result should equal(ast(
      s"""match n where id(n) IN [0,1,2,3]
        |with n.division AS `n.division`, max(n.age) AS `max(n.age)`
        |with `n.division` AS `n.division`, `max(n.age)` AS `max(n.age)`
        |with `n.division` AS `n.division`, `max(n.age)` AS $freshIdName order by $freshIdName
        |RETURN `n.division` AS `n.division`, $freshIdName AS `max(n.age)`
      """.stripMargin))
  }

  test("should not inline expressions used many times: WITH 1 as a MATCH (a) WHERE a.prop = x OR a.bar > x RETURN a, x => WITH 1 as a MATCH (a) WHERE a.prop = x OR a.bar > x RETURN a, x") {
    val result = projectionInlinedAst(
      """WITH 1 as x
        |MATCH (a) WHERE a.prop = x OR a.bar > x
        |RETURN a, x""".stripMargin)

    result should equal(ast(
      """WITH 1 as x
        |MATCH (a) WHERE a.prop = x OR a.bar > x
        |RETURN a, x""".stripMargin))
  }

  test("should not inline relationship identifiers if not inlinging expressions") {
   val result = projectionInlinedAst("MATCH (u)-[r1]->(v) WITH r1 AS r2 MATCH (a)-[r2]->(b) RETURN r2 AS rel")

    result should equal(ast("MATCH (u)-[r1]->(v) WITH r1 AS r2 MATCH (a)-[r2]->(b) RETURN r2 AS rel"))
  }

  private def projectionInlinedAst(queryText: String) = ast(queryText).endoRewrite(inlineProjections)

  private def ast(queryText: String) = {
    val parsed = parser.parse(queryText)
    val mkException = new SyntaxExceptionCreator(queryText, Some(pos))
    val normalized = parsed.endoRewrite(inSequence(normalizeReturnClauses(mkException), normalizeWithClauses(mkException)))
    val checkResult = normalized.semanticCheck(SemanticState.clean)
    normalized.endoRewrite(inSequence(expandStar(checkResult.state)))
  }
}

