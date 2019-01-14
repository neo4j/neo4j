/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.rewriting

import org.neo4j.cypher.internal.v3_5.ast._
import org.neo4j.cypher.internal.v3_5.ast.semantics.{SemanticState, SyntaxExceptionCreator}
import org.neo4j.cypher.internal.v3_5.expressions.{Add, SignedDecimalIntegerLiteral, Variable}
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.{expandStar, inlineProjections, normalizeWithAndReturnClauses}
import org.neo4j.cypher.internal.v3_5.util.helpers.StringHelper.RichString
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.util.{InternalException, inSequence}

class InlineProjectionsTest extends CypherFunSuite with AstRewritingTestSupport {

  test("should inline: MATCH a, b, c WITH c AS c, b AS a RETURN c") {
    val result = projectionInlinedAst(
      """MATCH (a), (b), (c)
        |WITH c AS c, b AS d
        |RETURN c
      """.stripMargin)

    result should equal(ast(
      """MATCH (a), (b), (c)
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
        |WITH a AS a WHERE true
        |RETURN a AS a
      """.stripMargin))
  }

  test("should inline pattern variables when possible") {
    val result = projectionInlinedAst(
      """MATCH (n)
        |WITH n
        |MATCH (n)-->(x)
        |RETURN x
      """.stripMargin)

    result should equal(ast(
      """MATCH (n)
        |WITH n
        |MATCH (n)-->(x)
        |RETURN x""".stripMargin))
  }

  test("should inline: WITH 1 AS x RETURN 1 + x => ... RETURN 1 + 1") {
    val result = projectionInlinedAst(
      """WITH 1 AS x
        |RETURN 1 + x
      """.stripMargin)

    // Changing the assert to use the AST, since an empty WITH cannot be written in CYPHER
    result should equal(Query(None,
      SingleQuery(List(
        With(distinct = false, ReturnItems(includeExisting = false, items = Vector())(pos), None, None, None, None)(pos),
        Return(distinct = false, returnItems = ReturnItems(includeExisting = false, List(AliasedReturnItem(Add(SignedDecimalIntegerLiteral("1")(pos), SignedDecimalIntegerLiteral("1")(pos))(pos), Variable("1 + x")(pos))(pos)))(pos), orderBy = None, skip = None, limit = None, excludedNames = Set())(pos)
      ))(pos))(pos))
  }

  test("should not inline aggregations: WITH 1 as b WITH DISTINCT b AS c RETURN c => WITH DISTINCT 1 AS c RETURN c AS c") {
    val result = projectionInlinedAst(
      """WITH 1 as b
        |WITH DISTINCT b AS c
        |RETURN c
      """.stripMargin)

    // Changing the assert to use the AST, since an empty WITH cannot be written in CYPHER
    result should equal(Query(None,
      SingleQuery(List(
        With(distinct = false, ReturnItems(includeExisting = false, items = Vector())(pos), None, None, None, None)(pos),
        With(distinct = true, ReturnItems(includeExisting = false, items = List(AliasedReturnItem(SignedDecimalIntegerLiteral("1")(pos), Variable("c")(pos))(pos)))(pos), None, None, None, None)(pos),
        Return(distinct = false, returnItems = ReturnItems(includeExisting = false, items = List(AliasedReturnItem(Variable("c")(pos), Variable("c")(pos))(pos)))(pos), orderBy = None, skip = None, limit = None, excludedNames = Set())(pos)
      ))(pos))(pos))
  }

  test("should not inline variables into patterns: WITH {node} as a MATCH (a) RETURN a => WITH {node} as a MATCH (a) RETURN a AS `a`") {
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

  test("should inline multiple variables across multiple WITH clauses: WITH 1 as n WITH n+1 AS m RETURN m => RETURN 1+1 as m") {
    val result = projectionInlinedAst(
      """WITH 1 as n
        |WITH n + 1 AS m
        |RETURN m
      """.stripMargin)

    // Changing the assert to use the AST, since an empty WITH cannot be written in CYPHER
    result should equal(Query(None,
      SingleQuery(List(
        With(distinct = false, ReturnItems(includeExisting = false, items = Vector())(pos), None, None, None, None)(pos),
        With(distinct = false, ReturnItems(includeExisting = false, items = Vector())(pos), None, None, None, None)(pos),
        Return(distinct = false, returnItems = ReturnItems(includeExisting = false, items = List(AliasedReturnItem(Add(SignedDecimalIntegerLiteral("1")(pos),SignedDecimalIntegerLiteral("1")(pos))(pos), Variable("m")(pos))(pos)))(pos), orderBy = None, skip = None, limit = None, excludedNames = Set())(pos)
      ))(pos))(pos))
  }

  test("should inline node patterns: MATCH (a) WITH a as b MATCH (b) RETURN b => MATCH (a) WITH a AS a MATCH (a) RETURN a as `b`") {
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

  test("should inline relationship patterns: MATCH ()-[a]->() WITH a as b MATCH ()-[b]->() RETURN b => MATCH ()-[a]->() WITH a AS a MATCH ()-[a]->() RETURN a as `b`") {
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

  test("should not inline variables which are reused multiple times: WITH 1 as n WITH 2 AS n RETURN n") {
    intercept[AssertionError](projectionInlinedAst(
      """WITH 1 as n
        |WITH 2 AS n
        |RETURN n
      """.stripMargin))
  }

  test("should inline same variable across multiple WITH clauses, case #1: WITH 1 as n WITH n+1 AS n RETURN n => RETURN 1+1 as n") {
    intercept[AssertionError](projectionInlinedAst(
      """WITH 1 as n
        |WITH n+1 AS n
        |RETURN n
      """.stripMargin))
  }

  test("should inline same variable across multiple WITH clauses, case #2: WITH 1 as n WITH n+2 AS m WITH n + m as n RETURN n => RETURN 1+1+2 as n") {
    intercept[AssertionError](projectionInlinedAst(
      """WITH 1 as n
        |WITH n+2 AS m
        |WITH n + m as n
        |RETURN n
      """.stripMargin))
  }

  test("should not inline variables which cannot be inlined when they are shadowed later on: WITH 1 as n MATCH (n) WITH 2 AS n RETURN n => WITH 1 as n MATCH (n) RETURN 2 as n") {
    intercept[AssertionError](projectionInlinedAst(
      """WITH 1 as n
        |MATCH (n)
        |WITH 2 AS n
        |RETURN n
      """.stripMargin))
  }

  test("should refuse to inline queries containing update clauses by throwing CantHandleQueryException") {
    evaluating {
      projectionInlinedAst(
        """CREATE (n)
          |RETURN n
        """.stripMargin)
    } should produce[InternalException]
  }

  test("MATCH (n) WITH n.prop AS x WITH x LIMIT 10 RETURN x") {
    val result = projectionInlinedAst(
      """MATCH (n)
        |WITH n.prop AS x
        |WITH x LIMIT 10
        |RETURN x
      """.stripMargin)

    result should equal(ast(
      """MATCH (n)
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
        |WHERE (owner)--()
        |RETURN owner AS `owner`
      """.stripMargin))
  }

  test("WITH 1 as b RETURN b") {
    val result = projectionInlinedAst(
      """WITH 1 as b
        |RETURN b
      """.stripMargin)

    // Changing the assert to use the AST, since an empty WITH cannot be written in CYPHER
    result should equal(Query(None,
      SingleQuery(List(
        With(distinct = false, ReturnItems(includeExisting = false, items = Vector())(pos), None, None, None, None)(pos),
        Return(distinct = false, returnItems = ReturnItems(includeExisting = false, List(AliasedReturnItem(SignedDecimalIntegerLiteral("1")(pos), Variable("b")(pos))(pos)))(pos), orderBy = None, skip = None, limit = None, excludedNames = Set())(pos)
      ))(pos))(pos))
  }

  test("match (n) where id(n) IN [0,1,2,3] with n.division AS `n.division`, max(n.age) AS `max(n.age)` with `n.division` AS `n.division`, `max(n.age)` AS `max(n.age)` RETURN `n.division` AS `n.division`, `max(n.age)` AS `max(n.age)` order by `max(n.age)`") {
    val result = projectionInlinedAst(
      """MATCH (n) WHERE id(n) IN [0,1,2,3]
        |WITH n.division AS `n.division`, max(n.age) AS `max(n.age)`
        |WITH `n.division` AS `n.division`, `max(n.age)` AS `max(n.age)`
        |RETURN `n.division` AS `n.division`, `max(n.age)` AS `max(n.age)` ORDER BY `max(n.age)`
      """.stripMargin.fixNewLines)

    result should equal(ast(
      s"""MATCH (n) WHERE id(n) IN [0,1,2,3]
         |WITH n.division AS `n.division`, max(n.age) AS `max(n.age)`
         |WITH `n.division` AS `n.division`, `max(n.age)` AS `max(n.age)`
         |RETURN `n.division` AS `n.division`, `max(n.age)` AS `max(n.age)` ORDER BY `max(n.age)`
      """.stripMargin.fixNewLines))
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

  test("should not inline relationship variables if not inlinging expressions") {
   val result = projectionInlinedAst("MATCH (u)-[r1]->(v) WITH r1 AS r2 MATCH (a)-[r2]->(b) RETURN r2 AS rel")

    result should equal(ast("MATCH (u)-[r1]->(v) WITH r1 AS r2 MATCH (a)-[r2]->(b) RETURN r2 AS rel"))
  }

  private def projectionInlinedAst(queryText: String) = ast(queryText).endoRewrite(inlineProjections)

  private def ast(queryText: String) = {
    val parsed = parser.parse(queryText)
    val mkException = new SyntaxExceptionCreator(queryText, Some(pos))
    val normalized = parsed.endoRewrite(inSequence(normalizeWithAndReturnClauses(mkException)))
    val checkResult = normalized.semanticCheck(SemanticState.clean)
    normalized.endoRewrite(inSequence(expandStar(checkResult.state)))
  }
}

