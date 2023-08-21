/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.rewriting.AstRewritingTestSupport
import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

class simplifyIterablePredicatesTest extends CypherFunSuite with Matchers with RewriteTest
    with AstRewritingTestSupport {

  test("should rewrite simple any with literal") {
    assertRewrite(
      "WITH [1,2,3] AS list RETURN any(x IN list WHERE x = 2) AS result",
      "WITH [1,2,3] AS list RETURN 2 IN list AS result"
    )
    assertRewrite(
      "WITH [1,2,3] AS list RETURN any(x IN list WHERE 2 = x) AS result",
      "WITH [1,2,3] AS list RETURN 2 IN list AS result"
    )
  }

  test("should rewrite simple any with in and single literal") {
    assertRewrite(
      "WITH [1,2,3] AS list RETURN any(x IN list WHERE x IN [2]) AS result",
      "WITH [1,2,3] AS list RETURN 2 IN list AS result"
    )
  }

  test("should rewrite simple any with variable") {
    assertRewrite(
      "WITH [1,2,3] AS list, 1 AS a RETURN any(x IN list WHERE x = a) AS result",
      "WITH [1,2,3] AS list, 1 AS a RETURN a IN list AS result"
    )
  }

  test("should not rewrite when inner predicate is not equals") {
    assertIsNotRewritten("WITH [1,2,3] AS list RETURN any(x IN list WHERE x > 2) AS result")
    assertIsNotRewritten("WITH [1,2,3] AS list RETURN any(x IN list WHERE 2 < x) AS result")
    assertIsNotRewritten("WITH [1,2,3] AS list RETURN any(x IN list WHERE x IN [1,2]) AS result")
  }

  test("should not rewrite queries that depends on the scope variable on both sides") {
    assertIsNotRewritten("MATCH (n) RETURN any(x IN n.list WHERE x = (2*x)) AS result")
    assertIsNotRewritten("MATCH (n) RETURN any(x IN n.a WHERE x = x.b) AS result")
  }

  test("should not rewrite queries that uses the scope variable in any way other than for equality") {
    assertIsNotRewritten("MATCH (n) RETURN any(x IN n.list WHERE x.a = 1) AS result")
    assertIsNotRewritten("MATCH (n) RETURN any(x IN n.list WHERE 1 = x.a) AS result")
    assertIsNotRewritten("MATCH (n) RETURN any(x IN n.list WHERE size(a) > 0) AS result")
  }

  // The following tests can't be written in RewriteTest style because there is no way to express Not.

  test("should rewrite none(x in list WHERE x = 2) to not(2 IN list)") {
    val expr = noneInList(
      varFor("x"),
      varFor("list"),
      equals(varFor("x"), literal(2))
    )
    rewrite(expr) shouldBe not(in(literal(2), varFor("list")))
  }

  test("should rewrite none(x in [1, 2, 3] WHERE 2 = x) to 2 IN [1, 2, 3]") {
    val expr = noneInList(
      varFor("x"),
      varFor("list"),
      equals(literal(2), varFor("x"))
    )
    rewrite(expr) shouldBe not(in(literal(2), varFor("list")))
  }

  test("should rewrite none(x in list WHERE x = a) to not(a IN list)") {
    val expr = noneInList(
      varFor("x"),
      varFor("list"),
      equals(varFor("x"), varFor("a"))
    )
    rewrite(expr) shouldBe not(in(varFor("a"), varFor("list")))
  }

  test("should not rewrite none(x in list WHERE x > 1)") {
    val expr = noneInList(
      varFor("x"),
      varFor("list"),
      greaterThan(varFor("x"), literalInt(1))
    )
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite none(x in list WHERE x = x.b)") {
    val expr = noneInList(
      varFor("x"),
      varFor("list"),
      equals(varFor("x"), prop(varFor("x"), "a"))
    )
    rewrite(expr) shouldBe expr
  }

  test("should not rewrite none(x in list WHERE x.a = 1)") {
    val expr = noneInList(
      varFor("x"),
      varFor("list"),
      equals(prop(varFor("x"), "a"), literalInt(1))
    )
    rewrite(expr) shouldBe expr
  }

  // the actual auto-parameterization is tested else where
  test("should rewrite any(x in list WHERE x IN $autoList) to $autoList IN list when possible") {
    def expr(innerList: Expression) = anyInList(
      varFor("x"),
      varFor("list"),
      in(varFor("x"), innerList)
    )

    rewrite(expr(autoParameter("autoList", CTList(CTInteger), Some(1)))) shouldBe
      in(containerIndex(autoParameter("autoList", CTList(CTInteger), Some(1)), 0), varFor("list"))
    shouldNotRewrite(expr(autoParameter("autoList", CTList(CTInteger), Some(11))))
    shouldNotRewrite(expr(autoParameter("autoList", CTList(CTInteger), Some(0))))
    shouldNotRewrite(expr(autoParameter("autoList", CTString, Some(1))))
  }

  test("should rewrite any(x in list WHERE x IN $p) to $p IN list when possible") {
    def expr(innerList: Expression) = anyInList(
      varFor("x"),
      varFor("list"),
      in(varFor("x"), innerList)
    )

    rewrite(expr(parameter("p", CTList(CTInteger), Some(1)))) shouldBe
      in(containerIndex(parameter("p", CTList(CTInteger), Some(1)), 0), varFor("list"))
    shouldNotRewrite(expr(parameter("p", CTList(CTInteger), Some(11))))
    shouldNotRewrite(expr(parameter("p", CTList(CTInteger), Some(0))))
    shouldNotRewrite(expr(parameter("p", CTString, Some(1))))
  }

  override def rewriterUnderTest: Rewriter = simplifyIterablePredicates.instance

  private def rewrite(e: Expression): Expression = e.endoRewrite(rewriterUnderTest)

  private def shouldNotRewrite(e: Expression): Assertion = {
    rewrite(e) shouldBe e
  }
}
