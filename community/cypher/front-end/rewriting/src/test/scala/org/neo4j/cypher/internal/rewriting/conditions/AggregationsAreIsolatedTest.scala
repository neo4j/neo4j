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
package org.neo4j.cypher.internal.rewriting.conditions

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class AggregationsAreIsolatedTest extends CypherFunSuite with AstConstructionTestSupport {

  private val condition: Any => Seq[String] = aggregationsAreIsolated(_)(CancellationChecker.NeverCancelled)

  test("happy when aggregation are top level in expressions") {
    val ast = CountStar() _

    condition(ast) shouldBe empty
  }

  test("unhappy when aggregation is sub-expression of the expressions") {
    val ast = equals(CountStar() _, literalUnsignedInt(42))

    condition(ast) should equal(Seq(s"Expression $ast contains child expressions which are aggregations"))
  }

  test("unhappy when aggregations are both top-level and sub-expression of the expression") {
    val innerEquals = equals(CountStar() _, literalUnsignedInt(42))
    val ast = count(innerEquals)

    condition(ast) should equal(Seq(s"Expression $innerEquals contains child expressions which are aggregations"))
  }

  test("happy when aggregations are in Exists Expressions inside an Expression") {
    val fe = ExistsExpression(
      singleQuery(with_(CountStar()(pos) as "x"), match_(nodePat(Some("n"))), return_(varFor("n").as("n")))
    )(pos, None, None)
    val l = listOf(fe)

    condition(l) shouldBe empty
  }

  test("unhappy when aggregations and Exists Expressions inside an Expression") {
    val fe = ExistsExpression(
      singleQuery(match_(nodePat(Some("x"))), return_(varFor("n").as("n")))
    )(pos, None, None)

    val l = listOf(fe, CountStar() _)

    condition(l) should equal(Seq(s"Expression $l contains child expressions which are aggregations"))
  }

  test("unhappy when aggregations and Exists Expressions inside an Expression 2") {
    val fe = ExistsExpression(
      singleQuery(match_(nodePat(Some("x"))), return_(varFor("n").as("n")))
    )(pos, None, None)

    val l = listOf(CountStar() _, fe)

    condition(l) should equal(Seq(s"Expression $l contains child expressions which are aggregations"))
  }

  test("happy when aggregations are in Count Expressions inside an Expression") {
    val fe = CountExpression(
      singleQuery(with_(CountStar()(pos) as "x"), match_(nodePat(Some("n"))), return_(varFor("n").as("n")))
    )(pos, None, None)
    val l = listOf(fe)

    condition(l) shouldBe empty
  }

  test("unhappy when aggregations and Count Expressions inside an Expression") {
    val fe = CountExpression(
      singleQuery(match_(nodePat(Some("x"))), return_(varFor("n").as("n")))
    )(pos, None, None)

    val l = listOf(fe, CountStar() _)

    condition(l) should equal(Seq(s"Expression $l contains child expressions which are aggregations"))
  }

  test("unhappy when aggregations and Count Expressions inside an Expression 2") {
    val fe = CountExpression(
      singleQuery(match_(nodePat(Some("x"))), return_(varFor("n").as("n")))
    )(pos, None, None)

    val l = listOf(CountStar() _, fe)

    condition(l) should equal(Seq(s"Expression $l contains child expressions which are aggregations"))
  }

  test("happy when aggregations are in Collect Expressions inside an Expression") {
    val fe = CollectExpression(
      singleQuery(with_(CountStar()(pos) as "x"), match_(nodePat(Some("n"))), return_(varFor("n").as("n")))
    )(pos, None, None)
    val l = listOf(fe)

    condition(l) shouldBe empty
  }

  test("unhappy when aggregations and Collect Expressions inside an Expression") {
    val fe = CollectExpression(
      singleQuery(match_(nodePat(Some("x"))), return_(varFor("n").as("n")))
    )(pos, None, None)

    val l = listOf(fe, CountStar() _)

    condition(l) should equal(Seq(s"Expression $l contains child expressions which are aggregations"))
  }

  test("unhappy when aggregations and Collect Expressions inside an Expression 2") {
    val fe = CollectExpression(
      singleQuery(match_(nodePat(Some("x"))), return_(varFor("n").as("n")))
    )(pos, None, None)

    val l = listOf(CountStar() _, fe)

    condition(l) should equal(Seq(s"Expression $l contains child expressions which are aggregations"))
  }
}
