/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ir.helpers

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.helpers.LabelExpressionEvaluator.NodesToCheckOverlap
import org.neo4j.cypher.internal.ir.helpers.LabelExpressionEvaluator.labelAndPropertyExpressionEvaluator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.annotation.tailrec

class LabelExpressionEvaluatorTest extends CypherFunSuite with AstConstructionTestSupport {

  private val labelA: HasLabels = hasLabels("n", "A")
  private val labelB: HasLabels = hasLabels("n", "B")

  test("should work for expression: A") {
    val expr = labelA
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")

    def testLabel(labels: Set[String], expectedResult: Boolean): Unit = withClue(labels) {
      labelAndPropertyExpressionEvaluator(expr, nodesToCheckOverlap, labels).result shouldBe Some(expectedResult)
    }

    testLabel(Set("A"), expectedResult = true)
    testLabel(Set("B"), expectedResult = false)
    testLabel(Set("A", "B"), expectedResult = true)
  }

  test("should work for expression: !A") {
    val expr = not(labelA)
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")

    def testNot(labels: Set[String], expectedResult: Boolean): Unit = withClue(labels) {
      labelAndPropertyExpressionEvaluator(expr, nodesToCheckOverlap, labels).result shouldBe Some(expectedResult)
    }

    testNot(Set("A"), expectedResult = false)
    testNot(Set("B"), expectedResult = true)
    testNot(Set("A", "B"), expectedResult = false)
  }

  test("should work for expression: A&B") {
    val exprAnd = and(labelA, labelB)
    val exprAnds = ands(labelA, labelB)
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")

    def testAnd(labels: Set[String], expectedResult: Boolean): Unit = withClue(labels) {
      labelAndPropertyExpressionEvaluator(exprAnd, nodesToCheckOverlap, labels).result shouldBe Some(expectedResult)
      labelAndPropertyExpressionEvaluator(exprAnds, nodesToCheckOverlap, labels).result shouldBe Some(expectedResult)
    }

    testAnd(Set("A"), expectedResult = false)
    testAnd(Set("B"), expectedResult = false)
    testAnd(Set("C"), expectedResult = false)
    testAnd(Set("A", "B"), expectedResult = true)
    testAnd(Set("C", "A", "B"), expectedResult = true)
  }

  test("should work for expression: A|B") {
    val exprOr = or(labelA, labelB)
    val exprOrs = ors(labelA, labelB)
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")

    def testOr(labels: Set[String], expectedResult: Boolean): Unit = withClue(labels) {
      labelAndPropertyExpressionEvaluator(exprOrs, nodesToCheckOverlap, labels).result shouldBe Some(expectedResult)
      labelAndPropertyExpressionEvaluator(exprOr, nodesToCheckOverlap, labels).result shouldBe Some(expectedResult)
    }

    testOr(Set("A"), expectedResult = true)
    testOr(Set("B"), expectedResult = true)
    testOr(Set("C"), expectedResult = false)
    testOr(Set("A", "B"), expectedResult = true)
    testOr(Set("C", "A", "B"), expectedResult = true)
  }

  test("should work for expression: A^B") {
    val exprXor = xor(labelA, labelB)
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")

    def testXor(labels: Set[String], expectedResult: Boolean): Unit = withClue(labels) {
      labelAndPropertyExpressionEvaluator(exprXor, nodesToCheckOverlap, labels).result shouldBe Some(expectedResult)
    }

    testXor(Set("A"), expectedResult = true)
    testXor(Set("B"), expectedResult = true)
    testXor(Set("A", "C"), expectedResult = true)
    testXor(Set("C"), expectedResult = false)
    testXor(Set("A", "B"), expectedResult = false)
    testXor(Set("C", "A", "B"), expectedResult = false)
  }

  test("should work for expression: A=B") {
    val exprEquals = equals(labelA, labelB)
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")

    def testEquals(labels: Set[String], expectedResult: Boolean): Unit = withClue(labels) {
      labelAndPropertyExpressionEvaluator(exprEquals, nodesToCheckOverlap, labels).result shouldBe Some(expectedResult)
    }

    testEquals(Set("A"), expectedResult = false)
    testEquals(Set("B"), expectedResult = false)
    testEquals(Set("C"), expectedResult = true)
    testEquals(Set("A", "B"), expectedResult = true)
    testEquals(Set("C", "A", "B"), expectedResult = true)
  }

  test("should work for expression: A!=B") {
    val exprNotEquals = notEquals(labelA, labelB)
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")

    def testNotEquals(labels: Set[String], expectedResult: Boolean): Unit = withClue(labels) {
      labelAndPropertyExpressionEvaluator(exprNotEquals, nodesToCheckOverlap, labels).result shouldBe Some(
        expectedResult
      )
    }

    testNotEquals(Set("A"), expectedResult = true)
    testNotEquals(Set("B"), expectedResult = true)
    testNotEquals(Set("C"), expectedResult = false)
    testNotEquals(Set("A", "B"), expectedResult = false)
    testNotEquals(Set("C", "A", "B"), expectedResult = false)
  }

  test("should work for expression: !(A=B)|(C^(A&D))") {
    val bigExpr =
      or(
        not(equals(labelA, labelB)),
        xor(hasLabels("n", "C"), ands(labelA, hasLabels("n", "D")))
      )
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")

    def testNotEquals(labels: Set[String], expectedResult: Boolean): Unit = withClue(labels) {
      labelAndPropertyExpressionEvaluator(bigExpr, nodesToCheckOverlap, labels).result shouldBe Some(expectedResult)
    }

    testNotEquals(Set("A"), expectedResult = true)
    testNotEquals(Set("B"), expectedResult = true)
    testNotEquals(Set("C"), expectedResult = true)
    testNotEquals(Set("D"), expectedResult = false)
    testNotEquals(Set("A", "B"), expectedResult = false)
    testNotEquals(Set("A", "B", "D"), expectedResult = true)
    testNotEquals(Set("A", "B", "C"), expectedResult = true)
    testNotEquals(Set("A", "B", "C", "D"), expectedResult = false)
  }

  test("should return None when unknown node") {
    val expr = hasLabels("unknownNode", "A")
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")
    val labels = Set("A")

    val resultExpr = labelAndPropertyExpressionEvaluator(expr, nodesToCheckOverlap, labels).result

    resultExpr shouldBe None
  }

  test("should return None when unknown expression") {
    val expr = lessThan(prop("n", "prop1"), prop("n", "prop2"))
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")
    val labels = Set("A")

    val resultExpr = labelAndPropertyExpressionEvaluator(expr, nodesToCheckOverlap, labels).result

    resultExpr shouldBe None
  }

  test("labelExpressionEvaluator is stack-safe") {
    @tailrec
    def buildExpression(i: Int, expression: Expression): Expression =
      if (i <= 0) expression else buildExpression(i - 1, Not(expression)(InputPosition.NONE))

    val n = Variable("n")(InputPosition.NONE)
    val label = LabelName("a")(InputPosition.NONE)
    val hasLabels = HasLabels(n, Seq(label))(InputPosition.NONE)
    val labelExpression = buildExpression(10000, hasLabels)

    LabelExpressionEvaluator.labelAndPropertyExpressionEvaluator(
      labelExpression,
      NodesToCheckOverlap(Some("n"), "n"),
      Set("a")
    ).result shouldBe Some(true)
  }
}
