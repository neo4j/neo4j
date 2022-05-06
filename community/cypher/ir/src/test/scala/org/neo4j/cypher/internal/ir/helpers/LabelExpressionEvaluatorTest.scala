package org.neo4j.cypher.internal.ir.helpers

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.ir.helpers.LabelExpressionEvaluator.NodesToCheckOverlap
import org.neo4j.cypher.internal.ir.helpers.LabelExpressionEvaluator.labelExpressionEvaluator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LabelExpressionEvaluatorTest extends CypherFunSuite with AstConstructionTestSupport {

  private val labelA: HasLabels = hasLabels("n", "A")
  private val labelB: HasLabels = hasLabels("n", "B")

  test("should work for expression: A") {
    val expr = labelA
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")

    def testLabel(labels: Set[String], expectedResult: Boolean): Unit = {
      labelExpressionEvaluator(expr, nodesToCheckOverlap, labels) shouldBe Some(expectedResult)
    }

    testLabel(Set("A"), expectedResult = true)
    testLabel(Set("B"), expectedResult = false)
    testLabel(Set("A", "B"), expectedResult = true)
  }

  test("should work for expression: !A") {
    val expr = not(labelA)
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")

    def testNot(labels: Set[String], expectedResult: Boolean): Unit = {
      labelExpressionEvaluator(expr, nodesToCheckOverlap, labels) shouldBe Some(expectedResult)
    }

    testNot(Set("A"), expectedResult = false)
    testNot(Set("B"), expectedResult = true)
    testNot(Set("A", "B"), expectedResult = false)
  }

  test("should work for expression: A&B") {
    val exprAnd = and(labelA, labelB)
    val exprAnds = ands(labelA, labelB)
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")

    def testAnd(labels: Set[String], expectedResult: Boolean): Unit = {
      labelExpressionEvaluator(exprAnd, nodesToCheckOverlap, labels) shouldBe Some(expectedResult)
      labelExpressionEvaluator(exprAnds, nodesToCheckOverlap, labels) shouldBe Some(expectedResult)
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

    def testOr(labels: Set[String], expectedResult: Boolean): Unit = {
      labelExpressionEvaluator(exprOrs, nodesToCheckOverlap, labels)
      labelExpressionEvaluator(exprOr, nodesToCheckOverlap, labels)
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

    def testOr(labels: Set[String], expectedResult: Boolean): Unit = {
      labelExpressionEvaluator(exprXor, nodesToCheckOverlap, labels)
    }

    testOr(Set("A"), expectedResult = true)
    testOr(Set("B"), expectedResult = true)
    testOr(Set("A, C"), expectedResult = true)
    testOr(Set("C"), expectedResult = false)
    testOr(Set("A", "B"), expectedResult = false)
    testOr(Set("C", "A", "B"), expectedResult = false)
  }

  test("should work for expression: A=B") {
    val exprEquals = equals(labelA, labelB)
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")

    def testEquals(labels: Set[String], expectedResult: Boolean): Unit = {
      labelExpressionEvaluator(exprEquals, nodesToCheckOverlap, labels)
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

    def testNotEquals(labels: Set[String], expectedResult: Boolean): Unit = {
      labelExpressionEvaluator(exprNotEquals, nodesToCheckOverlap, labels)
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

    def testNotEquals(labels: Set[String], expectedResult: Boolean): Unit = {
      labelExpressionEvaluator(bigExpr, nodesToCheckOverlap, labels)
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

    val resultExpr = labelExpressionEvaluator(expr, nodesToCheckOverlap, labels)

    resultExpr shouldBe None
  }

  test("should return None when unknown expression") {
    val expr = and(labelA, prop("n", "prop"))
    val nodesToCheckOverlap = NodesToCheckOverlap(None, "n")
    val labels = Set("A")

    val resultExpr = labelExpressionEvaluator(expr, nodesToCheckOverlap, labels)

    resultExpr shouldBe None
  }
}
