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
package org.neo4j.cypher.internal.frontend.label_expressions

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.SemanticAnalysisTestSuiteWithDefaultQuery
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.TestName

abstract class LabelExpressionSemanticAnalysisTestSuiteWithChangeStatement(statement: ChangeStatement)
    extends SemanticAnalysisTestSuiteWithDefaultQuery
    with TestName {

  override def defaultQuery: String = s"MATCH (n), (m) $statement $testName"

  // The position of the SET/REMOVE clause in the query
  protected val errorPosition: InputPosition = InputPosition(15, 1, 16)

  protected val errorPositionDynamicLabels: InputPosition =
    if (statement == ChangeStatement.SET) InputPosition(19, 1, 20) else InputPosition(22, 1, 23)

  protected def multipleAssignmentErrorMessage(replacement: String) =
    s"It is not supported to use the `IS` keyword together with multiple labels in `$statement`. Rewrite the expression as `$replacement`."

  protected def notSupportedErrorMessage() = {
    val clauseAction = if (statement == ChangeStatement.SET) "Setting" else "Removing"
    s"$clauseAction labels or properties dynamically is not supported."
  }

  test("n:A") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("n IS A") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("n:A:B:C") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("n:A, n:B") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("n IS A, n IS B") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("n:A, n IS B") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("n IS A, n:B") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("n IS A:B") {
    runSemanticAnalysis().errors shouldEqual Seq(
      SemanticError(multipleAssignmentErrorMessage("n IS A, n IS B"), errorPosition)
    )
  }

  test("n IS A, m:A:B") {
    runSemanticAnalysis().errors shouldEqual Seq(
      SemanticError(multipleAssignmentErrorMessage("n IS A, m IS A, m IS B"), errorPosition)
    )
  }

  // Dynamic Labels
  test("n:$(\"Label1\")") {
    runSemanticAnalysis().errors shouldEqual Seq(
      SemanticError(
        notSupportedErrorMessage(),
        errorPositionDynamicLabels
      )
    )
  }

  test("n IS $(\"Label1\")") {
    runSemanticAnalysis().errors shouldEqual Seq(
      SemanticError(
        notSupportedErrorMessage(),
        errorPositionDynamicLabels
      )
    )
  }

  test("n:A:$(\"Label1\")") {
    runSemanticAnalysis().errors shouldEqual Seq(
      SemanticError(
        notSupportedErrorMessage(),
        errorPositionDynamicLabels
      )
    )
  }

  test("n IS A, m:$(\"Label1\"):B:$(\"Label2\")") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors shouldEqual Seq(
      SemanticError(
        multipleAssignmentErrorMessage("n IS A, m IS $(\"Label1\"), m IS B, m IS $(\"Label2\")"),
        errorPosition
      )
    )
  }

  test("n:$(\"Label2\")") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors shouldBe empty
  }

  test("n IS $(\"Label2\")") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors shouldBe empty
  }

  test("n:A:$(\"Label2\")") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors shouldBe empty
  }

  test("m:$(\"\")") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors shouldEqual Seq(
      SemanticError(
        "'' is not a valid token name. Token names cannot be empty or contain any null-bytes.",
        errorPositionDynamicLabels
      )
    )
  }

  test("n:$(1)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors shouldEqual Seq(
      SemanticError(
        "Type mismatch: expected String or List<String> but was Integer",
        if (statement == ChangeStatement.SET) InputPosition(23, 1, 24) else InputPosition(26, 1, 27)
      )
    )
  }

  test("n:$([1])") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors shouldEqual Seq(
      SemanticError(
        "Type mismatch: expected String or List<String> but was List<Integer>",
        if (statement == ChangeStatement.SET) InputPosition(23, 1, 24) else InputPosition(26, 1, 27)
      )
    )
  }

  test("n:$(point({x : 1, y: 1}))") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors shouldEqual Seq(
      SemanticError(
        "Type mismatch: expected String or List<String> but was Point",
        if (statement == ChangeStatement.SET) InputPosition(23, 1, 24) else InputPosition(26, 1, 27)
      )
    )
  }

  test("n:$([\"Label1\", \"Label2\"])") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.DynamicProperties).errors shouldBe empty
  }
}

sealed trait ChangeStatement

object ChangeStatement {
  case object SET extends ChangeStatement
  case object REMOVE extends ChangeStatement
}

class LabelExpressionInSetSemanticAnalysisTest
    extends LabelExpressionSemanticAnalysisTestSuiteWithChangeStatement(ChangeStatement.SET) {

  test("n IS A, n:B, n.prop = 1") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("n:A, n.prop = 1, m += $map, m:B:C, n IS B:C") {
    runSemanticAnalysis().errors shouldEqual Seq(
      SemanticError(
        multipleAssignmentErrorMessage("n IS A, n.prop = 1, m += $map, m IS B, m IS C, n IS B, n IS C"),
        errorPosition
      )
    )
  }
}

class LabelExpressionInRemoveSemanticAnalysisTest
    extends LabelExpressionSemanticAnalysisTestSuiteWithChangeStatement(ChangeStatement.REMOVE) {

  test("n IS A, n:B, n.prop") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("n:A, n.prop, m:B:C, n IS B:C") {
    runSemanticAnalysis().errors shouldEqual Seq(
      SemanticError(
        multipleAssignmentErrorMessage("n IS A, n.prop, m IS B, m IS C, n IS B, n IS C"),
        errorPosition
      )
    )
  }
}
