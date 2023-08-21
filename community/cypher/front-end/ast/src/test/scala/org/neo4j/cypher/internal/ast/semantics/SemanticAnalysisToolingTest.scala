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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.DummyExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SemanticAnalysisToolingTest extends CypherFunSuite with AstConstructionTestSupport {

  val expression: Expression = DummyExpression(CTAny)

  val toTest: SemanticAnalysisTooling = new SemanticAnalysisTooling {}

  test("shouldReturnCalculatedType") {
    SemanticState.clean.expressionType(expression).actual should equal(TypeSpec.all)
  }

  test("shouldReturnSpecifiedAndConstrainedTypes") {
    val state = (
      toTest.specifyType(CTNode | CTInteger, expression) chain
        toTest.expectType(CTNumber.covariant, expression)
    )(SemanticState.clean).state

    state.expressionType(expression).actual should equal(CTInteger.invariant)
  }

  test("shouldRaiseTypeErrorWhenMismatchBetweenSpecifiedTypeAndExpectedType") {
    val result = (
      toTest.specifyType(CTNode | CTInteger, expression) chain
        toTest.expectType(CTString.covariant, expression)
    )(SemanticState.clean)

    result.errors should have size 1
    result.errors.head.position should equal(expression.position)
    toTest.types(expression)(result.state) shouldBe empty
    result.errors.head.msg should equal("Type mismatch: expected String but was Integer or Node")
  }

  test("shouldRaiseTypeErrorWithCustomMessageWhenMismatchBetweenSpecifiedTypeAndExpectedType") {
    val result = (
      toTest.specifyType(CTNode | CTInteger, expression) chain
        toTest.expectType(
          CTString.covariant,
          expression,
          (expected: String, existing: String) => s"lhs was $expected yet rhs was $existing"
        )
    )(SemanticState.clean)

    result.errors should have size 1
    result.errors.head.position should equal(expression.position)
    toTest.types(expression)(result.state) shouldBe empty

    assert(result.errors.size === 1)
    assert(result.errors.head.position === expression.position)
    assert(result.errors.head.msg == "Type mismatch: lhs was String yet rhs was Integer or Node")
    assert(toTest.types(expression)(result.state).isEmpty)
  }

  test("should infer the right type for arguments of Ands") {
    // Given
    val varExpr = varFor("x")
    val expression = ands(varExpr)

    // When
    val checkResult = SemanticExpressionCheck.check(SemanticContext.Simple, expression).apply(SemanticState.clean)

    // Then
    checkResult.state.typeTable(varExpr).expected should be(Some(CTBoolean.covariant))
  }

  test("withState should work") {
    val initialState = SemanticState.clean
    val stateForCheck = initialState.declareVariable(varFor("x"), CTNode.invariant).getOrElse(fail())

    val error = SemanticError("some error", pos)

    val check = toTest.withState(stateForCheck) {
      SemanticCheck.fromFunction { state =>
        state shouldBe stateForCheck
        SemanticCheckResult.error(stateForCheck, error)
      }
    }

    check.run(initialState, SemanticCheckContext.default) shouldBe SemanticCheckResult.error(initialState, error)
  }
}
