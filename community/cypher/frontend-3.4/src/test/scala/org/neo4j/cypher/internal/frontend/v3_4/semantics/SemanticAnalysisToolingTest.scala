/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.semantics

import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.apa.v3_4.symbols._

class SemanticAnalysisToolingTest extends CypherFunSuite {

  val expression = DummyExpression(CTAny)

  val toTest = new SemanticAnalysisTooling {}

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
    result.errors.head.msg should equal ("Type mismatch: expected String but was Integer or Node")
  }

  test("shouldRaiseTypeErrorWithCustomMessageWhenMismatchBetweenSpecifiedTypeAndExpectedType") {
    val result = (
      toTest.specifyType(CTNode | CTInteger, expression) chain
        toTest.expectType(CTString.covariant, expression,
          (expected: String, existing: String) => s"lhs was $expected yet rhs was $existing")
      )(SemanticState.clean)

    result.errors should have size 1
    result.errors.head.position should equal(expression.position)
    toTest.types(expression)(result.state) shouldBe empty

    assert(result.errors.size === 1)
    assert(result.errors.head.position === expression.position)
    assert(result.errors.head.msg == "Type mismatch: lhs was String yet rhs was Integer or Node")
    assert(toTest.types(expression)(result.state).isEmpty)
  }
}
