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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{DummyPosition, SemanticCheckResult, SemanticError, SemanticState}

class ReduceExpressionTest extends CypherFunSuite {

  test("shouldEvaluateReduceExpressionWithTypedVariables") {
    val error = SemanticError("dummy error", DummyPosition(10))

    val reduceExpression = new DummyExpression(CTAny, DummyPosition(10)) {
      override def semanticCheck(ctx: SemanticContext) = s => {
        s.symbolTypes("x") should equal(CTString.invariant)
        s.symbolTypes("y") should equal(CTInteger.invariant)
        (this.specifyType(CTString) chain error)(s)
      }
    }

    val filter = ReduceExpression(
      accumulator = Variable("x")(DummyPosition(2)),
      init = DummyExpression(CTString),
      variable = Variable("y")(DummyPosition(6)),
      list = DummyExpression(CTList(CTInteger)),
      expression = reduceExpression
    )(DummyPosition(0))

    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors should equal(Seq(error))
    result.state.symbol("x") shouldBe empty
    result.state.symbol("y") shouldBe empty
  }

  test("shouldReturnMinimalTypeOfAccumulatorAndReduceFunction") {
    val initType = CTString.covariant | CTFloat.covariant
    val listType = CTList(CTInteger)

    val reduceExpression = new DummyExpression(CTAny, DummyPosition(10)) {
      override def semanticCheck(ctx: SemanticContext) = s => {
        s.symbolTypes("x") should equal(CTString | CTFloat)
        s.symbolTypes("y") should equal(listType.innerType.invariant)
        (this.specifyType(CTFloat) chain SemanticCheckResult.success)(s)
      }
    }

    val filter = ReduceExpression(
      accumulator = Variable("x")(DummyPosition(2)),
      init = DummyExpression(initType),
      variable = Variable("y")(DummyPosition(6)),
      list = DummyExpression(listType),
      expression = reduceExpression
    )(DummyPosition(0))

    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors shouldBe empty
    filter.types(result.state) should equal(CTAny | CTFloat)
  }

  test("shouldFailSemanticCheckIfReduceFunctionTypeDiffersFromAccumulator") {
    val accumulatorType = CTString | CTNumber
    val listType = CTList(CTInteger)

    val reduceExpression = new DummyExpression(CTAny, DummyPosition(10)) {
      override def semanticCheck(ctx: SemanticContext) = s => {
        s.symbolTypes("x") should equal(accumulatorType)
        s.symbolTypes("y") should equal(listType.innerType.invariant)
        (this.specifyType(CTNode) chain SemanticCheckResult.success)(s)
      }
    }

    val filter = ReduceExpression(
      accumulator = Variable("x")(DummyPosition(2)),
      init = DummyExpression(accumulatorType),
      variable = Variable("y")(DummyPosition(6)),
      list = DummyExpression(listType),
      expression = reduceExpression
    )(DummyPosition(0))

    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors should have size 1
    result.errors.head.msg should equal("Type mismatch: accumulator is Number or String but expression has type Node")
    result.errors.head.position should equal(reduceExpression.position)
  }
}
