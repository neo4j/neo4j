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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, SemanticCheckResult, SemanticError, SemanticState}

class ReduceExpressionTest extends CypherFunSuite {

  test("shouldEvaluateReduceExpressionWithTypedIdentifiers") {
    val error = SemanticError("dummy error", DummyPosition(10))

    val reduceExpression = new DummyExpression(CTAny, DummyPosition(10)) {
      override def semanticCheck(ctx: SemanticContext) = s => {
        s.symbolTypes("x") should equal(CTString.invariant)
        s.symbolTypes("y") should equal(CTInteger.invariant)
        (this.specifyType(CTString) chain error)(s)
      }
    }

    val filter = ReduceExpression(
      accumulator = Identifier("x")(DummyPosition(2)),
      init = DummyExpression(CTString),
      identifier = Identifier("y")(DummyPosition(6)),
      collection = DummyExpression(CTCollection(CTInteger)),
      expression = reduceExpression
    )(DummyPosition(0))

    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors should equal(Seq(error))
    result.state.symbol("x") shouldBe empty
    result.state.symbol("y") shouldBe empty
  }

  test("shouldReturnMinimalTypeOfAccumulatorAndReduceFunction") {
    val initType = CTString.covariant | CTFloat.covariant
    val collectionType = CTCollection(CTInteger)

    val reduceExpression = new DummyExpression(CTAny, DummyPosition(10)) {
      override def semanticCheck(ctx: SemanticContext) = s => {
        s.symbolTypes("x") should equal(CTString | CTFloat)
        s.symbolTypes("y") should equal(collectionType.innerType.invariant)
        (this.specifyType(CTFloat) chain SemanticCheckResult.success)(s)
      }
    }

    val filter = ReduceExpression(
      accumulator = Identifier("x")(DummyPosition(2)),
      init = DummyExpression(initType),
      identifier = Identifier("y")(DummyPosition(6)),
      collection = DummyExpression(collectionType),
      expression = reduceExpression
    )(DummyPosition(0))

    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors shouldBe empty
    filter.types(result.state) should equal(CTAny | CTFloat)
  }

  test("shouldFailSemanticCheckIfReduceFunctionTypeDiffersFromAccumulator") {
    val accumulatorType = CTString | CTNumber
    val collectionType = CTCollection(CTInteger)

    val reduceExpression = new DummyExpression(CTAny, DummyPosition(10)) {
      override def semanticCheck(ctx: SemanticContext) = s => {
        s.symbolTypes("x") should equal(accumulatorType)
        s.symbolTypes("y") should equal(collectionType.innerType.invariant)
        (this.specifyType(CTNode) chain SemanticCheckResult.success)(s)
      }
    }

    val filter = ReduceExpression(
      accumulator = Identifier("x")(DummyPosition(2)),
      init = DummyExpression(accumulatorType),
      identifier = Identifier("y")(DummyPosition(6)),
      collection = DummyExpression(collectionType),
      expression = reduceExpression
    )(DummyPosition(0))

    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors should have size 1
    result.errors.head.msg should equal("Type mismatch: accumulator is Number or String but expression has type Node")
    result.errors.head.position should equal(reduceExpression.position)
  }
}
