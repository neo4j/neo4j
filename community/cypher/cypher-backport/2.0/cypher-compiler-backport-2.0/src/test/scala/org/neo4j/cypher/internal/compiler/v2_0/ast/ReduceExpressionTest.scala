/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._
import org.junit.Test
import org.scalatest.Assertions

class ReduceExpressionTest extends Assertions {

  @Test
  def shouldEvaluateReduceExpressionWithTypedIdentifiers() {
    val error = SemanticError("dummy error", DummyPosition(10))

    val reduceExpression = new DummyExpression(CTAny, DummyPosition(10)) {
      override def semanticCheck(ctx: SemanticContext) = s => {
        assert(s.symbolTypes("x") === CTString.invariant)
        assert(s.symbolTypes("y") === CTInteger.invariant)
        (this.specifyType(CTString) then error)(s)
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
    assert(result.errors === Seq(error))
    assert(result.state.symbol("x").isEmpty)
    assert(result.state.symbol("y").isEmpty)
  }

  @Test
  def shouldReturnMinimalTypeOfAccumulatorAndReduceFunction() {
    val initType = CTString.covariant | CTFloat.covariant
    val collectionType = CTCollection(CTInteger)

    val reduceExpression = new DummyExpression(CTAny, DummyPosition(10)) {
      override def semanticCheck(ctx: SemanticContext) = s => {
        assert(s.symbolTypes("x") === (CTString | CTFloat))
        assert(s.symbolTypes("y") === collectionType.innerType.invariant)
        (this.specifyType(CTFloat) then SemanticCheckResult.success)(s)
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
    assert(result.errors === Seq())
    assert(filter.types(result.state) === (CTAny | CTFloat))
  }

  @Test
  def shouldFailSemanticCheckIfReduceFunctionTypeDiffersFromAccumulator() {
    val accumulatorType = CTString | CTNumber
    val collectionType = CTCollection(CTInteger)

    val reduceExpression = new DummyExpression(CTAny, DummyPosition(10)) {
      override def semanticCheck(ctx: SemanticContext) = s => {
        assert(s.symbolTypes("x") === accumulatorType)
        assert(s.symbolTypes("y") === collectionType.innerType.invariant)
        (this.specifyType(CTNode) then SemanticCheckResult.success)(s)
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
    assert(result.errors.size === 1)
    assert(result.errors.head.msg === "Type mismatch: accumulator is Number or String but expression has type Node")
    assert(result.errors.head.position === reduceExpression.position)
  }

}
