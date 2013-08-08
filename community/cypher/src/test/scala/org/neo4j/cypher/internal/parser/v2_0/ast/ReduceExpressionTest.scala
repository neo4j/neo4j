/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v2_0.ast

import org.neo4j.cypher.internal.symbols._
import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.internal.parser.v2_0._
import org.neo4j.cypher.internal.parser.v2_0.ast.Expression.SemanticContext

class ReduceExpressionTest extends Assertions {

  @Test
  def shouldEvaluateReduceExpressionWithTypedIdentifiers() {
    val accumulatorType = TypeSet(StringType())
    val collectionType = CollectionType(IntegerType())
    val error = SemanticError("dummy error", DummyToken(10,11))

    val reduceExpression = new Expression {
      def token = DummyToken(10,12)
      def semanticCheck(ctx: SemanticContext) = s => {
        assert(s.symbolTypes("x") === Some(accumulatorType))
        assert(s.symbolTypes("y") === Some(TypeSet(collectionType.iteratedType)))
        (limitType(StringType()) then error)(s)
      }

      def toCommand = ???
    }

    val filter = ReduceExpression(
      accumulator = Identifier("x", DummyToken(2,3)),
      init = DummyExpression(accumulatorType, DummyToken(4, 5)),
      id = Identifier("y", DummyToken(6, 7)),
      collection = DummyExpression(TypeSet(collectionType), DummyToken(8,9)),
      expression = reduceExpression,
      token = DummyToken(0, 12))

    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assert(result.errors === Seq(error))
    assert(result.state.symbol("x").isEmpty)
    assert(result.state.symbol("y").isEmpty)
  }

  @Test
  def shouldReturnMinimalTypeOfAccumulatorAndReduceFunction() {
    val accumulatorType = TypeSet(StringType(), NumberType())
    val collectionType = CollectionType(IntegerType())

    val reduceExpression = new Expression {
      def token = DummyToken(10,12)
      def semanticCheck(ctx: SemanticContext) = s => {
        assert(s.symbolTypes("x") === Some(accumulatorType))
        assert(s.symbolTypes("y") === Some(TypeSet(collectionType.iteratedType)))
        (limitType(DoubleType()) then SemanticCheckResult.success)(s)
      }

      def toCommand = ???
    }

    val filter = ReduceExpression(
      accumulator = Identifier("x", DummyToken(2,3)),
      init = DummyExpression(accumulatorType, DummyToken(4, 5)),
      id = Identifier("y", DummyToken(6, 7)),
      collection = DummyExpression(TypeSet(collectionType), DummyToken(8,9)),
      expression = reduceExpression,
      token = DummyToken(0, 12))

    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assert(result.errors === Seq())
    assert(filter.types(result.state) === TypeSet(NumberType()))
  }

  @Test
  def shouldFailSemanticCheckIfReduceFunctionTypeDiffersFromAccumulator() {
    val accumulatorType = TypeSet(StringType(), NumberType())
    val collectionType = CollectionType(IntegerType())

    val reduceExpression = new Expression {
      def token = DummyToken(10,12)
      def semanticCheck(ctx: SemanticContext) = s => {
        assert(s.symbolTypes("x") === Some(accumulatorType))
        assert(s.symbolTypes("y") === Some(TypeSet(collectionType.iteratedType)))
        (limitType(NodeType()) then SemanticCheckResult.success)(s)
      }

      def toCommand = ???
    }

    val filter = ReduceExpression(
      accumulator = Identifier("x", DummyToken(2,3)),
      init = DummyExpression(accumulatorType, DummyToken(4, 5)),
      id = Identifier("y", DummyToken(6, 7)),
      collection = DummyExpression(TypeSet(collectionType), DummyToken(8,9)),
      expression = reduceExpression,
      token = DummyToken(0, 12))

    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assert(result.errors.size === 1)
    assert(result.errors.head.msg === "Type mismatch: expected String or Number but was Node")
    assert(result.errors.head.token === reduceExpression.token)
  }

}
