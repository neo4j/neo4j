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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

class ListComprehensionTest extends Assertions {

  val dummyExpression = DummyExpression(
    TypeSet(CollectionType(NodeType()), BooleanType(), CollectionType(StringType())),
    DummyToken(2,3))

  @Test
  def withoutExtractExpressionShouldHaveCollectionTypesOfInnerExpression() {
    val filter = ListComprehension(Identifier("x", DummyToken(5,6)), dummyExpression, None, None, DummyToken(0, 10))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(), result.errors)
    assertEquals(Set(CollectionType(NodeType()), BooleanType(), CollectionType(StringType())), filter.types(result.state))
  }

  @Test
  def shouldHaveCollectionWithInnerTypesOfExtractExpression() {
    val extractExpression = new Expression with SimpleTypedExpression {
      def token: InputToken = DummyToken(2,3)
      protected def possibleTypes: TypeSet = Set(NodeType(), NumberType())

      def toCommand = ???
    }

    val filter = ListComprehension(Identifier("x", DummyToken(5,6)), dummyExpression, None, Some(extractExpression), DummyToken(0, 10))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(), result.errors)
    assertEquals(Set(CollectionType(NodeType()), CollectionType(NumberType())), filter.types(result.state))
  }

  @Test
  def shouldSemanticCheckPredicateInStateContainingTypedIdentifier() {
    val error = SemanticError("dummy error", DummyToken(8,9))
    val predicate = new Expression {
      def token = DummyToken(7,9)
      def semanticCheck(ctx: SemanticContext) = s => {
        assertEquals(Set(NodeType(), StringType(), BooleanType()), s.symbolTypes("x"))
        SemanticCheckResult.error(s, error)
      }

      def toCommand = ???
    }

    val filter = ListComprehension(Identifier("x", DummyToken(2,3)), dummyExpression, Some(predicate), None, DummyToken(0, 10))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(error), result.errors)
    assertEquals(None, result.state.symbol("x"))
  }
}
