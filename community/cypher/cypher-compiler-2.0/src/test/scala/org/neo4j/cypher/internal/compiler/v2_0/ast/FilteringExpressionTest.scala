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
import commands.{expressions, Predicate}
import symbols._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

class FilteringExpressionTest extends Assertions {

  case class TestableFilteringExpression(identifier: Identifier, expression: Expression, innerPredicate: Option[Expression]) extends FilteringExpression {
    def name = "Testable Filter Expression"
    def token = DummyToken(0,10)

    def toCommand(command: expressions.Expression, name: String, inner: Predicate) = ???

    def toPredicate(command: expressions.Expression, name: String, inner: Predicate) = ???
  }

  @Test
  def shouldSemanticCheckPredicateInStateContainingTypedIdentifier() {
    val expression = new Expression with SimpleTypedExpression {
      def token = DummyToken(5,6)
      protected def possibleTypes = Set(CollectionType(NodeType()), BooleanType(), CollectionType(StringType()))

      def toCommand = ???
    }

    val error = SemanticError("dummy error", DummyToken(8,9))
    val predicate = new Expression {
      def token = DummyToken(7,9)
      def semanticCheck(ctx: SemanticContext) = s => {
        assertEquals(Set(NodeType(), BooleanType(), StringType()), s.symbolTypes("x"))
        SemanticCheckResult.error(s, error)
      }

      def toCommand = ???
    }

    val filter = TestableFilteringExpression(Identifier("x", DummyToken(2,3)), expression, Some(predicate))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(error), result.errors)
    assertEquals(None, result.state.symbol("x"))
  }
}
