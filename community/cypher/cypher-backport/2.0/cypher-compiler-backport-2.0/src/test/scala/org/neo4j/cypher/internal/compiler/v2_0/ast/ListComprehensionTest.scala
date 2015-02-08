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
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

class ListComprehensionTest extends Assertions {

  val dummyExpression = DummyExpression(
    CTCollection(CTNode) | CTBoolean | CTCollection(CTString))

  @Test
  def withoutExtractExpressionShouldHaveCollectionTypesOfInnerExpression() {
    val filter = ListComprehension(Identifier("x")(DummyPosition(5)), dummyExpression, None, None)(DummyPosition(0))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(), result.errors)
    assertEquals(CTCollection(CTNode) | CTCollection(CTString), filter.types(result.state))
  }

  @Test
  def shouldHaveCollectionWithInnerTypesOfExtractExpression() {
    val extractExpression = DummyExpression(CTNode | CTNumber, DummyPosition(2))

    val filter = ListComprehension(Identifier("x")(DummyPosition(5)), dummyExpression, None, Some(extractExpression))(DummyPosition(0))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(), result.errors)
    assertEquals(CTCollection(CTNode) | CTCollection(CTNumber), filter.types(result.state))
  }

  @Test
  def shouldSemanticCheckPredicateInStateContainingTypedIdentifier() {
    val error = SemanticError("dummy error", DummyPosition(8))
    val predicate = new DummyExpression(CTAny, DummyPosition(7)) {
      override def semanticCheck(ctx: SemanticContext) = s => {
        assertEquals(CTNode | CTString, s.symbolTypes("x"))
        SemanticCheckResult.error(s, error)
      }
    }

    val filter = ListComprehension(Identifier("x")(DummyPosition(2)), dummyExpression, Some(predicate), None)(DummyPosition(0))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(error), result.errors)
    assertEquals(None, result.state.symbol("x"))
  }
}
