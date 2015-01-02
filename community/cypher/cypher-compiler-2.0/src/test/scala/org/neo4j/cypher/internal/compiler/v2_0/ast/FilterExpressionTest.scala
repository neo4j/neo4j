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

import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

class FilterExpressionTest extends Assertions {

  val dummyExpression = DummyExpression(
    possibleTypes = CTCollection(CTNode) | CTBoolean | CTCollection(CTString)
  )

  @Test
  def shouldHaveCollectionTypesOfInnerExpression() {
    val filter = FilterExpression(
      identifier = Identifier("x")(DummyPosition(5)),
      expression = dummyExpression,
      innerPredicate = Some(True()(DummyPosition(5)))
    )(DummyPosition(0))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(), result.errors)
    assertEquals(CTCollection(CTNode) | CTCollection(CTString), filter.types(result.state))
  }

  @Test
  def shouldRaiseSyntaxErrorIfMissingPredicate() {
    val filter = FilterExpression(
      identifier = Identifier("x")(DummyPosition(5)),
      expression = dummyExpression,
      innerPredicate = None
    )(DummyPosition(0))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(SemanticError("filter(...) requires a WHERE predicate", DummyPosition(0))), result.errors)
  }
}
