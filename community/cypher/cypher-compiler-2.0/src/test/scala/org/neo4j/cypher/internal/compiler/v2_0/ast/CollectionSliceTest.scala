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
import org.junit.Test
import org.scalatest.Assertions
import org.junit.Assert._

class CollectionSliceTest extends Assertions {
  val dummyCollection = DummyExpression(
    CTCollection(CTNode) | CTNode | CTCollection(CTString))

  @Test
  def shouldReturnCollectionTypesOfExpression() {
    val slice = CollectionSlice(dummyCollection,
      Some(SignedDecimalIntegerLiteral("1")(DummyPosition(5))),
      Some(SignedDecimalIntegerLiteral("2")(DummyPosition(7)))
    )(DummyPosition(4))

    val result = slice.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(), result.errors)
    assertEquals(CTCollection(CTNode) | CTCollection(CTString), slice.types(result.state))
  }

  @Test
  def shouldRaiseErrorWhenNeitherFromOrTwoSpecified() {
    val slice = CollectionSlice(dummyCollection,
      None,
      None
    )(DummyPosition(4))

    val result = slice.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(SemanticError("The start or end (or both) is required for a collection slice", slice.position)), result.errors)
  }

  @Test
  def shouldRaiseErrorIfStartingFromFraction() {
    val to = DecimalDoubleLiteral("1.3")(DummyPosition(5))
    val slice = CollectionSlice(dummyCollection,
      None,
      Some(to)
    )(DummyPosition(4))

    val result = slice.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(SemanticError("Type mismatch: expected Integer but was Float", to.position)), result.errors)
  }
}
