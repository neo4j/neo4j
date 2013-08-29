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
import org.neo4j.cypher.internal.parser.v2_0._
import org.junit.Test
import org.scalatest.Assertions
import org.junit.Assert._
import scala.collection.immutable.SortedSet

class CollectionSliceTest extends Assertions {
  val dummyCollection = DummyExpression(
    TypeSet(CollectionType(NodeType()), NodeType(), CollectionType(StringType())),
    DummyToken(2,3))

  @Test
  def shouldReturnCollectionTypesOfExpression() {
    val slice = CollectionSlice(dummyCollection,
      Some(SignedInteger(1, DummyToken(5,6))),
      Some(SignedInteger(2, DummyToken(7,8))),
      DummyToken(4, 8))

    val result = slice.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(), result.errors)
    assertEquals(Set(CollectionType(NodeType()), CollectionType(StringType())), slice.types(result.state))
  }

  @Test
  def shouldRaiseErrorWhenNeitherFromOrTwoSpecified() {
    val slice = CollectionSlice(dummyCollection,
      None,
      None,
      DummyToken(4, 8))

    val result = slice.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(SemanticError("The start or end (or both) is required for a collection slice", slice.token)), result.errors)
  }

  @Test
  def shouldRaiseErrorIfStartingFromFraction() {
    val to = Double(1.3, DummyToken(5,6))
    val slice = CollectionSlice(dummyCollection,
      None,
      Some(to),
      DummyToken(4, 8))

    val result = slice.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(SemanticError("Type mismatch: expected Integer or Long but was Double", to.token, SortedSet(to.token))), result.errors)
  }
}
