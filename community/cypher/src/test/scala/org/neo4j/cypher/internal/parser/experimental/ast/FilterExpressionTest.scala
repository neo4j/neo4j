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
package org.neo4j.cypher.internal.parser.experimental.ast

import org.neo4j.cypher.internal.symbols._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.internal.parser.experimental._

class FilterExpressionTest extends Assertions {

  val dummyExpression = DummyExpression(
    TypeSet(CollectionType(NodeType()), BooleanType(), CollectionType(StringType())),
    DummyToken(2,3))

  @Test
  def shouldHaveCollectionTypesOfInnerExpression() {
    val filter = FilterExpression(Identifier("x", DummyToken(5,6)), dummyExpression, Some(True(DummyToken(5,6))), DummyToken(0, 10))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(), result.errors)
    assertEquals(Set(CollectionType(NodeType()), CollectionType(StringType())), filter.types(result.state))
  }

  @Test
  def shouldRaiseSyntaxErrorIfMissingPredicate() {
    val filter = FilterExpression(Identifier("x", DummyToken(5, 6)), dummyExpression, None, DummyToken(0, 10))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(SemanticError("FILTER requires a WHERE predicate", DummyToken(0, 10))), result.errors)
  }
}
