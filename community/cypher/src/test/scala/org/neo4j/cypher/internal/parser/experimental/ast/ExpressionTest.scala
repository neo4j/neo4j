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

class ExpressionTest extends Assertions {

  @Test(expected = classOf[IllegalStateException])
  def shouldThrowIfTypesRequestedButNotEvaluated() {
    val expression = new Expression() {
      val token = DummyToken(0, 1)
      def semanticCheck(ctx: Expression.SemanticContext) = ???
      def toCommand = ???
    }

    expression.types(SemanticState.clean)
  }

  @Test
  def shouldNotThrowIfTypesRequestedAfterEvaluated() {
    val expression = new Expression() {
      val token = DummyToken(0, 1)
      def semanticCheck(ctx: Expression.SemanticContext) = ???
      def toCommand = ???
    }
    val state = expression.limitType(NodeType())(SemanticState.clean).right.get
    assertEquals(Set(NodeType()), expression.types(state))
  }
}
