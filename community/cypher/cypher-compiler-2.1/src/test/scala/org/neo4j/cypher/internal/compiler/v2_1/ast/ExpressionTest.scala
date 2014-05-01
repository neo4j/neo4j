/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.ast

import org.neo4j.cypher.internal.compiler.v2_1._
import symbols._
import org.junit.Test
import org.scalatest.Assertions

class ExpressionTest extends Assertions {

  val expression = DummyExpression(CTAny, DummyPosition(0))

  @Test
  def shouldReturnCalculatedType() {
    assert(expression.types(SemanticState.clean) === TypeSpec.all)
  }

  @Test
  def shouldReturnTypeSetOfAllIfTypesRequestedButNotEvaluated() {
    assert(expression.types(SemanticState.clean) === TypeSpec.all)
  }

  @Test
  def shouldReturnSpecifiedAndConstrainedTypes() {
    val state = (
      expression.specifyType(CTNode | CTInteger) chain
      expression.expectType(CTNumber.covariant)
    )(SemanticState.clean).state

    assert(expression.types(state) === CTInteger.invariant)
  }

  @Test
  def shouldRaiseTypeErrorWhenMismatchBetweenSpecifiedTypeAndExpectedType() {
    val result = (
      expression.specifyType(CTNode | CTInteger) chain
      expression.expectType(CTString.covariant)
    )(SemanticState.clean)

    assert(result.errors.size === 1)
    assert(result.errors.head.position === expression.position)
    assert(expression.types(result.state).isEmpty)
  }
}
