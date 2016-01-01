/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.symbols._

class ExpressionTest extends CypherFunSuite {

  val expression = DummyExpression(CTAny, DummyPosition(0))

  test("shouldReturnCalculatedType") {
    expression.types(SemanticState.clean) should equal(TypeSpec.all)
  }

  test("shouldReturnTypeSetOfAllIfTypesRequestedButNotEvaluated") {
    expression.types(SemanticState.clean) should equal(TypeSpec.all)
  }

  test("shouldReturnSpecifiedAndConstrainedTypes") {
    val state = (
      expression.specifyType(CTNode | CTInteger) chain
      expression.expectType(CTNumber.covariant)
    )(SemanticState.clean).state

    expression.types(state) should equal(CTInteger.invariant)
  }

  test("shouldRaiseTypeErrorWhenMismatchBetweenSpecifiedTypeAndExpectedType") {
    val result = (
      expression.specifyType(CTNode | CTInteger) chain
      expression.expectType(CTString.covariant)
    )(SemanticState.clean)

    result.errors should have size 1
    result.errors.head.position should equal(expression.position)
    expression.types(result.state) shouldBe empty
    result.errors.head.msg should equal ("Type mismatch: expected String but was Integer or Node")
  }
  test("shouldRaiseTypeErrorWithCustomMessageWhenMismatchBetweenSpecifiedTypeAndExpectedType") {
    val result = (
      expression.specifyType(CTNode | CTInteger) chain
      expression.expectType(CTString.covariant, (expected: String, existing: String) => s"lhs was $expected yet rhs was $existing")
    )(SemanticState.clean)

    result.errors should have size 1
    result.errors.head.position should equal(expression.position)
    expression.types(result.state) shouldBe empty

    assert(result.errors.size === 1)
    assert(result.errors.head.position === expression.position)
    assert(result.errors.head.msg == "Type mismatch: lhs was String yet rhs was Integer or Node")
    assert(expression.types(result.state).isEmpty)
  }
}
