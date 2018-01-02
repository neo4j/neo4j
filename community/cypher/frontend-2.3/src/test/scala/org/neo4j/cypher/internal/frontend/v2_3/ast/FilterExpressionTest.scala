/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, SemanticError, SemanticState}

class FilterExpressionTest extends CypherFunSuite {

  val dummyExpression = DummyExpression(
    possibleTypes = CTCollection(CTNode) | CTBoolean | CTCollection(CTString)
  )

  test("shouldHaveCollectionTypesOfInnerExpression") {
    val filter = FilterExpression(
      identifier = Identifier("x")(DummyPosition(5)),
      expression = dummyExpression,
      innerPredicate = Some(True()(DummyPosition(5)))
    )(DummyPosition(0))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors shouldBe empty
    filter.types(result.state) should equal(CTCollection(CTNode) | CTCollection(CTString))
  }

  test("shouldRaiseSyntaxErrorIfMissingPredicate") {
    val filter = FilterExpression(
      identifier = Identifier("x")(DummyPosition(5)),
      expression = dummyExpression,
      innerPredicate = None
    )(DummyPosition(0))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors should equal(Seq(SemanticError("filter(...) requires a WHERE predicate", DummyPosition(0))))
  }
}
