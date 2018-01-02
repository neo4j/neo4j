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

class CollectionSliceTest extends CypherFunSuite {
  val dummyCollection = DummyExpression(
    CTCollection(CTNode) | CTNode | CTCollection(CTString))

  test("shouldReturnCollectionTypesOfExpression") {
    val slice = CollectionSlice(dummyCollection,
      Some(SignedDecimalIntegerLiteral("1")(DummyPosition(5))),
      Some(SignedDecimalIntegerLiteral("2")(DummyPosition(7)))
    )(DummyPosition(4))

    val result = slice.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors shouldBe empty
    slice.types(result.state) should equal(CTCollection(CTNode) | CTCollection(CTString))
  }

  test("shouldRaiseErrorWhenNeitherFromOrTwoSpecified") {
    val slice = CollectionSlice(dummyCollection,
      None,
      None
    )(DummyPosition(4))

    val result = slice.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors should equal(Seq(SemanticError("The start or end (or both) is required for a collection slice", slice.position)))
  }

  test("shouldRaiseErrorIfStartingFromFraction") {
    val to = DecimalDoubleLiteral("1.3")(DummyPosition(5))
    val slice = CollectionSlice(dummyCollection,
      None,
      Some(to)
    )(DummyPosition(4))

    val result = slice.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors should equal(Seq(SemanticError("Type mismatch: expected Integer but was Float", to.position)))
  }
}
