/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{DummyPosition, SemanticError, SemanticState}

class ListSliceTest extends CypherFunSuite {
  val dummyList = DummyExpression(
    CTList(CTNode) | CTNode | CTList(CTString))

  test("shouldReturnCollectionTypesOfExpression") {
    val slice = ListSlice(dummyList,
                                Some(SignedDecimalIntegerLiteral("1")(DummyPosition(5))),
                                Some(SignedDecimalIntegerLiteral("2")(DummyPosition(7)))
    )(DummyPosition(4))

    val result = slice.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors shouldBe empty
    slice.types(result.state) should equal(CTList(CTNode) | CTList(CTString))
  }

  test("shouldRaiseErrorWhenNeitherFromOrTwoSpecified") {
    val slice = ListSlice(dummyList, None, None)(DummyPosition(4))

    val result = slice.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors should equal(Seq(SemanticError("The start or end (or both) is required for a collection slice", slice.position)))
  }

  test("shouldRaiseErrorIfStartingFromFraction") {
    val to = DecimalDoubleLiteral("1.3")(DummyPosition(5))
    val slice = ListSlice(dummyList, None, Some(to))(DummyPosition(4))

    val result = slice.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors should equal(Seq(SemanticError("Type mismatch: expected Integer but was Float", to.position)))
  }
}
