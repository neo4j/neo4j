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
package org.neo4j.cypher.internal.frontend.v3_4.semantics

import org.neo4j.cypher.internal.apa.v3_4.DummyPosition
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.symbols._

class ListComprehensionTest extends SemanticFunSuite {

  val dummyExpression = DummyExpression(
    CTList(CTNode) | CTBoolean | CTList(CTString))

  test("withoutExtractExpressionShouldHaveCollectionTypesOfInnerExpression") {
    val filter = ListComprehension(Variable("x")(DummyPosition(5)), dummyExpression, None, None)(DummyPosition(0))
    val result = SemanticExpressionCheck.simple(filter)(SemanticState.clean)
    result.errors shouldBe empty
    types(filter)(result.state) should equal(CTList(CTNode) | CTList(CTString))
  }

  test("shouldHaveCollectionWithInnerTypesOfExtractExpression") {
    val extractExpression = DummyExpression(CTNode | CTNumber, DummyPosition(2))

    val filter = ListComprehension(Variable("x")(DummyPosition(5)), dummyExpression, None, Some(extractExpression))(DummyPosition(0))
    val result = SemanticExpressionCheck.simple(filter)(SemanticState.clean)
    result.errors shouldBe empty
    types(filter)(result.state) should equal(CTList(CTNode) | CTList(CTNumber))
  }

  test("shouldSemanticCheckPredicateInStateContainingTypedVariable") {
    val error = SemanticError("dummy error", DummyPosition(8))
    val predicate = ErrorExpression(error, CTAny, DummyPosition(7))

    val filter = ListComprehension(Variable("x")(DummyPosition(2)), dummyExpression, Some(predicate), None)(DummyPosition(0))
    val result = SemanticExpressionCheck.simple(filter)(SemanticState.clean)
    result.errors should equal(Seq(error))
    result.state.symbol("x") should equal(None)
  }
}
