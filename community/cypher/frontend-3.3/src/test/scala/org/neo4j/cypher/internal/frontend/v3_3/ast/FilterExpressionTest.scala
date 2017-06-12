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

class FilterExpressionTest extends CypherFunSuite {

  val dummyExpression = DummyExpression(
    possibleTypes = CTList(CTNode) | CTBoolean | CTList(CTString)
  )

  test("shouldHaveCollectionTypesOfInnerExpression") {
    val filter = FilterExpression(
      variable = Variable("x")(DummyPosition(5)),
      expression = dummyExpression,
      innerPredicate = Some(True()(DummyPosition(5)))
    )(DummyPosition(0))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors shouldBe empty
    filter.types(result.state) should equal(CTList(CTNode) | CTList(CTString))
  }

  test("shouldRaiseSyntaxErrorIfMissingPredicate") {
    val filter = FilterExpression(
      variable = Variable("x")(DummyPosition(5)),
      expression = dummyExpression,
      innerPredicate = None
    )(DummyPosition(0))
    val result = filter.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors should equal(Seq(SemanticError("filter(...) requires a WHERE predicate", DummyPosition(0))))
  }
}
