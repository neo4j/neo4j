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

import org.neo4j.cypher.internal.util.v3_4.DummyPosition
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.ast.DummyExpression
import org.neo4j.cypher.internal.v3_4.expressions.{ExtractExpression, True, Variable}

class SemanticExtractExpressionTest extends SemanticFunSuite {

  val dummyExpression = DummyExpression(
    CTList(CTNode) | CTBoolean | CTList(CTString)
  )

  val extractExpression = DummyExpression(CTNode | CTNumber, DummyPosition(2))

  test("shouldHaveCollectionWithInnerTypesOfExtractExpression") {
    val extract = ExtractExpression(Variable("x")(DummyPosition(5)), dummyExpression, None, Some(extractExpression))(DummyPosition(0))
    val result = SemanticExpressionCheck.simple(extract)(SemanticState.clean)
    result.errors shouldBe empty
    types(extract)(result.state) should equal(CTList(CTNode) | CTList(CTNumber))
  }

  test("shouldRaiseSemanticErrorIfPredicateSpecified") {
    val extract = ExtractExpression(Variable("x")(DummyPosition(5)), dummyExpression, Some(True()(DummyPosition(5))), Some(extractExpression))(DummyPosition(0))
    val result = SemanticExpressionCheck.simple(extract)(SemanticState.clean)
    result.errors should equal(Seq(SemanticError("extract(...) should not contain a WHERE predicate", DummyPosition(0))))
  }

  test("shouldRaiseSemanticErrorIfMissingExtractExpression") {
    val extract = ExtractExpression(Variable("x")(DummyPosition(5)), dummyExpression, None, None)(DummyPosition(0))
    val result = SemanticExpressionCheck.simple(extract)(SemanticState.clean)
    result.errors should equal(Seq(SemanticError("extract(...) requires '| expression' (an extract expression)", DummyPosition(0))))
  }
}
