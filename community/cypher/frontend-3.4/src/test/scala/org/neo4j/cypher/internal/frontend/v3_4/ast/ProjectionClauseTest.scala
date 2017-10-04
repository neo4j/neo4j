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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticState
import org.neo4j.cypher.internal.v3_4.expressions._

class ProjectionClauseTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should introduce variables into scope") {
    // GIVEN WITH "a" as n
    val returnItem = AliasedReturnItem(StringLiteral("a")_, varFor("n"))_
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem))_
    val withObj = With(distinct = false, listedReturnItems, PassAllGraphReturnItems(pos), None, None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // THEN
    result.errors shouldBe empty
    result.state.symbolTypes("n") should equal(CTString.invariant)
  }

  test("should remove variables from scope") {
    // GIVEN n WITH "a" as X
    val returnItem = AliasedReturnItem(StringLiteral("a")_, varFor("X"))_
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem))_
    val withObj = With(distinct = false, listedReturnItems, PassAllGraphReturnItems(pos), None, None, None, None)_

    val beforeState = SemanticState.clean.newChildScope.declareVariable(varFor("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // WHEN
    val tree = result.state.scopeTree

    // THEN the n variable is no longer accessible
    result.errors shouldBe empty
    result.state.symbol("n") shouldBe empty
  }

  test("test order by scoping") {
    // GIVEN MATCH n WITH n AS X ORDER BY X.prop1, X.prop2
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(Property(varFor("X"), PropertyKeyName("prop1")_)_)_,
      AscSortItem(Property(varFor("X"), PropertyKeyName("prop2")_)_)_
    ))_

    val returnItem = AliasedReturnItem(varFor("n"), varFor("X"))_
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem))_
    val withObj = With(distinct = false, listedReturnItems, PassAllGraphReturnItems(pos), Some(orderBy), None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareVariable(varFor("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // THEN the n variable is no longer accessible
    result.errors shouldBe empty
    result.state.symbol("n") shouldBe empty
    result.state.symbol("X") shouldNot be(empty)
  }

  test("test order by scoping 2") {
    // GIVEN MATCH n WITH n.prop AS introducedVariable ORDER BY introducedVariable + 2
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(Add(varFor("introducedVariable"), SignedDecimalIntegerLiteral("2")_)_)_
    ))_

    val returnItem = AliasedReturnItem(Property(varFor("n"), PropertyKeyName("prop")_)_, varFor("introducedVariable"))_
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem))_
    val withObj = With(distinct = false, listedReturnItems, PassAllGraphReturnItems(pos), Some(orderBy), None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareVariable(varFor("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // THEN the n variable should be an integer
    result.errors shouldBe empty
    result.state.symbol("introducedVariable") shouldNot be(empty)
  }

  test("test order by scoping & shadowing 2") {
    // GIVEN MATCH n WITH n AS n ORDER BY n + 2
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(Add(varFor("n"), SignedDecimalIntegerLiteral("2")_)_)_
    ))_

    val returnItem = AliasedReturnItem(varFor("n"), varFor("n"))_
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem))_
    val withObj = With(distinct = false, listedReturnItems, PassAllGraphReturnItems(pos), Some(orderBy), None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareVariable(varFor("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // THEN the n variable should be an integer
    result.errors shouldNot be(empty)
  }

  test("WITH * allowed when no variables in scope") {
    // GIVEN CREATE () WITH * CREATE ()
    val withObj = With(distinct = false, ReturnItems(includeExisting = true, Seq())_, PassAllGraphReturnItems(pos), None, None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // THEN the n variable should be an integer
    result.errors should be(empty)
  }

  test("RETURN * not allowed when no variables in scope") {
    // GIVEN CREATE () WITH * CREATE ()
    val withObj = Return(distinct = false, ReturnItems(includeExisting = true, Seq())_, None, None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope
    val result = withObj.semanticCheck(beforeState)

    // THEN
    result.errors shouldNot be(empty)
  }

  test("Aggregating queries remove variables from scope") {
    // GIVEN MATCH n WITH n.prop as x, count(*) ORDER BY n.bar
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(Property(varFor("n"), PropertyKeyName("bar")_)_)_
    ))_

    val returnItems: Seq[AliasedReturnItem] = Seq(
      AliasedReturnItem(Property(varFor("n"), PropertyKeyName("prop")_)_, varFor("x"))_,
      AliasedReturnItem(CountStar()_, varFor("count"))_
    )
    val listedReturnItems = ReturnItems(includeExisting = false, returnItems)_
    val withObj = With(distinct = false, listedReturnItems, PassAllGraphReturnItems(pos), Some(orderBy), None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareVariable(varFor("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // THEN
    result.errors shouldNot be(empty)
  }

  test("order by a property that isn't projected") {
    // GIVEN MATCH n WITH n.prop as x ORDER BY n.bar
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(Property(varFor("n"), PropertyKeyName("bar")_)_)_
    ))_

    val returnItems: Seq[AliasedReturnItem] = Seq(
      AliasedReturnItem(Property(varFor("n"), PropertyKeyName("prop")_)_, varFor("x"))_
    )
    val listedReturnItems = ReturnItems(includeExisting = false, returnItems)_
    val withObj = With(distinct = false, listedReturnItems, PassAllGraphReturnItems(pos), Some(orderBy), None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareVariable(varFor("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // THEN
    result.errors should be(empty)
  }
}
