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

import org.neo4j.cypher.internal.frontend.v2_3.SemanticState
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ProjectionClauseTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should introduce identifiers into scope") {
    // GIVEN WITH "a" as n
    val returnItem = AliasedReturnItem(StringLiteral("a")_, ident("n"))_
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem))_
    val withObj = With(distinct = false, listedReturnItems, None, None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // THEN
    result.errors shouldBe empty
    result.state.symbolTypes("n") should equal(CTString.invariant)
  }

  test("should remove identifiers from scope") {
    // GIVEN n WITH "a" as X
    val returnItem = AliasedReturnItem(StringLiteral("a")_, ident("X"))_
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem))_
    val withObj = With(distinct = false, listedReturnItems, None, None, None, None)_

    val beforeState = SemanticState.clean.newChildScope.declareIdentifier(ident("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // WHEN
    val tree = result.state.scopeTree

    // THEN the n identifier is no longer accessible
    result.errors shouldBe empty
    result.state.symbol("n") shouldBe empty
  }

  test("test order by scoping") {
    // GIVEN MATCH n WITH n AS X ORDER BY X.prop1, X.prop2
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(Property(ident("X"), PropertyKeyName("prop1")_)_)_,
      AscSortItem(Property(ident("X"), PropertyKeyName("prop2")_)_)_
    ))_

    val returnItem = AliasedReturnItem(ident("n"), ident("X"))_
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem))_
    val withObj = With(distinct = false, listedReturnItems, Some(orderBy), None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareIdentifier(ident("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // THEN the n identifier is no longer accessible
    result.errors shouldBe empty
    result.state.symbol("n") shouldBe empty
    result.state.symbol("X") shouldNot be(empty)
  }

  test("test order by scoping 2") {
    // GIVEN MATCH n WITH n.prop AS introducedIdentifier ORDER BY introducedIdentifier + 2
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(Add(ident("introducedIdentifier"), SignedDecimalIntegerLiteral("2")_)_)_
    ))_

    val returnItem = AliasedReturnItem(Property(ident("n"), PropertyKeyName("prop")_)_, ident("introducedIdentifier"))_
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem))_
    val withObj = With(distinct = false, listedReturnItems, Some(orderBy), None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareIdentifier(ident("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // THEN the n identifier should be an integer
    result.errors shouldBe empty
    result.state.symbol("introducedIdentifier") shouldNot be(empty)
  }

  test("test order by scoping & shadowing 2") {
    // GIVEN MATCH n WITH n AS n ORDER BY n + 2
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(Add(ident("n"), SignedDecimalIntegerLiteral("2")_)_)_
    ))_

    val returnItem = AliasedReturnItem(ident("n"), ident("n"))_
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem))_
    val withObj = With(distinct = false, listedReturnItems, Some(orderBy), None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareIdentifier(ident("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // THEN the n identifier should be an integer
    result.errors shouldNot be(empty)
  }

  test("WITH * allowed when no identifiers in scope") {
    // GIVEN CREATE () WITH * CREATE ()
    val withObj = With(distinct = false, ReturnItems(includeExisting = true, Seq())_, None, None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // THEN the n identifier should be an integer
    result.errors should be(empty)
  }

  test("RETURN * not allowed when no identifiers in scope") {
    // GIVEN CREATE () WITH * CREATE ()
    val withObj = Return(distinct = false, ReturnItems(includeExisting = true, Seq())_, None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope
    val result = withObj.semanticCheck(beforeState)

    // THEN
    result.errors shouldNot be(empty)
  }

  test("Aggregating queries remove identifiers from scope") {
    // GIVEN MATCH n WITH n.prop as x, count(*) ORDER BY n.bar
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(Property(ident("n"), PropertyKeyName("bar")_)_)_
    ))_

    val returnItems: Seq[AliasedReturnItem] = Seq(
      AliasedReturnItem(Property(ident("n"), PropertyKeyName("prop")_)_, ident("x"))_,
      AliasedReturnItem(CountStar()_, ident("count"))_
    )
    val listedReturnItems = ReturnItems(includeExisting = false, returnItems)_
    val withObj = With(distinct = false, listedReturnItems, Some(orderBy), None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareIdentifier(ident("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // THEN
    result.errors shouldNot be(empty)
  }

  test("order by a property that isn't projected") {
    // GIVEN MATCH n WITH n.prop as x ORDER BY n.bar
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(Property(ident("n"), PropertyKeyName("bar")_)_)_
    ))_

    val returnItems: Seq[AliasedReturnItem] = Seq(
      AliasedReturnItem(Property(ident("n"), PropertyKeyName("prop")_)_, ident("x"))_
    )
    val listedReturnItems = ReturnItems(includeExisting = false, returnItems)_
    val withObj = With(distinct = false, listedReturnItems, Some(orderBy), None, None, None)_

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareIdentifier(ident("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState.newSiblingScope)

    // THEN
    result.errors should be(empty)
  }
}
