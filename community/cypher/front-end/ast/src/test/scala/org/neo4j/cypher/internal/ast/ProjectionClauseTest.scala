/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ProjectionClauseTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should introduce variables into scope") {
    // GIVEN WITH "a" as n
    val returnItem = AliasedReturnItem(literalString("a"), varFor("n"))(pos)
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem)) _
    val withObj = With(distinct = false, listedReturnItems, None, None, None, None) _

    // WHEN
    val beforeState = SemanticState.clean.newChildScope
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState)

    // THEN
    result.errors shouldBe empty
    result.state.symbolTypes("n") should equal(CTString.invariant)
  }

  test("should remove variables from scope") {
    // GIVEN n WITH "a" as X
    val returnItem = AliasedReturnItem(literalString("a"), varFor("X"))(pos)
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem)) _
    val withObj = With(distinct = false, listedReturnItems, None, None, None, None) _

    val beforeState = SemanticState.clean.newChildScope.declareVariable(varFor("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState)

    // WHEN
    result.state.scopeTree

    // THEN the n variable is no longer accessible
    result.errors shouldBe empty
    result.state.symbol("n") shouldBe empty
  }

  test("test order by scoping") {
    // GIVEN MATCH n WITH n AS X ORDER BY X.prop1, X.prop2
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(prop("X", "prop1"))(pos),
      AscSortItem(prop("X", "prop2"))(pos)
    )) _

    val returnItem = AliasedReturnItem(varFor("n"), varFor("X"))(pos)
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem)) _
    val withObj = With(distinct = false, listedReturnItems, Some(orderBy), None, None, None) _

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareVariable(varFor("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState)

    // THEN the n variable is no longer accessible
    result.errors shouldBe empty
    result.state.symbol("n") shouldBe empty
    result.state.symbol("X") shouldNot be(empty)
  }

  test("test order by scoping 2") {
    // GIVEN MATCH n WITH n.prop AS introducedVariable ORDER BY introducedVariable + 2
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(add(varFor("introducedVariable"), literalInt(2)))(pos)
    )) _

    val returnItem = AliasedReturnItem(prop("n", "prop"), varFor("introducedVariable"))(pos)
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem)) _
    val withObj = With(distinct = false, listedReturnItems, Some(orderBy), None, None, None) _

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareVariable(varFor("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState)

    // THEN the n variable should be an integer
    result.errors shouldBe empty
    result.state.symbol("introducedVariable") shouldNot be(empty)
  }

  test("test where and order by scoping referring to previous scope items") {
    // GIVEN MATCH n, m WITH m AS X ORDER BY n.foo, X.bar WHERE n.foo = 10 AND X.bar = 2
    val where: Where = Where(and(
      equals(prop("n", "foo"), literalUnsignedInt(10)),
      equals(prop("X", "bar"), literalUnsignedInt(2))
    ))(pos)

    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(prop("n", "foo"))(pos),
      AscSortItem(prop("X", "bar"))(pos)
    )) _

    val returnItem = AliasedReturnItem(varFor("m"), varFor("X"))(pos)
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem)) _
    val withObj = With(distinct = false, listedReturnItems, Some(orderBy), None, None, Some(where)) _

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareVariable(varFor("n"), CTNode).right.get.declareVariable(
      varFor("m"),
      CTNode
    ).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState)

    // THEN the n and m variable is no longer accessible
    result.errors shouldBe empty
    result.state.symbol("n") shouldBe empty
    result.state.symbol("m") shouldBe empty
    result.state.symbol("X") shouldNot be(empty)
  }

  test("test order by scoping & shadowing 2") {
    // GIVEN MATCH n WITH n AS n ORDER BY n + 2
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(add(varFor("n"), literalInt(2)))(pos)
    )) _

    val returnItem = AliasedReturnItem(varFor("n"), varFor("n"))(pos)
    val listedReturnItems = ReturnItems(includeExisting = false, Seq(returnItem)) _
    val withObj = With(distinct = false, listedReturnItems, Some(orderBy), None, None, None) _

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareVariable(varFor("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState)

    // THEN the n variable should be an integer
    result.errors shouldNot be(empty)
  }

  test("WITH * allowed when no variables in scope") {
    // GIVEN CREATE () WITH * CREATE ()
    val withObj = With(distinct = false, ReturnItems(includeExisting = true, Seq()) _, None, None, None, None) _

    // WHEN
    val beforeState = SemanticState.clean.newChildScope
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState)

    // THEN the n variable should be an integer
    result.errors should be(empty)
  }

  test("RETURN * not allowed when no variables in scope") {
    // GIVEN CREATE () WITH * CREATE ()
    val withObj = Return(distinct = false, ReturnItems(includeExisting = true, Seq()) _, None, None, None) _

    // WHEN
    val beforeState = SemanticState.clean.newChildScope
    val result = withObj.semanticCheck(beforeState)

    // THEN
    result.errors shouldNot be(empty)
  }

  test("Aggregating queries remove variables from scope") {
    // GIVEN MATCH n WITH n.prop as x, count(*) ORDER BY n.bar
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(prop("n", "bar"))(pos)
    )) _

    val returnItems: Seq[AliasedReturnItem] = Seq(
      AliasedReturnItem(prop("n", "prop"), varFor("x"))(pos),
      AliasedReturnItem(CountStar() _, varFor("count"))(pos)
    )
    val listedReturnItems = ReturnItems(includeExisting = false, returnItems) _
    val withObj = With(distinct = false, listedReturnItems, Some(orderBy), None, None, None) _

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareVariable(varFor("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState)

    // THEN
    result.errors shouldNot be(empty)
  }

  test("Distinct queries remove variables from scope") {
    // GIVEN MATCH n WITH DISTINCT n.prop as x ORDER BY n.bar
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(Property(varFor("n"), PropertyKeyName("bar") _) _)(pos)
    )) _

    val returnItems: Seq[AliasedReturnItem] = Seq(
      AliasedReturnItem(Property(varFor("n"), PropertyKeyName("prop") _) _, varFor("x"))(pos)
    )
    val listedReturnItems = ReturnItems(includeExisting = false, returnItems) _
    val withObj = With(distinct = true, listedReturnItems, Some(orderBy), None, None, None) _

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareVariable(varFor("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState)

    // THEN
    result.errors shouldNot be(empty)
  }

  test("order by a property that isn't projected") {
    // GIVEN MATCH n WITH n.prop as x ORDER BY n.bar
    val orderBy: OrderBy = OrderBy(Seq(
      AscSortItem(prop("n", "bar"))(pos)
    )) _

    val returnItems: Seq[AliasedReturnItem] = Seq(
      AliasedReturnItem(prop("n", "prop"), varFor("x"))(pos)
    )
    val listedReturnItems = ReturnItems(includeExisting = false, returnItems) _
    val withObj = With(distinct = false, listedReturnItems, Some(orderBy), None, None, None) _

    // WHEN
    val beforeState = SemanticState.clean.newChildScope.declareVariable(varFor("n"), CTNode).right.get
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope)(middleState)

    // THEN
    result.errors should be(empty)
  }

  test("WITH should not care about outer scope") {
    val returnItems: Seq[AliasedReturnItem] = Seq(
      AliasedReturnItem(literalInt(1), varFor("x"))(pos)
    )
    val listedReturnItems = ReturnItems(includeExisting = false, returnItems) _
    val withObj = With(distinct = false, listedReturnItems, None, None, None, None) _

    // WHEN
    val outerState = SemanticState.clean.newChildScope.declareVariable(varFor("x"), CTNode).right.get
    val outerScope = outerState.currentScope.scope
    val beforeState = SemanticState.clean.newChildScope
    val middleState = withObj.semanticCheck(beforeState).state
    val result = withObj.semanticCheckContinuation(middleState.currentScope.scope, Some(outerScope))(middleState)

    // THEN
    result.errors should be(empty)
  }

  test("RETURN should fail to declare variable existing in outer scope") {
    val varPosition = InputPosition(100, 4, 10)
    val returnItems: Seq[AliasedReturnItem] = Seq(
      AliasedReturnItem(literalInt(1), varFor("x").copy()(varPosition))(pos)
    )
    val listedReturnItems = ReturnItems(includeExisting = false, returnItems) _
    val returnObj = Return(distinct = false, listedReturnItems, None, None, None) _

    // WHEN
    val outerState = SemanticState.clean.newChildScope.declareVariable(varFor("x"), CTNode).right.get
    val outerScope = outerState.currentScope.scope
    val beforeState = SemanticState.clean.newChildScope
    val middleState = returnObj.semanticCheck(beforeState).state
    val result = returnObj.semanticCheckContinuation(middleState.currentScope.scope, Some(outerScope))(middleState)

    // THEN
    result.errors shouldEqual Seq(
      SemanticError("Variable `x` already declared in outer scope", varPosition)
    )
  }
}
