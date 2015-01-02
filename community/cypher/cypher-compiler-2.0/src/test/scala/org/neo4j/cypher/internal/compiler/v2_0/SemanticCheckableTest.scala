/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0

import ast.Identifier
import symbols._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

class SemanticCheckableTest extends Assertions with SemanticChecking {

  @Test
  def shouldChainSemanticCheckableFunctions() {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq(error1))

    val state2 = SemanticState.clean
    val error2 = SemanticError("another error", DummyPosition(0))
    val func2: SemanticCheck = s => {
      assertEquals(s, state1)
      SemanticCheckResult(state2, Seq(error2))
    }

    val chain: SemanticCheck = func1 then func2
    val result = chain(SemanticState.clean)
    assertEquals(state2, result.state)
    assertEquals(Seq(error1, error2), result.errors)
  }

  @Test
  def shouldChainSemanticFunctionReturningRightOfEither() {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq(error1))

    val state2 = SemanticState.clean
    val func2: SemanticState => Either[SemanticError, SemanticState] = s => Right(state2)

    val chain: SemanticCheck = func1 then func2
    val result = chain(SemanticState.clean)
    assertEquals(state2, result.state)
    assertEquals(Seq(error1), result.errors)

    val chain2: SemanticCheck = func2 then func1
    val result2 = chain2(SemanticState.clean)
    assertEquals(state1, result2.state)
    assertEquals(Seq(error1), result2.errors)

    val chain3: SemanticCheck = func2 then func2
    val result3 = chain3(SemanticState.clean)
    assertEquals(state2, result3.state)
    assertEquals(Seq(), result3.errors)
  }

  @Test
  def shouldChainSemanticFunctionReturningLeftOfEither() {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq(error1))

    val error2 = SemanticError("another error", DummyPosition(0))
    val func2: SemanticState => Either[SemanticError, SemanticState] = s => Left(error2)

    val chain1: SemanticCheck = func1 then func2
    val result = chain1(SemanticState.clean)
    assertEquals(state1, result.state)
    assertEquals(Seq(error1, error2), result.errors)

    val chain2: SemanticCheck = func2 then func1
    val result2 = chain2(SemanticState.clean)
    assertEquals(state1, result2.state)
    assertEquals(Seq(error2, error1), result2.errors)

    val chain3: SemanticCheck = func2 then func2
    val state3 = SemanticState.clean
    val result3 = chain3(state3)
    assertEquals(state3, result3.state)
    assertEquals(Seq(error2, error2), result3.errors)
  }

  @Test
  def shouldChainSemanticFunctionReturningNone() {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq(error1))

    val func2: SemanticState => Option[SemanticError] = s => None

    val chain1: SemanticCheck = func1 then func2
    val result = chain1(SemanticState.clean)
    assertEquals(state1, result.state)
    assertEquals(Seq(error1), result.errors)

    val chain2: SemanticCheck = func2 then func1
    val result2 = chain2(SemanticState.clean)
    assertEquals(state1, result2.state)
    assertEquals(Seq(error1), result2.errors)

    val chain3: SemanticCheck = func2 then func2
    val state3 = SemanticState.clean
    val result3 = chain3(state3)
    assertEquals(state3, result3.state)
    assertEquals(Seq(), result3.errors)
  }

  @Test
  def shouldChainSemanticFunctionReturningSomeError() {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq(error1))

    val error2 = SemanticError("another error", DummyPosition(0))
    val func2: SemanticState => Option[SemanticError] = s => Some(error2)

    val chain1: SemanticCheck = func1 then func2
    val result = chain1(SemanticState.clean)
    assertEquals(state1, result.state)
    assertEquals(Seq(error1, error2), result.errors)

    val chain2: SemanticCheck = func2 then func1
    val result2 = chain2(SemanticState.clean)
    assertEquals(state1, result2.state)
    assertEquals(Seq(error2, error1), result2.errors)

    val chain3: SemanticCheck = func2 then func2
    val state3 = SemanticState.clean
    val result3 = chain3(state3)
    assertEquals(state3, result3.state)
    assertEquals(Seq(error2, error2), result3.errors)
  }

  @Test
  def shouldChainSemanticCheckAfterNoErrorWithIfOkThen() {
    val state1 = SemanticState.clean
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq())

    val error2 = SemanticError("an error", DummyPosition(0))
    val func2: SemanticState => Option[SemanticError] = s => Some(error2)

    val chain: SemanticCheck = func1 then func2
    val result = chain(SemanticState.clean)
    assertEquals(state1, result.state)
    assertEquals(Seq(error2), result.errors)
  }

  @Test
  def shouldNotChainSemanticFunctionAfterAnErrorWithIfOkThen() {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1 = (s: SemanticState) => SemanticCheckResult(state1, Seq(error1))
    val func2: SemanticCheck = s => fail("Second check was incorrectly run")

    val chain: SemanticCheck = func1 ifOkThen func2
    val result = chain(SemanticState.clean)
    assertEquals(state1, result.state)
    assertEquals(Seq(error1), result.errors)
  }

  @Test
  def shouldEvaluateInnerCheckForTrueWhen() {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq(error1))

    val error2 = SemanticError("another error", DummyPosition(0))
    val func2: SemanticState => Option[SemanticError] = s => Some(error2)

    val chain: SemanticCheck = func1 then when(condition = true) { func2 }
    val result = chain(SemanticState.clean)
    assertEquals(state1, result.state)
    assertEquals(Seq(error1, error2), result.errors)
  }

  @Test
  def shouldNotEvaluateInnerCheckForFalseWhen() {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq(error1))
    val func2: SemanticCheck = s => fail("Second check was incorrectly run")

    val chain: SemanticCheck = func1 then when(condition = false) { func2 }
    val result = chain(SemanticState.clean)
    assertEquals(state1, result.state)
    assertEquals(Seq(error1), result.errors)
  }

  @Test
  def shouldScopeState() {
    val func1: SemanticCheck = Identifier("name")(DummyPosition(0)).declare(CTNode)

    val error2 = SemanticError("an error", DummyPosition(0))
    val func2: SemanticCheck = s => {
      assertTrue(s.symbolTable.get("name").isDefined)
      assertTrue(s.parent.isDefined)
      SemanticCheckResult.error(s, error2)
    }

    val chain: SemanticCheck = withScopedState { func1 then func2 }
    val state = SemanticState.clean
    val result = chain(state)
    assertEquals(Map(), result.state.symbolTable)
    assertEquals(Seq(error2), result.errors)
  }
}
