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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.frontend.v2_3._
import org.neo4j.cypher.internal.frontend.v2_3.ast.Identifier
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class SemanticCheckableTest extends CypherFunSuite with SemanticChecking {

  test("shouldChainSemanticCheckableFunctions") {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq(error1))

    val state2 = SemanticState.clean
    val error2 = SemanticError("another error", DummyPosition(0))
    val func2: SemanticCheck = s => {
      s should equal(state1)
      SemanticCheckResult(state2, Seq(error2))
    }

    val chain: SemanticCheck = func1 chain func2
    val result = chain(SemanticState.clean)
    result.state should equal(state2)
    result.errors should equal(Seq(error1, error2))
  }

  test("shouldChainSemanticFunctionReturningRightOfEither") {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq(error1))

    val state2 = SemanticState.clean
    val func2: SemanticState => Either[SemanticError, SemanticState] = s => Right(state2)

    val chain: SemanticCheck = func1 chain func2
    val result = chain(SemanticState.clean)
    result.state should equal(state2)
    result.errors should equal(Seq(error1))

    val chain2: SemanticCheck = func2 chain func1
    val result2 = chain2(SemanticState.clean)
    result2.state should equal(state2)
    result2.errors should equal(Seq(error1))

    val chain3: SemanticCheck = func2 chain func2
    val result3 = chain3(SemanticState.clean)
    result3.state should equal(state2)
    result3.errors shouldBe empty
  }

  test("shouldChainSemanticFunctionReturningLeftOfEither") {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq(error1))

    val error2 = SemanticError("another error", DummyPosition(0))
    val func2: SemanticState => Either[SemanticError, SemanticState] = s => Left(error2)

    val chain1: SemanticCheck = func1 chain func2
    val result = chain1(SemanticState.clean)
    result.state should equal(state1)
    result.errors should equal(Seq(error1, error2))

    val chain2: SemanticCheck = func2 chain func1
    val result2 = chain2(SemanticState.clean)
    result2.state should equal(state1)
    result2.errors should equal(Seq(error2, error1))

    val chain3: SemanticCheck = func2 chain func2
    val state3 = SemanticState.clean
    val result3 = chain3(state3)
    result3.state should equal(state3)
    result3.errors should equal(Seq(error2, error2))
  }

  test("shouldChainSemanticFunctionReturningNone") {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq(error1))

    val func2: SemanticState => Option[SemanticError] = s => None

    val chain1: SemanticCheck = func1 chain func2
    val result = chain1(SemanticState.clean)
    result.state should equal(state1)
    result.errors should equal(Seq(error1))

    val chain2: SemanticCheck = func2 chain func1
    val result2 = chain2(SemanticState.clean)
    result2.state should equal(state1)
    result2.errors should equal(Seq(error1))

    val chain3: SemanticCheck = func2 chain func2
    val state3 = SemanticState.clean
    val result3 = chain3(state3)
    result3.state should equal(state3)
    result3.errors shouldBe empty
  }

  test("shouldChainSemanticFunctionReturningSomeError") {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq(error1))

    val error2 = SemanticError("another error", DummyPosition(0))
    val func2: SemanticState => Option[SemanticError] = s => Some(error2)

    val chain1: SemanticCheck = func1 chain func2
    val result = chain1(SemanticState.clean)
    result.state should equal(state1)
    result.errors should equal(Seq(error1, error2))

    val chain2: SemanticCheck = func2 chain func1
    val result2 = chain2(SemanticState.clean)
    result2.state should equal(state1)
    result2.errors should equal(Seq(error2, error1))

    val chain3: SemanticCheck = func2 chain func2
    val state3 = SemanticState.clean
    val result3 = chain3(state3)
    result3.state should equal(state3)
    result3.errors should equal(Seq(error2, error2))
  }

  test("shouldChainSemanticCheckAfterNoErrorWithIfOkThen") {
    val state1 = SemanticState.clean
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq())

    val error2 = SemanticError("an error", DummyPosition(0))
    val func2: SemanticState => Option[SemanticError] = s => Some(error2)

    val chain: SemanticCheck = func1 chain func2
    val result = chain(SemanticState.clean)
    result.state should equal(state1)
    result.errors should equal(Seq(error2))
  }

  test("shouldNotChainSemanticFunctionAfterAnErrorWithIfOkThen") {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1 = (s: SemanticState) => SemanticCheckResult(state1, Seq(error1))
    val func2: SemanticCheck = s => fail("Second check was incorrectly run")

    val chain: SemanticCheck = func1 ifOkChain func2
    val result = chain(SemanticState.clean)
    result.state should equal(state1)
    result.errors should equal(Seq(error1))
  }

  test("shouldEvaluateInnerCheckForTrueWhen") {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq(error1))

    val error2 = SemanticError("another error", DummyPosition(0))
    val func2: SemanticState => Option[SemanticError] = s => Some(error2)

    val chain: SemanticCheck = func1 chain when(condition = true) { func2 }
    val result = chain(SemanticState.clean)
    result.state should equal(state1)
    result.errors should equal(Seq(error1, error2))
  }

  test("shouldNotEvaluateInnerCheckForFalseWhen") {
    val state1 = SemanticState.clean
    val error = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = s => SemanticCheckResult(state1, Seq(error))
    val func2: SemanticCheck = s => fail("Second check was incorrectly run")

    val chain: SemanticCheck = func1 chain when(condition = false) { func2 }
    val result = chain(SemanticState.clean)
    result.state should equal(state1)
    result.errors should equal(Seq(error))
  }

  test("shouldScopeState") {
    val func1: SemanticCheck = Identifier("name")(DummyPosition(0)).declare(CTNode)

    val error = SemanticError("an error", DummyPosition(0))
    val func2: SemanticCheck = s => {
      s.currentScope.localSymbol("name") shouldBe defined
      s.currentScope.parent shouldBe defined
      SemanticCheckResult.error(s, error)
    }

    val chain: SemanticCheck = withScopedState { func1 chain func2 }
    val state = SemanticState.clean
    val result = chain(state)
    result.state.currentScope.symbolNames shouldBe empty
    result.errors should equal(Seq(error))
  }
}
