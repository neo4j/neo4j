/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v3_5.ast.semantics

import org.neo4j.cypher.internal.v3_5.util.DummyPosition
import org.neo4j.cypher.internal.v3_5.util.symbols.CTNode
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.expressions.Variable

class SemanticCheckableTest extends CypherFunSuite with SemanticAnalysisTooling {

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
    val func1 =
      (s:SemanticState) => {
        val variable = Variable("name")(DummyPosition(0))
        s.declareVariable(variable, CTNode)
      }

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
