/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SemanticCheckableTest extends CypherFunSuite with SemanticAnalysisTooling with AstConstructionTestSupport {

  test("shouldChainSemanticCheckableFunctions") {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = SemanticCheck.fromFunction(_ => SemanticCheckResult(state1, Seq(error1)))

    val state2 = SemanticState.clean
    val error2 = SemanticError("another error", DummyPosition(0))
    val func2: SemanticCheck = SemanticCheck.fromFunction { s =>
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
    val func1: SemanticCheck = SemanticCheck.fromFunction(_ => SemanticCheckResult(state1, Seq(error1)))

    val state2 = SemanticState.clean
    val func2: SemanticState => Either[SemanticError, SemanticState] = _ => Right(state2)

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
    val func1: SemanticCheck = SemanticCheck.fromFunction(_ => SemanticCheckResult(state1, Seq(error1)))

    val error2 = SemanticError("another error", DummyPosition(0))
    val func2: SemanticState => Either[SemanticError, SemanticState] = _ => Left(error2)

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
    val func1: SemanticCheck = SemanticCheck.fromFunction(_ => SemanticCheckResult(state1, Seq(error1)))

    val func2: SemanticState => Option[SemanticError] = _ => None

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
    val func1: SemanticCheck = SemanticCheck.fromFunction(_ => SemanticCheckResult(state1, Seq(error1)))

    val error2 = SemanticError("another error", DummyPosition(0))
    val func2: SemanticState => Option[SemanticError] = _ => Some(error2)

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
    val func1: SemanticCheck = SemanticCheck.fromFunction(_ => SemanticCheckResult(state1, Seq()))

    val error2 = SemanticError("an error", DummyPosition(0))
    val func2: SemanticState => Option[SemanticError] = _ => Some(error2)

    val chain: SemanticCheck = func1 chain func2
    val result = chain(SemanticState.clean)
    result.state should equal(state1)
    result.errors should equal(Seq(error2))
  }

  test("shouldNotChainSemanticFunctionAfterAnErrorWithIfOkThen") {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1 = (_: SemanticState) => SemanticCheckResult(state1, Seq(error1))
    val func2: SemanticCheck = SemanticCheck.fromFunction(_ => fail("Second check was incorrectly run"))

    val chain: SemanticCheck = func1 ifOkChain func2
    val result = chain(SemanticState.clean)
    result.state should equal(state1)
    result.errors should equal(Seq(error1))
  }

  test("shouldEvaluateInnerCheckForTrueWhen") {
    val state1 = SemanticState.clean
    val error1 = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = SemanticCheck.fromFunction(_ => SemanticCheckResult(state1, Seq(error1)))

    val error2 = SemanticError("another error", DummyPosition(0))
    val func2: SemanticState => Option[SemanticError] = _ => Some(error2)

    val chain: SemanticCheck = func1 chain when(condition = true) { func2 }
    val result = chain(SemanticState.clean)
    result.state should equal(state1)
    result.errors should equal(Seq(error1, error2))
  }

  test("shouldNotEvaluateInnerCheckForFalseWhen") {
    val state1 = SemanticState.clean
    val error = SemanticError("an error", DummyPosition(0))
    val func1: SemanticCheck = SemanticCheck.fromFunction(_ => SemanticCheckResult(state1, Seq(error)))
    val func2: SemanticCheck = SemanticCheck.fromFunction(_ => fail("Second check was incorrectly run"))

    val chain: SemanticCheck = func1 chain when(condition = false) { func2 }
    val result = chain(SemanticState.clean)
    result.state should equal(state1)
    result.errors should equal(Seq(error))
  }

  test("shouldScopeState") {
    val func1 =
      (s: SemanticState) => {
        val variable = Variable("name")(DummyPosition(0))
        s.declareVariable(variable, CTNode)
      }

    val error = SemanticError("an error", DummyPosition(0))
    val func2: SemanticCheck = SemanticCheck.fromFunction { s =>
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

  test("SemanticCheck.success shouldn't alter result") {
    val state1 = SemanticState.clean
    val Right(state2) = SemanticState.clean.declareVariable(varFor("x"), CTInteger.invariant)

    SemanticCheck.success(state1) shouldBe SemanticCheckResult(state1, Seq.empty)
    SemanticCheck.success(state2) shouldBe SemanticCheckResult(state2, Seq.empty)
  }

  test("SemanticCheck.error should produce error") {
    val state = SemanticState.clean
    val error = SemanticError("first error", pos)

    SemanticCheck.error(error)(state) shouldBe SemanticCheckResult(state, Vector(error))
  }

  test("map should work") {
    val state1 = SemanticState.clean
    val Right(state2) = SemanticState.clean.declareVariable(varFor("x"), CTInteger.invariant)

    val check = SemanticCheck.success.map(res => res.copy(state = state2))
    check(state1) shouldBe SemanticCheckResult(state2, Seq.empty)
  }

  test("flatMap should work") {
    val state1 = SemanticState.clean
    val Right(state2) = SemanticState.clean.declareVariable(varFor("x"), CTInteger.invariant)

    val leafCheck = SemanticCheck.fromFunction(_ => SemanticCheckResult(state2, Seq.empty))
    val check = SemanticCheck.success.flatMap(_ => leafCheck)
    check(state1) shouldBe SemanticCheckResult(state2, Seq.empty)
  }

  test("for-comprehension should thread state between checks and keep errors separate") {
    val state0 = SemanticState.clean
    val Right(state1) = state0.declareVariable(varFor("x"), CTInteger.invariant)
    val Right(state2) = state1.declareVariable(varFor("y"), CTInteger.invariant)
    val Right(state3) = state2.declareVariable(varFor("z"), CTInteger.invariant)

    val error1 = SemanticError("first error", pos)
    val error3 = SemanticError("second error", pos)

    val check1 = SemanticCheck.fromFunction { s =>
      s shouldBe state0
      SemanticCheckResult.error(state1, error1)
    }
    val check2 = SemanticCheck.fromFunction { s =>
      s shouldBe state1
      SemanticCheckResult.success(state2)
    }
    val check3 = SemanticCheck.fromFunction { s =>
      s shouldBe state2
      SemanticCheckResult.error(state3, error3)
    }

    val check = for {
      res1 <- check1
      res2 <- check2
      res3 <- check3
    } yield {
      res1 shouldBe SemanticCheckResult.error(state1, error1)
      res2 shouldBe SemanticCheckResult.success(state2)
      res3 shouldBe SemanticCheckResult.error(state3, error3)

      SemanticCheckResult(res3.state, res1.errors ++ res2.errors ++ res3.errors)
    }

    check(state0) shouldBe SemanticCheckResult(state3, Vector(error1, error3))
  }

  test("SemanticCheck.nestedCheck should work") {
    val error = SemanticError("some error", pos)
    val check = SemanticCheck.nestedCheck {
      SemanticCheck.error(error)
    }

    check.run(SemanticState.clean) shouldBe SemanticCheckResult(SemanticState.clean, Vector(error))
  }

  test("SemanticCheck.nestedCheck should not evaluate nested check during construction") {
    val error = SemanticError("some error", pos)
    val failingCheck = SemanticCheck.error(error)

    val nested = SemanticCheck.nestedCheck {
      fail("should not be called")
      SemanticCheck.success
    }

    val check = failingCheck ifOkChain nested

    check.run(SemanticState.clean) shouldBe SemanticCheckResult(SemanticState.clean, Vector(error))
  }

  test("SemanticCheck.fromState should work") {
    val error1 = SemanticError("first error", pos)
    val error2 = SemanticError("second error", pos)

    val checkFromState = SemanticCheck.fromState { state =>
      if (!state.isNode("x"))
        error1
      else
        error2
    }

    val check =
      checkFromState chain
        declareVariable(varFor("x"), CTNode.invariant) chain
        checkFromState

    check.run(SemanticState.clean).errors shouldBe Vector(error1, error2)
  }
}
