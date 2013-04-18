/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v2_0peg

import org.neo4j.cypher.internal.symbols._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

class SemanticCheckableTest extends Assertions {

  @Test
  def shouldChainSemanticCheckableFunctions() {
    val error1 = SemanticError("an error", DummyToken(0,1))
    val func1 = (s: SemanticState) => SemanticCheckResult(s.newScope, Seq(error1))

    val error2 = SemanticError("another error", DummyToken(0,1))
    val func2 = (s: SemanticState) => SemanticCheckResult(s.newScope, Seq(error2))

    val chain : SemanticState => SemanticCheckResult = func1 >>= func2
    val state = SemanticState.clean
    val result = chain(state)
    assertEquals(state, result.state.parent.get.parent.get)
    assertEquals(Seq(error1, error2), result.errors)
  }

  @Test
  def shouldChainSemanticFunctionReturningRightOfEither() {
    val error1 = SemanticError("an error", DummyToken(0,1))
    val func1 = (s: SemanticState) => SemanticCheckResult(s.newScope, Seq(error1))

    val func2 : SemanticState => Either[SemanticError, SemanticState] = s => Right(s.newScope)

    val chain : SemanticState => SemanticCheckResult = func1 >>= func2
    val state = SemanticState.clean
    val result = chain(state)
    assertEquals(state, result.state.parent.get.parent.get)
    assertEquals(Seq(error1), result.errors)

    val chain2 : SemanticState => SemanticCheckResult = func2 >>= func1
    val result2 = chain2(state)
    assertEquals(state, result2.state.parent.get.parent.get)
    assertEquals(Seq(error1), result2.errors)

    val chain3 : SemanticState => SemanticCheckResult = func2 >>= func2
    val result3 = chain3(state)
    assertEquals(state, result3.state.parent.get.parent.get)
    assertEquals(Seq(), result3.errors)
  }

  @Test
  def shouldChainSemanticFunctionReturningLeftOfEither() {
    val error1 = SemanticError("an error", DummyToken(0,1))
    val func1 = (s: SemanticState) => SemanticCheckResult(s.newScope, Seq(error1))

    val error2 = SemanticError("another error", DummyToken(0,1))
    val func2 : SemanticState => Either[SemanticError, SemanticState] = s => Left(error2)

    val chain : SemanticState => SemanticCheckResult = func1 >>= func2
    val state = SemanticState.clean
    val result = chain(state)
    assertEquals(state, result.state.parent.get)
    assertEquals(Seq(error1, error2), result.errors)

    val chain2 : SemanticState => SemanticCheckResult = func2 >>= func1
    val result2 = chain2(state)
    assertEquals(state, result2.state.parent.get)
    assertEquals(Seq(error2, error1), result2.errors)

    val chain3 : SemanticState => SemanticCheckResult = func2 >>= func2
    val result3 = chain3(state)
    assertEquals(state, result3.state)
    assertEquals(Seq(error2, error2), result3.errors)
  }

  @Test
  def shouldChainSemanticFunctionReturningNone() {
    val error1 = SemanticError("an error", DummyToken(0,1))
    val func1 = (s: SemanticState) => SemanticCheckResult(s.newScope, Seq(error1))

    val func2 : SemanticState => Option[SemanticError] = s => None

    val chain : SemanticState => SemanticCheckResult = func1 >>= func2
    val state = SemanticState.clean
    val result = chain(state)
    assertEquals(state, result.state.parent.get)
    assertEquals(Seq(error1), result.errors)

    val chain2 : SemanticState => SemanticCheckResult = func2 >>= func1
    val result2 = chain2(state)
    assertEquals(state, result2.state.parent.get)
    assertEquals(Seq(error1), result2.errors)

    val chain3 : SemanticState => SemanticCheckResult = func2 >>= func2
    val result3 = chain3(state)
    assertEquals(state, result3.state)
    assertEquals(Seq(), result3.errors)
  }

  @Test
  def shouldChainSemanticFunctionReturningSomeError() {
    val error1 = SemanticError("an error", DummyToken(0,1))
    val func1 = (s: SemanticState) => SemanticCheckResult(s.newScope, Seq(error1))

    val error2 = SemanticError("another error", DummyToken(0,1))
    val func2 : SemanticState => Option[SemanticError] = s => Some(error2)

    val chain : SemanticState => SemanticCheckResult = func1 >>= func2
    val state = SemanticState.clean
    val result = chain(state)
    assertEquals(state, result.state.parent.get)
    assertEquals(Seq(error1, error2), result.errors)

    val chain2 : SemanticState => SemanticCheckResult = func2 >>= func1
    val result2 = chain2(state)
    assertEquals(state, result2.state.parent.get)
    assertEquals(Seq(error2, error1), result2.errors)

    val chain3 : SemanticState => SemanticCheckResult = func2 >>= func2
    val result3 = chain3(state)
    assertEquals(state, result3.state)
    assertEquals(Seq(error2, error2), result3.errors)
  }
}
