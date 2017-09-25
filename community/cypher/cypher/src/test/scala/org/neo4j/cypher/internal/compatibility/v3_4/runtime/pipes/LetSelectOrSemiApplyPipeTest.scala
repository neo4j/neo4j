/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.{Literal, Variable}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.predicates.{Equals, Not, True}
import org.neo4j.cypher.internal.frontend.v3_4.symbols.CTNumber
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.{FALSE, TRUE, intValue}

class LetSelectOrSemiApplyPipeTest extends CypherFunSuite with PipeTestSupport {

  test("should only write let = true for the one that matches when the expression is false") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = pipeWithResults((state) => {
      val initialContext = state.initialContext.get
      if (initialContext("a") == intValue(1)) Iterator(initialContext) else Iterator.empty
    })

    val result =
      LetSelectOrSemiApplyPipe(lhs, rhs, "let", Not(True()), negated = false)().
        createResults(QueryStateHelper.empty).toList

    result should equal(List(
      Map("a" -> intValue(1), "let" -> TRUE),
      Map("a" -> intValue(2), "let" -> FALSE)
    ))
  }

  test("should only write let = true for the one that not matches when the expression is false and it is negated") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = pipeWithResults((state) => {
      val initialContext = state.initialContext.get
      if (initialContext("a") == intValue(1)) Iterator(initialContext) else Iterator.empty
    })

    val result =
      LetSelectOrSemiApplyPipe(lhs, rhs, "let", Not(True()), negated = true)().
        createResults(QueryStateHelper.empty).toList

    result should equal(List(
      Map("a" -> intValue(1), "let" -> FALSE),
      Map("a" -> intValue(2), "let" -> TRUE)
    ))
  }

  test("should not write let = true for anything if rhs is empty and expression is false") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator.empty)

    val result =
      LetSelectOrSemiApplyPipe(lhs, rhs, "let", Not(True()), negated = false)().
        createResults(QueryStateHelper.empty).toList

    result should equal(List(
      Map("a" -> intValue(1), "let" -> FALSE),
      Map("a" -> intValue(2), "let" -> FALSE)
    ))
  }

  test("should write let = true for everything if rhs is nonEmpty and the expression is false") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator(Map("a" -> 1)))

    val result =
      LetSelectOrSemiApplyPipe(lhs, rhs, "let", Not(True()), negated = false)().
        createResults(QueryStateHelper.empty).toList

    result should equal(List(
      Map("a" -> intValue(1), "let" -> TRUE),
      Map("a" -> intValue(2), "let" -> TRUE)
    ))
  }

  test("if lhs is empty, rhs should not be touched regardless the given expression") {
    val rhs = pipeWithResults((_) => fail("should not use this"))
    val lhs = new FakePipe(Iterator.empty)

    // Should not throw
    LetSelectOrSemiApplyPipe(lhs, rhs, "let", True(), negated = false)().
      createResults(QueryStateHelper.empty).toList
  }

  test("should write let = true for the one satisfying the expression even if the rhs is empty") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator.empty)

    val result =
      LetSelectOrSemiApplyPipe(lhs, rhs, "let", Equals(Variable("a"), Literal(2)), negated = false)().
        createResults(QueryStateHelper.empty).toList

    result should equal(List(
      Map("a" -> intValue(1), "let" -> FALSE),
      Map("a" -> intValue(2), "let" -> TRUE)
    ))
  }

  test("should write let = true for the one that matches and the one satisfying the expression") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2), Map("a" -> 3))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = pipeWithResults((state: QueryState) => {
      val initialContext = state.initialContext.get
      if (initialContext("a") == intValue(1)) Iterator(initialContext) else Iterator.empty
    })

    val result =
      LetSelectOrSemiApplyPipe(lhs, rhs, "let", Equals(Variable("a"), Literal(2)), negated = false)().
        createResults(QueryStateHelper.empty).toList

    result should equal(List(
      Map("a" -> intValue(1), "let" -> TRUE),
      Map("a" -> intValue(2), "let" -> TRUE),
      Map("a" -> intValue(3), "let" -> FALSE)
    ))
  }

  test("should not write let = true for anything if the rhs is empty and the expression is false") {
    val lhsData = List(Map("a" -> 3), Map("a" -> 4))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = pipeWithResults((state: QueryState) => {
      val initialContext = state.initialContext.get
      if (initialContext("a") == intValue(1)) Iterator(initialContext) else Iterator.empty
    })

    val result =
      LetSelectOrSemiApplyPipe(lhs, rhs, "let", Equals(Variable("a"), Literal(2)), negated = false)().
        createResults(QueryStateHelper.empty).toList
    result should equal(List(
      Map("a" -> intValue(3), "let" -> FALSE),
      Map("a" -> intValue(4), "let" -> FALSE)
    ))
  }
}
