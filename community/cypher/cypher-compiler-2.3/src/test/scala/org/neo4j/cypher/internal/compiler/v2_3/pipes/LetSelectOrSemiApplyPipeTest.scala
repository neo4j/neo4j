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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{Equals, True, Not}
import org.neo4j.cypher.internal.frontend.v2_3.symbols.CTNumber
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Literal, Identifier}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class LetSelectOrSemiApplyPipeTest extends CypherFunSuite with PipeTestSupport {

  test("should only write let = true for the one that matches when the expression is false") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = pipeWithResults((state) => {
      val initialContext = state.initialContext.get
      if (initialContext("a") == 1) Iterator(initialContext) else Iterator.empty
    })

    val result =
      LetSelectOrSemiApplyPipe(lhs, rhs, "let", Not(True()), negated = false)()(newMonitor).
        createResults(QueryStateHelper.empty).toList

    result should equal(List(
      Map("a" -> 1, "let" -> true),
      Map("a" -> 2, "let" -> false)
    ))
  }

  test("should only write let = true for the one that not matches when the expression is false and it is negated") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = pipeWithResults((state) => {
      val initialContext = state.initialContext.get
      if (initialContext("a") == 1) Iterator(initialContext) else Iterator.empty
    })

    val result =
      LetSelectOrSemiApplyPipe(lhs, rhs, "let", Not(True()), negated = true)()(newMonitor).
        createResults(QueryStateHelper.empty).toList

    result should equal(List(
      Map("a" -> 1, "let" -> false),
      Map("a" -> 2, "let" -> true)
    ))
  }

  test("should not write let = true for anything if rhs is empty and expression is false") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator.empty)

    val result =
      LetSelectOrSemiApplyPipe(lhs, rhs, "let", Not(True()), negated = false)()(newMonitor).
        createResults(QueryStateHelper.empty).toList

    result should equal(List(
      Map("a" -> 1, "let" -> false),
      Map("a" -> 2, "let" -> false)
    ))
  }

  test("should write let = true for everything if rhs is nonEmpty and the expression is false") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator(Map("a" -> 1)))

    val result =
      LetSelectOrSemiApplyPipe(lhs, rhs, "let", Not(True()), negated = false)()(newMonitor).
        createResults(QueryStateHelper.empty).toList

    result should equal(List(
      Map("a" -> 1, "let" -> true),
      Map("a" -> 2, "let" -> true)
    ))
  }

  test("if lhs is empty, rhs should not be touched regardless the given expression") {
    val rhs = pipeWithResults((_) => fail("should not use this"))
    val lhs = new FakePipe(Iterator.empty)

    // Should not throw
    LetSelectOrSemiApplyPipe(lhs, rhs, "let", True(), negated = false)()(newMonitor).
      createResults(QueryStateHelper.empty).toList
  }

  test("should write let = true for the one satisfying the expression even if the rhs is empty") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator.empty)

    val result =
      LetSelectOrSemiApplyPipe(lhs, rhs, "let", Equals(Identifier("a"), Literal(2)), negated = false)()(newMonitor).
        createResults(QueryStateHelper.empty).toList

    result should equal(List(
      Map("a" -> 1, "let" -> false),
      Map("a" -> 2, "let" -> true)
    ))
  }

  test("should write let = true for the one that matches and the one satisfying the expression") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2), Map("a" -> 3))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = pipeWithResults((state: QueryState) => {
      val initialContext = state.initialContext.get
      if (initialContext("a") == 1) Iterator(initialContext) else Iterator.empty
    })

    val result =
      LetSelectOrSemiApplyPipe(lhs, rhs, "let", Equals(Identifier("a"), Literal(2)), negated = false)()(newMonitor).
        createResults(QueryStateHelper.empty).toList

    result should equal(List(
      Map("a" -> 1, "let" -> true),
      Map("a" -> 2, "let" -> true),
      Map("a" -> 3, "let" -> false)
    ))
  }

  test("should not write let = true for anything if the rhs is empty and the expression is false") {
    val lhsData = List(Map("a" -> 3), Map("a" -> 4))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = pipeWithResults((state: QueryState) => {
      val initialContext = state.initialContext.get
      if (initialContext("a") == 1) Iterator(initialContext) else Iterator.empty
    })

    val result =
      LetSelectOrSemiApplyPipe(lhs, rhs, "let", Equals(Identifier("a"), Literal(2)), negated = false)()(newMonitor).
        createResults(QueryStateHelper.empty).toList
    result should equal(List(
      Map("a" -> 3, "let" -> false),
      Map("a" -> 4, "let" -> false)
    ))
  }
}
