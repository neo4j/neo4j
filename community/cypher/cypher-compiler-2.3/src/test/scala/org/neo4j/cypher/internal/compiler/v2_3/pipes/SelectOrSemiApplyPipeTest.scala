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
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Identifier, Literal}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class SelectOrSemiApplyPipeTest extends CypherFunSuite with PipeTestSupport {

  test("should only let through the one that matches when the expression is false") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = pipeWithResults((state) => {
      val initialContext = state.initialContext.get
      if (initialContext("a") == 1) Iterator(initialContext) else Iterator.empty
    })

    val result =
      SelectOrSemiApplyPipe(lhs, rhs, Not(True()), negated = false)()(newMonitor).
        createResults(QueryStateHelper.empty).toList

    result should equal(List(Map("a" -> 1)))
  }

  test("should only let through the one that not matches when the expression is false and it is negated") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = pipeWithResults((state) => {
      val initialContext = state.initialContext.get
      if (initialContext("a") == 1) Iterator(initialContext) else Iterator.empty
    })

    val result =
      SelectOrSemiApplyPipe(lhs, rhs, Not(True()), negated = true)()(newMonitor).
        createResults(QueryStateHelper.empty).toList

    result should equal(List(Map("a" -> 2)))
  }

  test("should not let anything through if rhs is empty and expression is false") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator.empty)

    val result =
      SelectOrSemiApplyPipe(lhs, rhs, Not(True()), negated = false)()(newMonitor).
        createResults(QueryStateHelper.empty).toList

    result should equal(List.empty)
  }

  test("should let everything through if rhs is nonEmpty and the expression is false") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator(Map("a" -> 1)))

    val result =
      SelectOrSemiApplyPipe(lhs, rhs, Not(True()), negated = false)()(newMonitor).
        createResults(QueryStateHelper.empty).toList

    result should equal(lhsData)
  }

  test("if lhs is empty, rhs should not be touched regardless the given expression") {
    val rhs = pipeWithResults((_) => fail("should not use this"))
    val lhs = new FakePipe(Iterator.empty)

    // Should not throw
    SelectOrSemiApplyPipe(lhs, rhs, True(), negated = false)()(newMonitor).
      createResults(QueryStateHelper.empty).toList
  }

  test("should let pass the one satisfying the expression even if the rhs is empty") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)
    val rhs = new FakePipe(Iterator.empty)

    val result =
      SelectOrSemiApplyPipe(lhs, rhs, Equals(Identifier("a"), Literal(2)), negated = false)()(newMonitor).createResults(QueryStateHelper.empty).toList

    result should equal(List(Map("a" -> 2)))
  }

  test("should let through the one that matches and the one satisfying the expression") {
    val lhsData = List(Map("a" -> 1), Map("a" -> 2), Map("a" -> 3))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = pipeWithResults((state: QueryState) => {
        val initialContext = state.initialContext.get
        if (initialContext("a") == 1) Iterator(initialContext) else Iterator.empty
      })

    val result = SelectOrSemiApplyPipe(lhs, rhs, Equals(Identifier("a"), Literal(2)), negated = false)()(newMonitor).createResults(QueryStateHelper.empty).toList

    result should equal(List(Map("a" -> 1), Map("a" -> 2)))
  }

  test("should let pass nothing if the rhs is empty and the expression is false") {
    val lhsData = List(Map("a" -> 3), Map("a" -> 4))
    val lhs = new FakePipe(lhsData.iterator, "a" -> CTNumber)

    val rhs = pipeWithResults((state: QueryState) => {
        val initialContext = state.initialContext.get
        if (initialContext("a") == 1) Iterator(initialContext) else Iterator.empty
      })

    val result = SelectOrSemiApplyPipe(lhs, rhs, Equals(Identifier("a"), Literal(2)), negated = false)()(newMonitor).createResults(QueryStateHelper.empty).toList
    result should equal(List.empty)
  }
}
