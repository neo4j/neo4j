/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.mockito.Matchers._
import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.Variable
import org.neo4j.cypher.internal.compiler.v3_2.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v3_2.test_helpers.TestableIterator
import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node

class ValueHashJoinPipeTest extends CypherFunSuite {

  implicit val monitor = mock[PipeMonitor]

  import org.mockito.Mockito._

  test("should support simple hash join between two identifiers") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("a" -> CTInteger)))
    when(left.createResults(queryState)).thenReturn(rows("a", 1, 2))

    val right = newMockedPipe(SymbolTable(Map("b" -> CTInteger)))
    when(right.createResults(queryState)).thenReturn(rows("b", 2, 3))

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)

    // then
    result.toList should equal(List(Map("a" -> 2, "b" -> 2)))
  }

  test("should handle nulls") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("a" -> CTInteger)))
    when(left.createResults(queryState)).thenReturn(rows("a", 1, 2, null))

    val right = newMockedPipe(SymbolTable(Map("b" -> CTInteger)))
    when(right.createResults(queryState)).thenReturn(rows("b", 2, 3, null))

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)

    // then
    result.toList should equal(List(Map("a" -> 2, "b" -> 2)))
  }

  test("should handle multiples on both sides") {
    // given
    val leftSide = Iterator(
      row("a" -> 1, "a2" -> 1),
      row("a" -> 1, "a2" -> 2),
      row("a" -> 2, "a2" -> 3),
      row("a" -> 3, "a2" -> 4))

    val rightSide = Iterator(
      row("b" -> 1, "b2" -> 1),
      row("b" -> 2, "b2" -> 2),
      row("b" -> 2, "b2" -> 3),
      row("b" -> 4, "b2" -> 4))

    val queryState = QueryStateHelper.empty


    val left = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(leftSide)

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenReturn(rightSide)

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map("a" -> 1, "b" -> 1, "a2" -> 1, "b2" -> 1),
      Map("a" -> 1, "b" -> 1, "a2" -> 2, "b2" -> 1),
      Map("a" -> 2, "b" -> 2, "a2" -> 3, "b2" -> 2),
      Map("a" -> 2, "b" -> 2, "a2" -> 3, "b2" -> 3)
    ))
  }

  test("should not fetch results from RHS if LHS is empty") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("a" -> CTInteger)))
    when(left.createResults(queryState)).thenReturn(Iterator.empty)

    val right = newMockedPipe(SymbolTable(Map("b" -> CTInteger)))

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)

    // then
    result shouldBe empty
    verify(right, times(0)).createResults(any())
  }

  test("should not fetch results from RHS if no probe table was built") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("a" -> CTInteger)))
    when(left.createResults(queryState)).thenReturn(rows("a", null, null, null))

    val right = newMockedPipe(SymbolTable(Map("b" -> CTInteger)))
    val rhsIter = new TestableIterator(rows("b", 1, 2, 3))
    when(right.createResults(queryState)).thenReturn(rhsIter)

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)

    // then
    result shouldBe empty
    rhsIter.fetched should equal(0)
  }


  test("if RHS is empty, terminate building of the probe map early") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("b" -> CTInteger)))

    val lhsIterator = new TestableIterator(rows("a", 1, 2, 3))
    when(left.createResults(queryState)).thenReturn(lhsIterator)

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenReturn(Iterator.empty)

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)

    // then
    result shouldBe empty
    lhsIterator.fetched should equal(0)
  }

  test("should support joining on arrays") {
    // given
    val ints = Array(1, 2, 3)
    val doubles = Array(1.0, 2.0, 3.0)

    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("a" -> CTInteger)))
    when(left.createResults(queryState)).thenReturn(rows("a", ints, Array(2, 3, 4)))

    val right = newMockedPipe(SymbolTable(Map("b" -> CTInteger)))
    when(right.createResults(queryState)).thenReturn(rows("b",  doubles, Array(0, 1, 2)))

    // when
    val result = ValueHashJoinPipe(Variable("a"), Variable("b"), left, right)().createResults(queryState)

    // then
    result.toList should equal(List(Map("a" -> ints, "b" ->  doubles)))
  }


  private def row(values: (String, Any)*) = ExecutionContext.from(values: _*)

  private def rows(variable: String, values: Any*): Iterator[ExecutionContext] =
    values.map(x => ExecutionContext.from(variable -> x)).iterator

  private def newMockedPipe(symbolTable: SymbolTable): Pipe = {
    val pipe = mock[Pipe]
    when(pipe.sources).thenReturn(Seq.empty)
    when(pipe.symbols).thenReturn(symbolTable)
    pipe
  }
}

