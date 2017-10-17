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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.mockito.Matchers._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ImplicitValueConversion._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v3_3.test_helpers.TestableIterator
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.kernel.impl.util.ValueUtils.fromNodeProxy
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.{NO_VALUE, intValue}

class NodeHashJoinPipeTest extends CypherFunSuite {

  import org.mockito.Mockito._

  test("should support simple hash join over nodes") {
    // given
    val node1 = newMockedNode(1)
    val node2 = newMockedNode(2)
    val node3 = newMockedNode(3)
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(left.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]]() {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[ExecutionContext] =
        Iterator(row("b" -> fromNodeProxy(node1)), row("b" -> fromNodeProxy(node2)))
    })

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]]() {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[ExecutionContext] =
        Iterator(row("b" -> fromNodeProxy(node2)), row("b" -> fromNodeProxy(node3)))
    })

    // when
    val result = NodeHashJoinPipe(Set("b"), left, right)().createResults(queryState)

    // then
    result.map(_("b")).toList should equal(List(fromNodeProxy(node2)))
  }

  test("should support joining on two different variables") {
    // given
    val node0 = newMockedNode(0)
    val node1 = newMockedNode(1)
    val node2 = newMockedNode(2)
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(left.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]]() {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[ExecutionContext] =
        Iterator(
          row("a" -> fromNodeProxy(node0), "b" -> fromNodeProxy(node1), "c" -> intValue(1)),
          row("a" -> fromNodeProxy(node0), "b" -> fromNodeProxy(node2), "c" -> intValue(2)),
          row("a" -> fromNodeProxy(node0), "b" -> fromNodeProxy(node2), "c" -> intValue(3)),
          row("a" -> fromNodeProxy(node1), "b" -> fromNodeProxy(node2), "c" -> intValue(4)),
          row("a" -> fromNodeProxy(node0), "b" -> NO_VALUE, "c" -> intValue(5)))
    })


    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]]() {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[ExecutionContext] =
        Iterator(
          row("a" -> fromNodeProxy(node0), "b" -> fromNodeProxy(node1), "d" -> intValue(1)),
          row("a" -> fromNodeProxy(node0), "b" -> fromNodeProxy(node2), "d" -> intValue(2)),
          row("a" -> fromNodeProxy(node2), "b" -> fromNodeProxy(node2), "d" -> intValue(3)),
          row("a" -> NO_VALUE,  "b" -> fromNodeProxy(node2),  "d" -> intValue(4)))
    })

    // when
    val result = NodeHashJoinPipe(Set("a", "b"), left, right)().createResults(queryState).toList

    // then
    result should equal(List(
      Map("a"->fromNodeProxy(node0), "b"->fromNodeProxy(node1), "c" -> intValue(1), "d" -> intValue(1)),
      Map("a"->fromNodeProxy(node0), "b"->fromNodeProxy(node2), "c" -> intValue(2), "d" -> intValue(2)),
      Map("a"->fromNodeProxy(node0), "b"->fromNodeProxy(node2), "c" -> intValue(3), "d" -> intValue(2))
    ))
  }

  test("should work when the inner pipe produces multiple rows with the same join key") {
    // given
    val node1 = newMockedNode(1)
    val node2 = newMockedNode(2)
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(left.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]]() {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[ExecutionContext] =
        Iterator(row("b" -> fromNodeProxy(node1), "a" -> intValue(10)), row("b" -> fromNodeProxy(node2), "a" -> intValue(20)))
    })

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]]() {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[ExecutionContext] =
        Iterator(row("b" -> fromNodeProxy(node2), "c" -> intValue(30)), row("b" -> fromNodeProxy(node2), "c" -> intValue(40)))
    })

    // when
    val result = NodeHashJoinPipe(Set("b"), left, right)().createResults(queryState)

    // then
    result.toList should equal(List(
      Map("a" -> intValue(20), "b" -> fromNodeProxy(node2), "c" -> intValue(30)),
      Map("a" -> intValue(20), "b" -> fromNodeProxy(node2), "c" -> intValue(40))
    ))
  }

  test("should work when the outer pipe produces rows with a null key") {
    // given
    val node1 = newMockedNode(1)
    val node2 = newMockedNode(2)
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(left.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]]() {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[ExecutionContext] =
        Iterator(row("b" -> NO_VALUE, "a" -> intValue(10)), row("b" -> fromNodeProxy(node2), "a" -> intValue(20)))
    })

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]]() {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[ExecutionContext] =
        Iterator(row("b" -> fromNodeProxy(node2), "c" -> intValue(30)), row("b" -> fromNodeProxy(node1), "c" -> intValue(40)))
    })

    // when
    val result = NodeHashJoinPipe(Set("b"), left, right)().createResults(queryState)

    // then
    result.toList should equal(List(
      Map("a" -> intValue(20), "b" -> fromNodeProxy(node2), "c" -> intValue(30))
    ))
  }

  test("should work when the inner pipe produces rows with a null key") {
    // given
    val node1 = newMockedNode(1)
    val node2 = newMockedNode(2)
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(left.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]]() {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[ExecutionContext] =
        Iterator(row("b" -> fromNodeProxy(node2), "a" -> intValue(10)), row("b" -> fromNodeProxy(node1), "a" -> intValue(20)))
    })

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]]() {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[ExecutionContext] =
        Iterator(row("b" -> NO_VALUE, "c" -> intValue(30)), row("b" -> fromNodeProxy(node2), "c" -> intValue(40)))
    })

    // when
    val result = NodeHashJoinPipe(Set("b"), left, right)().createResults(queryState)

    // then
    result.toList should equal(List(
      Map("a" -> intValue(10), "b" -> fromNodeProxy(node2), "c" -> intValue(40))
    ))
  }

  test("should not fetch results from RHS if LHS is empty") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator.empty)

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))

    // when
    val result = NodeHashJoinPipe(Set("b"), left, right)().createResults(queryState)

    // then
    result shouldBe empty
    verify(right, times(0)).createResults(any())
  }

  test("should not fetch results from RHS if no probe table was built") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("b" -> null), row("b" -> null)))

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    val rhsIterator = new TestableIterator(Iterator(row("b" -> newMockedNode(0))))
    when(left.createResults(queryState)).thenReturn(Iterator.empty)

    // when
    val result = NodeHashJoinPipe(Set("b"), left, right)().createResults(queryState)

    // then
    result shouldBe empty
    rhsIterator.fetched should equal(0)
  }

  test("if RHS is empty, terminate building of the probe map early") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    val node1 = newMockedNode(1)
    val node2 = newMockedNode(2)

    val lhsIterator = new TestableIterator(Iterator(row("b" -> node1), row("b" -> node2)))
    when(left.createResults(queryState)).thenReturn(lhsIterator)

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenReturn(Iterator.empty)

    // when
    val result = NodeHashJoinPipe(Set("b"), left, right)().createResults(queryState)

    // then
    result shouldBe empty
    lhsIterator.fetched should equal(0)
  }

  private def row(values: (String, AnyValue)*) = ExecutionContext.from(values: _*)

  private def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    when(node.toString).thenReturn("node - " + id.toString)
    node
  }

  private def newMockedPipe(symbolTable: SymbolTable): Pipe = {
    val pipe = mock[Pipe]
    pipe
  }
}

