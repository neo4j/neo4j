/**
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.mockito.Mockito
import org.neo4j.graphdb.Node
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext

class NodeHashJoinPipeTest extends CypherFunSuite {

  implicit val monitor = mock[PipeMonitor]
  import Mockito.when

  test("should support simple hash join over nodes") {
    // given
    val node1 = newMockedNode(1)
    val node2 = newMockedNode(2)
    val node3 = newMockedNode(3)
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("b" -> node1), row("b" -> node2)))

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenReturn(Iterator(row("b" -> node2), row("b" -> node3)))

    // when
    val result = NodeHashJoinPipe("b", left, right).createResults(queryState)

    // then
    result.map(_("b")).toList should equal(List(node2))
  }

  test("should work when the inner pipe produces multiple rows with the same join key") {
    // given
    val node1 = newMockedNode(1)
    val node2 = newMockedNode(2)
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("b" -> node1, "a" -> 10), row("b" -> node2, "a" -> 20)))

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenReturn(Iterator(row("b" -> node2, "c" -> 30), row("b" -> node2, "c" -> 40)))

    // when
    val result = NodeHashJoinPipe("b", left, right).createResults(queryState)

    // then
    result.toList should equal(List(
      Map("a" -> 20, "b" -> node2, "c" -> 30),
      Map("a" -> 20, "b" -> node2, "c" -> 40)
    ))
  }

  test("should work when the outer pipe produces rows with a null key") {
    // given
    val node1 = newMockedNode(1)
    val node2 = newMockedNode(2)
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("b" -> null, "a" -> 10), row("b" -> node2, "a" -> 20)))

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenReturn(Iterator(row("b" -> node2, "c" -> 30), row("b" -> node1, "c" -> 40)))

    // when
    val result = NodeHashJoinPipe("b", left, right).createResults(queryState)

    // then
    result.toList should equal(List(
      Map("a" -> 20, "b" -> node2, "c" -> 30)
    ))
  }

  test("should work when the inner pipe produces rows with a null key") {
    // given
    val node1 = newMockedNode(1)
    val node2 = newMockedNode(2)
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("b" -> node2, "a" -> 10), row("b" -> node1, "a" -> 20)))

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenReturn(Iterator(row("b" -> null, "c" -> 30), row("b" -> node2, "c" -> 40)))

    // when
    val result = NodeHashJoinPipe("b", left, right).createResults(queryState)

    // then
    result.toList should equal(List(
      Map("a" -> 10, "b" -> node2, "c" -> 40)
    ))
  }


  private def row(values: (String, Any)*) = ExecutionContext.from(values: _*)

  private def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    node
  }

  private def newMockedPipe(symbolTable: SymbolTable): Pipe = {
    val pipe = mock[Pipe]
    when(pipe.sources).thenReturn(Seq.empty)
    when(pipe.symbols).thenReturn(symbolTable)
    pipe
  }
}
