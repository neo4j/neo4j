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

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.TestableIterator
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node


class NodeOuterHashJoinPipeTest extends CypherFunSuite {

  implicit val monitor = mock[PipeMonitor]
  val node1 = newMockedNode(1)
  val node2 = newMockedNode(2)
  val node3 = newMockedNode(3)

  test("should support simple hash join over nodes") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe("b",
      row("b" -> node1),
      row("b" -> node2))

    val right = newMockedPipe("b",
      row("b" -> node2, "a" -> 2),
      row("b" -> node3, "a" -> 3))

    // when
    val result = NodeOuterHashJoinPipe(Set("b"), left, right, Set("a"))().createResults(queryState)

    // then
    result.map(_("a")).toSet should equal(Set(null, 2))
  }

  test("should work when the inner pipe produces multiple rows with the same join key") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe("b",
      row("b" -> node1, "a" -> 10),
      row("b" -> node2, "a" -> 20))

    val right = newMockedPipe("b",
      row("b" -> node2, "c" -> 30),
      row("b" -> node2, "c" -> 40))

    // when
    val result = NodeOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map("a" -> 10, "b" -> node1, "c" -> null),
      Map("a" -> 20, "b" -> node2, "c" -> 30),
      Map("a" -> 20, "b" -> node2, "c" -> 40)
    ))
  }

  test("empty lhs should give empty results and not fetch anything from the rhs") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe("b")

    val right = mock[Pipe]
    when(right.sources).thenReturn(Seq.empty)
    when(right.symbols).thenReturn(SymbolTable(Map("b" -> CTNode)))
    val rhsIterator = new TestableIterator(Iterator(row("b" -> newMockedNode(0))))
    when(right.createResults(any())).thenReturn(rhsIterator)

    // when
    val result = NodeOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.toList shouldBe 'empty
    rhsIterator.fetched should equal(0)
  }

  test("empty rhs should give null results") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe("b",
      row("b" -> node1, "a" -> 10),
      row("b" -> node2, "a" -> 20),
      row("b" -> node3, "a" -> 30))

    val right = newMockedPipe("b")

    // when
    val result = NodeOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map("a" -> 10, "b" -> node1, "c" -> null),
      Map("a" -> 20, "b" -> node2, "c" -> null),
      Map("a" -> 30, "b" -> node3, "c" -> null)
    ))
  }

  test("lhs with null in the join key should not match anything on rhs") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe("b",
      row("b" -> node1, "a" -> 10),
      row("b" -> null,  "a" -> 20),
      row("b" -> node3, "a" -> 30))

    val right = newMockedPipe("b",
      row("b" -> node1, "c" -> 10),
      row("b" -> node2,  "c" -> 20),
      row("b" -> node3, "c" -> 30))

    // when
    val result = NodeOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map("a" -> 10, "b" -> node1, "c" -> 10),
      Map("a" -> 20, "b" -> null , "c" -> null),
      Map("a" -> 30, "b" -> node3, "c" -> 30)
    ))
  }

  test("rhs with null in the join key should not match anything") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe("b",
      row("b" -> node1, "a" -> 10),
      row("b" -> node2, "a" -> 20),
      row("b" -> node3, "a" -> 30))

    val right = newMockedPipe("b",
      row("b" -> null,  "c" -> 10),
      row("b" -> node2, "c" -> 20),
      row("b" -> node3, "c" -> 30))

    // when
    val result = NodeOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map("a" -> 10, "b" -> node1, "c" -> null),
      Map("a" -> 20, "b" -> node2, "c" -> 20),
      Map("a" -> 30, "b" -> node3, "c" -> 30)
    ))
  }

  test("null in both sides should still not match anything") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe("b",
      row("b" -> null,  "a" -> 20))

    val right = newMockedPipe("b",
      row("b" -> null,  "c" -> 20))

    // when
    val result = NodeOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map("a" -> 20, "b" -> null , "c" -> null)
    ))
  }

  test("should support joining on two different identifiers") {
    // given
    val queryState = QueryStateHelper.empty

    val left = newMockedPipe("b",
      row("a" -> node1, "b" -> node2, "c" -> 1),
      row("a" -> node1, "b" -> node3, "c" -> 2),
      row("a" -> node1, "b" -> node3, "c" -> 3),
      row("a" -> node2, "b" -> node3, "c" -> 4),
      row("a" -> node1, "b" -> null,  "c" -> 5))


    val right = newMockedPipe("b",
      row("a" -> node1, "b" -> node2, "d" -> 1),
      row("a" -> node1, "b" -> node3, "d" -> 2),
      row("a" -> node3, "b" -> node3, "d" -> 3),
      row("a" -> null, "b" -> node3,  "d" -> 4))

    // when
    val result = NodeOuterHashJoinPipe(Set("a","b"), left, right, Set("d"))().createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map("a" -> node1, "b" -> node2, "c" -> 1, "d" -> 1),
      Map("a" -> node1, "b" -> node3, "c" -> 2, "d" -> 2),
      Map("a" -> node1, "b" -> node3, "c" -> 3, "d" -> 2),
      Map("a" -> node2, "b" -> node3, "c" -> 4, "d" -> null),
      Map("a" -> node1, "b" -> null, "c" -> 5, "d" -> null)
    ))
  }

  private def row(values: (String, Any)*) = ExecutionContext.from(values: _*)

  private def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    when(node.toString).thenReturn(s"MockedNode($id)")
    node
  }

  private def newMockedPipe(node: String, rows: ExecutionContext*): Pipe = {
    val pipe = mock[Pipe]
    when(pipe.sources).thenReturn(Seq.empty)
    when(pipe.symbols).thenReturn(SymbolTable(Map(node -> CTNode)))
    when(pipe.createResults(any())).thenReturn(rows.iterator)

    pipe
  }
}
