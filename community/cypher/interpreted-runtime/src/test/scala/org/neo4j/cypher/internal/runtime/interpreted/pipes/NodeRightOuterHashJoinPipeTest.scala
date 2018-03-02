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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion._
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, QueryStateHelper, TestableIterator}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.kernel.impl.util.ValueUtils.fromNodeProxy
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.{NO_VALUE, intValue}

class NodeRightOuterHashJoinPipeTest extends CypherFunSuite {

  val node1 = newMockedNode(1)
  val node2 = newMockedNode(2)
  val node3 = newMockedNode(3)

  test("should support simple hash join over nodes") {
    // given
    val queryState = QueryStateHelper.empty

    val right = newMockedPipe(
                             row("b" -> fromNodeProxy(node1)),
                             row("b" -> fromNodeProxy(node2)))

    val left = newMockedPipe(
      row("b" -> fromNodeProxy(node2), "a" -> intValue(2)),
      row("b" -> fromNodeProxy(node3), "a" -> intValue(3)))

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set("a"))().createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map("b" -> fromNodeProxy(node1), "a" -> NO_VALUE),
      Map("b" -> fromNodeProxy(node2), "a" -> intValue(2))
    ))
  }

  test("should work when the inner pipe produces multiple rows with the same join key") {
    // given
    val queryState = QueryStateHelper.empty

    val right = newMockedPipe(
      row("b" -> fromNodeProxy(node1), "a" -> intValue(10)),
      row("b" -> fromNodeProxy(node2), "a" -> intValue(20)))

    val left = newMockedPipe(
      row("b" -> fromNodeProxy(node2), "c" -> intValue(30)),
      row("b" -> fromNodeProxy(node2), "c" -> intValue(40)))

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map("a" -> intValue(10), "b" -> fromNodeProxy(node1), "c" -> NO_VALUE),
      Map("a" -> intValue(20), "b" -> fromNodeProxy(node2), "c" -> intValue(30)),
      Map("a" -> intValue(20), "b" -> fromNodeProxy(node2), "c" -> intValue(40))
    ))
  }

  test("empty rhs should give empty results and not fetch anything from the lhs") {
    // given
    val queryState = QueryStateHelper.empty

    val right = newMockedPipe()

    val left = mock[Pipe]
    val lhsIterator = new TestableIterator(Iterator(row("b" -> newMockedNode(0))))
    when(left.createResults(any())).thenReturn(lhsIterator)

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.toList shouldBe 'empty
    lhsIterator.fetched should equal(0)
  }

  test("empty lhs should give null results") {
    // given
    val queryState = QueryStateHelper.empty

    val right = newMockedPipe(
      row("b" -> fromNodeProxy(node1), "a" -> intValue(10)),
      row("b" -> fromNodeProxy(node2), "a" -> intValue(20)),
      row("b" -> fromNodeProxy(node3), "a" -> intValue(30)))

    val left = newMockedPipe()

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map("a" -> intValue(10), "b" -> fromNodeProxy(node1), "c" -> NO_VALUE),
      Map("a" -> intValue(20), "b" -> fromNodeProxy(node2), "c" -> NO_VALUE),
      Map("a" -> intValue(30), "b" -> fromNodeProxy(node3), "c" -> NO_VALUE)
    ))
  }

  test("rhs with null in the join key should not match anything on lhs") {
    // given
    val queryState = QueryStateHelper.empty

    val right = newMockedPipe(
      row("b" -> fromNodeProxy(node1), "a" -> intValue(10)),
      row("b" -> NO_VALUE,  "a" -> intValue(20)),
      row("b" -> fromNodeProxy(node3), "a" -> intValue(30)))

    val left = newMockedPipe(
      row("b" -> fromNodeProxy(node1), "c" -> intValue(10)),
      row("b" -> fromNodeProxy(node2), "c" -> intValue(20)),
      row("b" -> fromNodeProxy(node3), "c" -> intValue(30)))

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map("a" -> intValue(10), "b" -> fromNodeProxy(node1), "c" -> intValue(10)),
      Map("a" -> intValue(20), "b" -> NO_VALUE , "c" -> NO_VALUE),
      Map("a" -> intValue(30), "b" -> fromNodeProxy(node3), "c" -> intValue(30))
    ))
  }

  test("lhs with null in the join key should not match anything") {
    // given
    val queryState = QueryStateHelper.empty

    val right = newMockedPipe(
      row("b" -> fromNodeProxy(node1), "a" -> intValue(10)),
      row("b" -> fromNodeProxy(node2), "a" -> intValue(20)),
      row("b" -> fromNodeProxy(node3), "a" -> intValue(30)))

    val left = newMockedPipe(
      row("b" -> NO_VALUE,  "c" -> intValue(10)),
      row("b" -> fromNodeProxy(node2), "c" -> intValue(20)),
      row("b" -> fromNodeProxy(node3), "c" -> intValue(30)))

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map("a" -> intValue(10), "b" -> fromNodeProxy(node1), "c" -> NO_VALUE),
      Map("a" -> intValue(20), "b" -> fromNodeProxy(node2), "c" -> intValue(20)),
      Map("a" -> intValue(30), "b" -> fromNodeProxy(node3), "c" -> intValue(30))
    ))
  }

  test("null in both sides should still not match anything") {
    // given
    val queryState = QueryStateHelper.empty

    val right = newMockedPipe(
      row("b" -> NO_VALUE,  "a" -> intValue(20)))

    val left = newMockedPipe(
      row("b" -> NO_VALUE,  "c" -> intValue(20)))

    // when
    val result = NodeRightOuterHashJoinPipe(Set("b"), left, right, Set("c"))().createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map("a" -> intValue(20), "b" -> NO_VALUE , "c" -> NO_VALUE)
    ))
  }

  test("should support joining on two different variables") {
    // given
    val queryState = QueryStateHelper.empty

    val right = newMockedPipe(
      row("a" -> fromNodeProxy(node1), "b" -> fromNodeProxy(node2), "c" -> intValue(1)),
      row("a" -> fromNodeProxy(node1), "b" -> fromNodeProxy(node3), "c" -> intValue(2)),
      row("a" -> fromNodeProxy(node1), "b" -> fromNodeProxy(node3), "c" -> intValue(3)),
      row("a" -> fromNodeProxy(node2), "b" -> fromNodeProxy(node3), "c" -> intValue(4)),
      row("a" -> fromNodeProxy(node1), "b" -> NO_VALUE,  "c" -> intValue(5)))


    val left = newMockedPipe(
      row("a" -> fromNodeProxy(node1), "b" -> fromNodeProxy(node2), "d" -> intValue(1)),
      row("a" -> fromNodeProxy(node1), "b" -> fromNodeProxy(node3), "d" -> intValue(2)),
      row("a" -> fromNodeProxy(node3), "b" -> fromNodeProxy(node3), "d" -> intValue(3)),
      row("a" -> NO_VALUE, "b" -> fromNodeProxy(node3),  "d" -> intValue(4)))

    // when
    val result = NodeRightOuterHashJoinPipe(Set("a","b"), left, right, Set("d"))().createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map("a" -> fromNodeProxy(node1), "b" -> fromNodeProxy(node2), "c" -> intValue(1), "d" -> intValue(1)),
      Map("a" -> fromNodeProxy(node1), "b" -> fromNodeProxy(node3), "c" -> intValue(2), "d" -> intValue(2)),
      Map("a" -> fromNodeProxy(node1), "b" -> fromNodeProxy(node3), "c" -> intValue(3), "d" -> intValue(2)),
      Map("a" -> fromNodeProxy(node2), "b" -> fromNodeProxy(node3), "c" -> intValue(4), "d" -> NO_VALUE),
      Map("a" -> fromNodeProxy(node1), "b" -> NO_VALUE, "c" -> intValue(5), "d" -> NO_VALUE)
    ))
  }

  private def row(values: (String, AnyValue)*) = ExecutionContext.from(values: _*)

  private def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    when(node.toString).thenReturn(s"MockedNode($id)")
    node
  }

  private def newMockedPipe(rows: ExecutionContext*): Pipe = {
    val pipe = mock[Pipe]
    when(pipe.createResults(any())).thenReturn(rows.iterator)

    pipe
  }
}
