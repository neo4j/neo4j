/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.pipes

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.{RowL, mockPipeFor, testableResult}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, QueryStateHelper}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class NodeHashJoinSlottedPipeTest extends CypherFunSuite {

  test("should support simple hash join over nodes") {
    // given
    val node1 = 1
    val node2 = 2
    val node3 = 3
    val queryState = QueryStateHelper.empty

    val slots = SlotConfiguration.empty.newLong("b", nullable = false, CTNode)

    val left = mockPipeFor(slots, RowL(node1), RowL(node2))
    val right = mockPipeFor(slots, RowL(node2), RowL(node3))

    // when
    val result = NodeHashJoinSlottedPipe(Array(0), Array(0), left, right, slots, Array(), Array())().createResults(queryState)

    // then
    val list: Iterator[ExecutionContext] = result
    testableResult(list, slots) should equal(List(Map("b" -> node2)))
  }

  test("should support joining on two different variables") {
    // given
    val queryState = QueryStateHelper.empty

    val leftSlots = SlotConfiguration.empty
    leftSlots.newLong("a", nullable = false, CTNode)
    leftSlots.newLong("b", nullable = false, CTNode)
    leftSlots.newLong("c", nullable = false, CTNode)
    val rightSlots = SlotConfiguration.empty
    rightSlots.newLong("a", nullable = false, CTNode)
    rightSlots.newLong("b", nullable = false, CTNode)
    rightSlots.newLong("d", nullable = false, CTNode)
    val hashSlots = SlotConfiguration.empty
    hashSlots.newLong("a", nullable = false, CTNode)
    hashSlots.newLong("b", nullable = false, CTNode)
    hashSlots.newLong("c", nullable = false, CTNode)
    hashSlots.newLong("d", nullable = false, CTNode)

    val left = mockPipeFor(leftSlots,
      RowL(node0, node1, node1),
      RowL(node0, node2, node2),
      RowL(node0, node2, node3),
      RowL(node1, node2, node4),
      RowL(node0, NULL, node5)
    )

    val right = mockPipeFor(rightSlots,
      RowL(node0, node1, node1),
      RowL(node0, node2, node2),
      RowL(node2, node2, node3),
      RowL(NULL, node2, node4)
    )

    // when
    val result = NodeHashJoinSlottedPipe(Array(0, 1), Array(0, 1), left, right, hashSlots, Array((2, 3)), Array())().
      createResults(queryState)

    // then
    testableResult(result, hashSlots).toSet should equal(Set(
      Map("a" -> node0, "b" -> node1, "c" -> node1, "d" -> node1),
      Map("a" -> node0, "b" -> node2, "c" -> node2, "d" -> node2),
      Map("a" -> node0, "b" -> node2, "c" -> node3, "d" -> node2)
    ))
  }

  test("should not fetch results from RHS if LHS is empty") {
    // given
    val queryState = QueryStateHelper.empty

    val slots = SlotConfiguration.empty
    slots.newLong("a", nullable = false, CTNode)

    val left = mockPipeFor(slots)


    val right = mock[Pipe]

    // when
    val result = NodeHashJoinSlottedPipe(Array(0, 1), Array(0, 1), left, right, slots, Array(), Array())().
      createResults(queryState)

    // then
    result should be(empty)
    verifyZeroInteractions(right)
  }

  test("should not fetch results from RHS if LHS did not contain any nodes that can be hashed against") {
    // given
    val queryState = QueryStateHelper.empty

    val slots = SlotConfiguration.empty
    slots.newLong("a", nullable = false, CTNode)

    val left = mockPipeFor(slots, RowL(NULL))
    val right = mockPipeFor(slots, RowL(node0))

    // when
    val result = NodeHashJoinSlottedPipe(Array(0, 1), Array(0, 1), left, right, slots, Array(), Array())().
      createResults(queryState)

    // then
    result should be(empty)
    verify(right, times(1)).createResults(any())
    verifyNoMoreInteractions(right)
  }

  private val node0 = 0
  private val node1 = 1
  private val node2 = 2
  private val node3 = 3
  private val node4 = 4
  private val node5 = 5
  private val NULL = -1

}
