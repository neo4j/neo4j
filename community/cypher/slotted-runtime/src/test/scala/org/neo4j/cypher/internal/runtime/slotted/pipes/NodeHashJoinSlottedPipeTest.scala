/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.Mockito.verifyNoMoreInteractions
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationBuilder
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.SlotMappings
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.RowL
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.mockPipeFor
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.KeyOffsets
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.SlotMapping
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.collection

import scala.collection.immutable

class NodeHashJoinSlottedPipeTest extends CypherFunSuite {

  private val node0 = 0
  private val NULL = -1

  test("should not fetch results from RHS if LHS is empty") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val slots = SlotConfigurationBuilder.empty
      .newLong("a", nullable = false, CTNode)
      .build()

    val left = mockPipeFor(slots)

    val right = mock[Pipe]

    // when
    val result = NodeHashJoinSlottedPipe(
      KeyOffsets.longs(0, 1),
      KeyOffsets.longs(0, 1),
      left,
      right,
      slots,
      SlotMappings(Array(), Array())
    )().createResults(queryState)

    // then
    result should be(empty)
    verifyNoInteractions(right)
  }

  test("should not fetch results from RHS if LHS did not contain any nodes that can be hashed against") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val slots = SlotConfigurationBuilder.empty
      .newLong("a", nullable = false, CTNode)
      .build()

    val left = mockPipeFor(slots, RowL(NULL))
    val right = mockPipeFor(slots, RowL(node0))

    // when
    val result = NodeHashJoinSlottedPipe(
      KeyOffsets.longs(0, 1),
      KeyOffsets.longs(0, 1),
      left,
      right,
      slots,
      SlotMappings(Array(), Array())
    )().createResults(queryState)

    // then
    result should be(empty)
    verify(right).createResults(any())
    verifyNoMoreInteractions(right)
  }

  test("worst case scenario should not lead to stackoverflow errors") {
    // This test case lead to stack overflow errors.
    // It's the worst case - large inputs on both sides that have no overlap on the join column
    val size = 10000
    val a_b: immutable.Seq[RowL] = (0 to size) map { i =>
      RowL(i.toLong, i.toLong)
    }
    val b_c: immutable.Seq[RowL] = (size + 1 to size * 2) map { i =>
      RowL(i.toLong, i.toLong)
    }

    val lhs = SlotConfigurationBuilder.empty
      .newLong("a", nullable = false, CTNode)
      .newLong("b", nullable = false, CTNode)
      .build()

    val rhs = SlotConfigurationBuilder.empty
      .newLong("b", nullable = false, CTNode)
      .newLong("c", nullable = false, CTNode)
      .build()

    val output = SlotConfigurationBuilder.empty
      .newLong("a", nullable = false, CTNode)
      .newLong("b", nullable = false, CTNode)
      .newLong("c", nullable = false, CTNode)
      .build()

    val lhsPipe = mockPipeFor(lhs, a_b: _*)
    val rhsPipe = mockPipeFor(rhs, b_c: _*)

    // when
    val result = NodeHashJoinSlottedPipe(
      lhsKeyOffsets = KeyOffsets.longs(0, 1),
      rhsKeyOffsets = KeyOffsets.longs(0, 1),
      left = lhsPipe,
      right = rhsPipe,
      slots = output,
      rhsSlotMappings = SlotMappings(
        slotMapping = Array(SlotMapping(1, 2, true, true)),
        cachedPropertyMappings = Array()
      )
    )().createResults(QueryStateHelper.emptyWithValueSerialization)

    // If we got here it means we did not throw a stack overflow exception. ooo-eeh!
    result should be(empty)
  }

  test("exhaust should close table") {
    // given
    val monitor = QueryStateHelper.trackClosedMonitor
    val queryState = QueryStateHelper.emptyWithResourceManager(new ResourceManager(monitor))

    val slots = SlotConfigurationBuilder.empty
      .newLong("n", nullable = false, CTNode)
      .build()

    val left = mockPipeFor(slots, RowL(node0))
    val right = mockPipeFor(slots, RowL(node0))

    // when
    NodeHashJoinSlottedPipe(
      KeyOffsets.longs(0),
      KeyOffsets.longs(0),
      left,
      right,
      slots,
      SlotMappings(Array(SlotMapping(0, 0, true, true)), Array())
    )().createResults(queryState).toList

    // then
    monitor.closedResources.collect { case t: collection.ProbeTable[_, _] => t } should have size (1)
  }

  test("close should close table") {
    // given
    val monitor = QueryStateHelper.trackClosedMonitor
    val queryState = QueryStateHelper.emptyWithResourceManager(new ResourceManager(monitor))

    val slots = SlotConfigurationBuilder.empty
      .newLong("n", nullable = false, CTNode)
      .build()

    val left = mockPipeFor(slots, RowL(node0))
    val right = mockPipeFor(slots, RowL(node0))

    // when
    val result = NodeHashJoinSlottedPipe(
      KeyOffsets.longs(0),
      KeyOffsets.longs(0),
      left,
      right,
      slots,
      SlotMappings(Array(SlotMapping(0, 0, true, true)), Array())
    )().createResults(queryState)
    result.close()

    // then
    monitor.closedResources.collect { case t: collection.ProbeTable[_, _] => t } should have size (1)
  }
}
