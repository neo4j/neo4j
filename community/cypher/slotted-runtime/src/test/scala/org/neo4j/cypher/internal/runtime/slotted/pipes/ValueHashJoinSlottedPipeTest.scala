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
import org.neo4j.cypher.internal.runtime.slotted.expressions.ReferenceFromSlot
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.Longs
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.Refs
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.RowR
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.RowRL
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.mockPipeFor
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.testableResult
import org.neo4j.cypher.internal.runtime.slotted.pipes.NodeHashJoinSlottedPipe.SlotMapping
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.collection
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.intValue

class ValueHashJoinSlottedPipeTest extends CypherFunSuite {

  test("should not fetch results from RHS if LHS is empty") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val slotInfo = SlotConfigurationBuilder.empty
      .newLong("a", nullable = false, CTNode)
      .build()

    val left = mockPipeFor(slotInfo)

    val right = mock[Pipe]
    val pipe = ValueHashJoinSlottedPipe(
      ReferenceFromSlot(0),
      ReferenceFromSlot(0),
      left,
      right,
      slotInfo,
      SlotMappings(Array(SlotMapping(0, 0, true, true), SlotMapping(1, 1, false, false)), Array.empty)
    )()

    // when
    val result = pipe.createResults(queryState)

    // then
    result should be(empty)
    verifyNoInteractions(right)
  }

  test("should not fetch results from RHS if LHS did not contain any nodes that can be hashed against") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization

    val slotInfo = SlotConfigurationBuilder.empty
      .newReference("a", nullable = false, CTNode)
      .build()

    val left = mockPipeFor(slotInfo, RowR(NO_VALUE))
    val right = mockPipeFor(slotInfo, RowR(intValue(42)))

    val pipe = ValueHashJoinSlottedPipe(
      ReferenceFromSlot(0),
      ReferenceFromSlot(0),
      left,
      right,
      slotInfo,
      SlotMappings(Array(SlotMapping(0, 0, true, true), SlotMapping(1, 1, false, false)), Array.empty)
    )()

    // when
    val result = pipe.createResults(queryState)

    // then
    result should be(empty)
    verify(right).createResults(any())
    verifyNoMoreInteractions(right)
  }

  test("should support hash join between two identifiers with shared arguments") {
    // given
    val queryState = QueryStateHelper.emptyWithValueSerialization
    val slotInfoForInputs = SlotConfigurationBuilder.empty
      .newLong("arg1", nullable = false, CTNode)
      .newReference("arg2", nullable = false, CTInteger)
      .newReference("b", nullable = false, CTInteger)
      .build()

    val slotInfoForJoin = SlotConfigurationBuilder.empty
      .newLong("arg1", nullable = false, CTNode)
      .newReference("arg2", nullable = false, CTInteger)
      .newReference("a", nullable = false, CTInteger)
      .newReference("b", nullable = false, CTInteger)
      .build()

    val left = mockPipeFor(
      slotInfoForInputs,
      RowRL(Longs(42), Refs(intValue(666), intValue(1))),
      RowRL(Longs(42), Refs(intValue(666), intValue(2))),
      RowRL(Longs(42), Refs(intValue(666), NO_VALUE))
    )
    val right = mockPipeFor(
      slotInfoForInputs,
      RowRL(Longs(42), Refs(intValue(666), intValue(2))),
      RowRL(Longs(42), Refs(intValue(666), intValue(3))),
      RowRL(Longs(42), Refs(intValue(666), NO_VALUE))
    )

    val pipe = ValueHashJoinSlottedPipe(
      ReferenceFromSlot(1),
      ReferenceFromSlot(1),
      left,
      right,
      slotInfoForJoin,
      SlotMappings(
        Array(
          SlotMapping(0, 0, true, true),
          SlotMapping(0, 0, false, false),
          SlotMapping(1, 1, false, false),
          SlotMapping(1, 2, false, false)
        ),
        Array.empty
      )
    )()

    // when
    val result = pipe.createResults(queryState)

    // then
    testableResult(result, slotInfoForJoin) should equal(
      List(Map[String, Any]("arg1" -> 42L, "arg2" -> intValue(666), "a" -> intValue(2), "b" -> intValue(2)))
    )
  }

  test("exhaust should close table") {
    // given
    val monitor = QueryStateHelper.trackClosedMonitor
    val queryState = QueryStateHelper.emptyWithResourceManager(new ResourceManager(monitor))

    val slots = SlotConfigurationBuilder.empty
      .newReference("n", nullable = false, CTInteger)
      .build()

    val left = mockPipeFor(slots, RowR(intValue(1)))
    val right = mockPipeFor(slots, RowR(intValue(1)))

    // when
    ValueHashJoinSlottedPipe(
      ReferenceFromSlot(0),
      ReferenceFromSlot(0),
      left,
      right,
      slots,
      SlotMappings(Array(SlotMapping(0, 0, false, false)), Array.empty)
    )().createResults(queryState).toList

    // then
    monitor.closedResources.collect { case t: collection.ProbeTable[_, _] => t } should have size (1)
  }

  test("close should close table") {
    // given
    val monitor = QueryStateHelper.trackClosedMonitor
    val queryState = QueryStateHelper.emptyWithResourceManager(new ResourceManager(monitor))

    val slots = SlotConfigurationBuilder.empty
      .newReference("n", nullable = false, CTInteger)
      .build()

    val left = mockPipeFor(slots, RowR(intValue(1)))
    val right = mockPipeFor(slots, RowR(intValue(1)))

    // when
    val result = ValueHashJoinSlottedPipe(
      ReferenceFromSlot(0),
      ReferenceFromSlot(0),
      left,
      right,
      slots,
      SlotMappings(Array(SlotMapping(0, 0, false, false)), Array.empty)
    )().createResults(queryState)
    result.close()

    // then
    monitor.closedResources.collect { case t: collection.ProbeTable[_, _] => t } should have size (1)
  }
}
