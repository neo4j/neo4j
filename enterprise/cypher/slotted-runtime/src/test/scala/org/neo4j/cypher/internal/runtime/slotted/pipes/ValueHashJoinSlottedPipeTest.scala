/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.slotted.expressions.ReferenceFromSlot
import org.neo4j.cypher.internal.runtime.slotted.pipes.HashJoinSlottedPipeTestHelper.{Longs, Refs, RowR, RowRL, mockPipeFor, testableResult}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.{NO_VALUE, intValue, stringValue}

class ValueHashJoinSlottedPipeTest extends CypherFunSuite {

  test("should support simple hash join between two identifiers") {
    // given
    val queryState = QueryStateHelper.empty
    val slotInfoForInputs = SlotConfiguration.empty.newReference("b", nullable = false, CTInteger)
    val slotInfoForJoin = SlotConfiguration.empty
      .newReference("a", nullable = false, CTInteger)
      .newReference("b", nullable = false, CTInteger)

    val left = mockPipeFor(slotInfoForInputs,
      RowR(intValue(1)),
      RowR(intValue(2)),
      RowR(NO_VALUE)
    )
    val right = mockPipeFor(slotInfoForInputs,
      RowR(intValue(2)),
      RowR(intValue(3)),
      RowR(NO_VALUE)
    )

    val pipe = ValueHashJoinSlottedPipe(ReferenceFromSlot(0), ReferenceFromSlot(0), left, right, slotInfoForJoin, 0, 1, SlotConfiguration.Size.zero)()
    // when
    val result = pipe.createResults(queryState)

    // then
    testableResult(result, slotInfoForJoin) should equal(List(Map("a" -> intValue(2), "b" -> intValue(2))))
  }

  test("should handle multiples from both sides") {
    // given
    val queryState = QueryStateHelper.empty
    val slotInfoForInputs = SlotConfiguration.empty
      .newReference("a", nullable = false, CTInteger)
      .newReference("b", nullable = false, CTInteger)
    val slotInfoForJoin = SlotConfiguration.empty
      .newReference("a", nullable = false, CTInteger)
      .newReference("b", nullable = false, CTInteger)
      .newReference("c", nullable = false, CTInteger)
      .newReference("d", nullable = false, CTInteger)

    val left = mockPipeFor(slotInfoForInputs,
      RowR(intValue(1), stringValue("a")),
      RowR(intValue(1), stringValue("b")),
      RowR(intValue(2), stringValue("c")),
      RowR(intValue(3), stringValue("d"))
    )
    val right = mockPipeFor(slotInfoForInputs,
      RowR(intValue(1), stringValue("e")),
      RowR(intValue(2), stringValue("f")),
      RowR(intValue(2), stringValue("g")),
      RowR(intValue(4), stringValue("h"))
    )

    val pipe = ValueHashJoinSlottedPipe(ReferenceFromSlot(0), ReferenceFromSlot(0), left, right, slotInfoForJoin, 0, 2, SlotConfiguration.Size.zero)()
    // when
    val result = pipe.createResults(queryState)

    // then
    testableResult(result, slotInfoForJoin) should equal(List(
      Map("a" -> intValue(1), "b" -> stringValue("a"), "c" -> intValue(1), "d" -> stringValue("e")),
      Map("a" -> intValue(1), "b" -> stringValue("b"), "c" -> intValue(1), "d" -> stringValue("e")),
      Map("a" -> intValue(2), "b" -> stringValue("c"), "c" -> intValue(2), "d" -> stringValue("f")),
      Map("a" -> intValue(2), "b" -> stringValue("c"), "c" -> intValue(2), "d" -> stringValue("g"))
    ))
  }

  test("should not fetch results from RHS if LHS is empty") {
    // given
    val queryState = QueryStateHelper.empty

    val slotInfo = SlotConfiguration.empty
    slotInfo.newLong("a", nullable = false, CTNode)

    val left = mockPipeFor(slotInfo)


    val right = mock[Pipe]
    val pipe = ValueHashJoinSlottedPipe(ReferenceFromSlot(0), ReferenceFromSlot(0), left, right, slotInfo, 0, 1, SlotConfiguration.Size.zero)()

    // when
    val result = pipe.createResults(queryState)

    // then
    result should be(empty)
    verifyZeroInteractions(right)
  }

  test("should not fetch results from RHS if LHS did not contain any nodes that can be hashed against") {
    // given
    val queryState = QueryStateHelper.empty

    val slotInfo = SlotConfiguration.empty
    slotInfo.newReference("a", nullable = false, CTNode)

    val left = mockPipeFor(slotInfo, RowR(NO_VALUE))
    val right = mockPipeFor(slotInfo, RowR(intValue(42)))

    val pipe = ValueHashJoinSlottedPipe(ReferenceFromSlot(0), ReferenceFromSlot(0), left, right, slotInfo, 0, 1, SlotConfiguration.Size.zero)()

    // when
    val result = pipe.createResults(queryState)

    // then
    result should be(empty)
    verify(right, times(1)).createResults(any())
    verifyNoMoreInteractions(right)
  }

  test("should support hash join between two identifiers with shared arguments") {
    // given
    val queryState = QueryStateHelper.empty
    val slotInfoForInputs = SlotConfiguration.empty
      .newLong("arg1", nullable = false, CTNode)
      .newReference("arg2", nullable = false, CTInteger)
      .newReference("b", nullable = false, CTInteger)

    val slotInfoForJoin = SlotConfiguration.empty
      .newLong("arg1", nullable = false, CTNode)
      .newReference("arg2", nullable = false, CTInteger)
      .newReference("a", nullable = false, CTInteger)
      .newReference("b", nullable = false, CTInteger)

    val left = mockPipeFor(slotInfoForInputs,
      RowRL(Longs(42), Refs(intValue(666), intValue(1))),
      RowRL(Longs(42), Refs(intValue(666), intValue(2))),
      RowRL(Longs(42), Refs(intValue(666), NO_VALUE))
    )
    val right = mockPipeFor(slotInfoForInputs,
      RowRL(Longs(42), Refs(intValue(666), intValue(2))),
      RowRL(Longs(42), Refs(intValue(666), intValue(3))),
      RowRL(Longs(42), Refs(intValue(666), NO_VALUE))
    )

    val pipe = ValueHashJoinSlottedPipe(ReferenceFromSlot(1), ReferenceFromSlot(1), left, right, slotInfoForJoin,
      longOffset = 1, refsOffset = 2, SlotConfiguration.Size(1, 1))()

    // when
    val result = pipe.createResults(queryState)

    // then
    testableResult(result, slotInfoForJoin) should equal(
      List(Map("arg1" -> 42L, "arg2" -> intValue(666), "a" -> intValue(2), "b" -> intValue(2))))
  }

}
