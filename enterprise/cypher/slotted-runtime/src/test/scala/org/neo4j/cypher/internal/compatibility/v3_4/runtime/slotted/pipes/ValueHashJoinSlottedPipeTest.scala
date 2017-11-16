/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.expressions.ReferenceFromSlot
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, PipelineInformation, RefSlot}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, QueryStateHelper}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.{NO_VALUE, intValue, stringValue}

class ValueHashJoinSlottedPipeTest extends CypherFunSuite {

  test("should support simple hash join between two identifiers") {
    // given
    val queryState = QueryStateHelper.empty
    val slotInfoForInputs = PipelineInformation.empty.newReference("b", nullable = false, CTInteger)
    val slotInfoForJoin = PipelineInformation.empty
      .newReference("a", nullable = false, CTInteger)
      .newReference("b", nullable = false, CTInteger)

    val left = mockPipeFor(slotInfoForInputs,
      Row(r = Refs(intValue(1))),
      Row(r = Refs(intValue(2))),
      Row(r = Refs(NO_VALUE))
    )
    val right = mockPipeFor(slotInfoForInputs,
      Row(r = Refs(intValue(2))),
      Row(r = Refs(intValue(3))),
      Row(r = Refs(NO_VALUE))
    )

    val pipe = ValueHashJoinSlottedPipe(ReferenceFromSlot(0), ReferenceFromSlot(0), left, right, slotInfoForJoin, 0, 1)()
    // when
    val result = pipe.createResults(queryState)

    // then
    testableResult(result, slotInfoForJoin) should equal(List(Map("a" -> intValue(2), "b" -> intValue(2))))
  }

  test("should handle multiples from both sides") {
    // given
    val queryState = QueryStateHelper.empty
    val slotInfoForInputs = PipelineInformation.empty
      .newReference("a", nullable = false, CTInteger)
      .newReference("b", nullable = false, CTInteger)
    val slotInfoForJoin = PipelineInformation.empty
      .newReference("a", nullable = false, CTInteger)
      .newReference("b", nullable = false, CTInteger)
      .newReference("c", nullable = false, CTInteger)
      .newReference("d", nullable = false, CTInteger)

    val left = mockPipeFor(slotInfoForInputs,
      Row(r = Refs(intValue(1), stringValue("a"))),
      Row(r = Refs(intValue(1), stringValue("b"))),
      Row(r = Refs(intValue(2), stringValue("c"))),
      Row(r = Refs(intValue(3), stringValue("d")))
    )
    val right = mockPipeFor(slotInfoForInputs,
      Row(r = Refs(intValue(1), stringValue("e"))),
      Row(r = Refs(intValue(2), stringValue("f"))),
      Row(r = Refs(intValue(2), stringValue("g"))),
      Row(r = Refs(intValue(4), stringValue("h")))
    )

    val pipe = ValueHashJoinSlottedPipe(ReferenceFromSlot(0), ReferenceFromSlot(0), left, right, slotInfoForJoin, 0, 2)()
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

    val slotInfo = PipelineInformation.empty
    slotInfo.newLong("a", nullable = false, CTNode)

    val left = mockPipeFor(slotInfo)


    val right = mock[Pipe]
    val pipe = ValueHashJoinSlottedPipe(ReferenceFromSlot(0), ReferenceFromSlot(0), left, right, slotInfo, 0, 1)()

    // when
    val result = pipe.createResults(queryState)

    // then
    result should be(empty)
    verifyZeroInteractions(right)
  }

  test("should not fetch results from RHS if LHS did not contain any nodes that can be hashed against") {
    // given
    val queryState = QueryStateHelper.empty

    val slotInfo = PipelineInformation.empty
    slotInfo.newReference("a", nullable = false, CTNode)

    val left = mockPipeFor(slotInfo, Row(r = Refs(NO_VALUE)))
    val right = mockPipeFor(slotInfo, Row(r = Refs(intValue(42))))

    val pipe = ValueHashJoinSlottedPipe(ReferenceFromSlot(0), ReferenceFromSlot(0), left, right, slotInfo, 0, 1)()

    // when
    val result = pipe.createResults(queryState)

    // then
    result should be(empty)
    verify(right, times(1)).createResults(any())
    verifyNoMoreInteractions(right)
  }


  case class Row(l: Longs = Longs(), r: Refs = Refs())

  case class Longs(l: Long*)

  case class Refs(l: AnyValue*)

  private def mockPipeFor(pipelineInformation: PipelineInformation, rows: Row*) = {
    val p = mock[Pipe]
    when(p.createResults(any())).thenAnswer(new Answer[Iterator[ExecutionContext]]() {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[ExecutionContext] = {
        rows.toIterator.map { row =>
          val createdRow = PrimitiveExecutionContext(pipelineInformation)
          row.l.l.zipWithIndex foreach {
            case (v, idx) => createdRow.setLongAt(idx, v)
          }
          row.r.l.zipWithIndex foreach {
            case (v, idx) => createdRow.setRefAt(idx, v)
          }
          createdRow
        }
      }
    })
    p
  }

  private def testableResult(list: Iterator[ExecutionContext], pipelineInformation: PipelineInformation): List[Map[String, Any]] = {
    val list1 = list.toList
    list1 map { in =>
      val build = scala.collection.mutable.HashMap.empty[String, Any]
      pipelineInformation.foreachSlot {
        case (column, LongSlot(offset, _, _)) => build.put(column, in.getLongAt(offset))
        case (column, RefSlot(offset, _, _)) => build.put(column, in.getRefAt(offset))
      }
      build.toMap
    }
  }
}

