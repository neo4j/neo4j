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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue

object HashJoinSlottedPipeTestHelper extends CypherFunSuite {

  abstract class Row {
    val l: Longs
    val r: Refs
  }

  case class RowR(refs: AnyValue*) extends Row {
    val r = Refs(refs: _*)
    val l = Longs()
  }

  case class RowL(longs: Long*) extends Row {
    val r = Refs()
    val l = Longs(longs: _*)
  }

  case class RowRL(l: Longs, r: Refs) extends Row

  case class Longs(l: Long*)

  case class Refs(l: AnyValue*)

  def mockPipeFor(slots: SlotConfiguration, rows: Row*): Pipe = {
    val p = mock[Pipe]
    when(p.createResults(any())).thenAnswer(new Answer[Iterator[ExecutionContext]]() {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[ExecutionContext] = {
        rows.toIterator.map { row =>
          val createdRow = SlottedExecutionContext(slots)
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

  def testableResult(list: Iterator[ExecutionContext], slots: SlotConfiguration): List[Map[String, Any]] = {
    val list1 = list.toList
    list1 map { in =>
      val build = scala.collection.mutable.HashMap.empty[String, Any]
      slots.foreachSlot {
        case (column, LongSlot(offset, _, _)) => build.put(column, in.getLongAt(offset))
        case (column, RefSlot(offset, _, _)) => build.put(column, in.getRefAt(offset))
      }
      build.toMap
    }
  }
}
