/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.ValueConversion.asValue
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.scalatest.mock.MockitoSugar

case class FakeSlottedPipe(data: Iterator[Map[String, Any]], slots: SlotConfiguration)
  extends Pipe with MockitoSugar {

  def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    data.map { values =>
      val result = SlottedExecutionContext(slots)

      values foreach {
        case (key, value) =>
          slots(key) match {
            case LongSlot(offset, _, _) if value == null =>
              result.setLongAt(offset, -1)

            case LongSlot(offset, _, _) =>
              result.setLongAt(offset, value.asInstanceOf[Number].longValue())

            case RefSlot(offset, _, _) =>
              result.setRefAt(offset, asValue(value))
          }
      }
      result
    }
  }

  var id: Id = Id.INVALID_ID
}
