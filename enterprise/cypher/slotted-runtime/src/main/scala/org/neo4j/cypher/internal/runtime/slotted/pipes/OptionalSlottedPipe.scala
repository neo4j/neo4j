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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, RefSlot, Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.values.storable.Values

case class OptionalSlottedPipe(source: Pipe,
                               nullableSlots: Seq[Slot],
                               slots: SlotConfiguration,
                               argumentSize: SlotConfiguration.Size)
                              (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) with Pipe {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val setNullableSlotToNullFunctions =
    nullableSlots.map {
      case LongSlot(offset, _, _) =>
        (context: ExecutionContext) => context.setLongAt(offset, -1L)
      case RefSlot(offset, _, _) =>
        (context: ExecutionContext) => context.setRefAt(offset, Values.NO_VALUE)
    }

  //===========================================================================
  // Runtime code
  //===========================================================================
  private def setNullableSlotsToNull(context: ExecutionContext) =
    setNullableSlotToNullFunctions.foreach { f =>
      f(context)
    }

  private def notFoundExecutionContext(state: QueryState): ExecutionContext = {
    val context = SlottedExecutionContext(slots)
    state.copyArgumentStateTo(context, argumentSize.nLongs, argumentSize.nReferences)
    setNullableSlotsToNull(context)
    context
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    if (input.isEmpty) {
      Iterator(notFoundExecutionContext(state))
    } else {
      input
    }
}
