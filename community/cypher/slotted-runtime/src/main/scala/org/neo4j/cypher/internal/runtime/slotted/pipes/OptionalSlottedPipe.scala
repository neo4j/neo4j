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

import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.Values

case class OptionalSlottedPipe(source: Pipe, nullableSlots: Seq[Slot])(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) with Pipe {

  // ===========================================================================
  // Compile-time initializations
  // ===========================================================================
  private val setNullableSlotToNullFunctions =
    nullableSlots.map {
      case LongSlot(offset, _, _) =>
        (context: CypherRow) => context.setLongAt(offset, -1L)
      case RefSlot(offset, _, _) =>
        (context: CypherRow) => context.setRefAt(offset, Values.NO_VALUE)
    }

  // ===========================================================================
  // Runtime code
  // ===========================================================================
  private def setNullableSlotsToNull(context: CypherRow): Unit = {
    val functions = setNullableSlotToNullFunctions.iterator
    while (functions.hasNext) {
      functions.next()(context)
    }
  }

  private def notFoundExecutionContext(state: QueryState): CypherRow = {
    val context = state.newRowWithArgument(rowFactory)
    setNullableSlotsToNull(context)
    context
  }

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] =
    if (!input.hasNext) {
      ClosingIterator.single(notFoundExecutionContext(state))
    } else {
      input
    }
}
