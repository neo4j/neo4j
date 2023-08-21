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

import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeSetValueInSlotFunctionFor
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.values.AnyValue

case class DistinctSlottedSinglePrimitivePipe(
  source: Pipe,
  slots: SlotConfiguration,
  toSlot: Slot,
  offset: Int,
  expression: Expression
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {

  // ===========================================================================
  // Compile-time initializations
  // ===========================================================================
  private val setInSlot: (CypherRow, AnyValue) => Unit = makeSetValueInSlotFunctionFor(toSlot)

  // ===========================================================================
  // Runtime code
  // ===========================================================================
  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {

    new PrefetchingIterator[CypherRow] {
      private var seen =
        HeapTrackingCollections.newLongSet(state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x))
      private var seenWrapper = DefaultCloseListenable.wrap(seen)

      state.query.resources.trace(seenWrapper)

      override def produceNext(): Option[CypherRow] = {
        while (input.hasNext) { // Let's pull data until we find something not already seen
          val next = input.next()
          val key = next.getLongAt(offset)
          if (seen.add(key)) {
            // Found something! Set it as the next element to yield, and exit
            val outputValue = expression(next, state)
            setInSlot(next, outputValue)
            return Some(next)
          }
        }
        seenWrapper.close()
        seen = null
        seenWrapper = null
        None
      }

      override protected[this] def closeMore(): Unit = if (seenWrapper != null) seenWrapper.close()
    }
  }
}
