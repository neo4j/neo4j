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

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.JavaIteratorAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.SlottedRowEagerBuffer
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.EagerBuffer

case class EagerSlottedPipe(source: Pipe, slots: SlotConfiguration)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val buffer = SlottedRowEagerBuffer(
      state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x),
      1024,
      8192,
      EagerBuffer.GROW_NEW_CHUNKS_BY_100_PCT,
      slots
    )
    state.query.resources.trace(buffer)
    while (input.hasNext) {
      val row = input.next()
      row.compact()
      buffer.add(row)
    }
    buffer.autoClosingIterator().asClosingIterator.map { bufferedRow =>
      // this is necessary because Eager is the beginning of a new pipeline
      // We do this on the output side, and buffer the input rows they will use less memory
      val outputRow = SlottedRow(slots)
      outputRow.copyAllFrom(bufferedRow)
      outputRow
    }.closing(buffer)
  }
}
