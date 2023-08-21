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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PartialSortPipe.NO_MORE_ROWS_TO_SKIP_SORTING
import org.neo4j.cypher.internal.util.attribution.Id

import java.util.Comparator

case class PartialSortPipe(
  source: Pipe,
  prefixComparator: Comparator[ReadableRow],
  suffixComparator: Comparator[ReadableRow],
  skipSortingPrefixLengthExp: Option[Expression]
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) with OrderedInputPipe {

  class PartialSortReceiver(state: QueryState) extends OrderedChunkReceiver {
    private val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    private val rowsMemoryTracker = memoryTracker.getScopedMemoryTracker
    private val buffer = HeapTrackingArrayList.newArrayList[CypherRow](16, memoryTracker)

    private val skipSortingPrefixLength =
      skipSortingPrefixLengthExp.map(SkipPipe.evaluateStaticSkipOrLimitNumberOrThrow(_, state, "SKIP"))

    // How many rows remain until we need to start sorting?
    private var remainingSkipSorting: Long = skipSortingPrefixLength.getOrElse(NO_MORE_ROWS_TO_SKIP_SORTING)

    override def clear(): Unit = {
      rowsMemoryTracker.reset()
      buffer.clear()
    }

    override def close(): Unit = {
      buffer.close()
      rowsMemoryTracker.close()
    }

    override def isSameChunk(first: CypherRow, current: CypherRow): Boolean =
      prefixComparator.compare(first, current) == 0

    override def processRow(row: CypherRow): Unit = {
      rowsMemoryTracker.allocateHeap(row.estimatedHeapUsage)
      buffer.add(row)
      remainingSkipSorting = math.max(NO_MORE_ROWS_TO_SKIP_SORTING, remainingSkipSorting - 1)
    }

    override def result(): ClosingIterator[CypherRow] = {
      if (buffer.size() > 1 && remainingSkipSorting == NO_MORE_ROWS_TO_SKIP_SORTING) {
        // Sort this chunk
        buffer.sort(suffixComparator)
      }
      ClosingIterator(buffer.iterator())
    }

    override def processNextChunk: Boolean = true
  }

  override def getReceiver(state: QueryState): OrderedChunkReceiver = new PartialSortReceiver(state)
}

object PartialSortPipe {
  val NO_MORE_ROWS_TO_SKIP_SORTING: Long = -1L
}
