/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import java.util.Comparator

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.util.attribution.Id

import scala.collection.JavaConverters.asScalaIteratorConverter

case class PartialSortPipe(source: Pipe,
                           prefixComparator: Comparator[ReadableRow],
                           suffixComparator: Comparator[ReadableRow])
                          (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) with OrderedInputPipe {

  class PartialSortReceiver(state: QueryState) extends OrderedChunkReceiver {
    private val memoryTracker = state.memoryTracker.memoryTrackerForOperator(id.x)
    private val rowsMemoryTracker = memoryTracker.getScopedMemoryTracker
    private val buffer = HeapTrackingArrayList.newArrayList[CypherRow](16, memoryTracker)

    override def clear(): Unit = {
      rowsMemoryTracker.reset()
      buffer.clear()
    }

    override def close(): Unit = {
      buffer.close()
      rowsMemoryTracker.close()
    }

    override def isSameChunk(first: CypherRow, current: CypherRow): Boolean = prefixComparator.compare(first, current) == 0

    override def processRow(row: CypherRow): Unit = {
      rowsMemoryTracker.allocateHeap(row.estimatedHeapUsage)
      buffer.add(row)
    }

    override def result(): Iterator[CypherRow] = {
      if (buffer.size() > 1) {
        // Sort this chunk
        buffer.sort(suffixComparator)
      }
      buffer.iterator().asScala
    }

    override def processNextChunk: Boolean = true
  }

  override def getReceiver(state: QueryState): OrderedChunkReceiver = new PartialSortReceiver(state)
}
