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

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.DelegatingClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

import java.util.Comparator

import scala.jdk.CollectionConverters.IteratorHasAsScala

case class SortSlottedPipe(
  source: Pipe,
  comparator: Comparator[ReadableRow]
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val scopedMemoryTracker =
      state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x).getScopedMemoryTracker
    var arrayList = HeapTrackingArrayList.newArrayList[CypherRow](256, scopedMemoryTracker)
    var previous: CypherRow = null
    while (input.hasNext) {
      val row = input.next()
      // Note, not safe to call row.compact() here, like we do in pipelined, because sort is not breaking in slotted.
      scopedMemoryTracker.allocateHeap(row.deduplicatedEstimatedHeapUsage(previous))
      arrayList.add(row)
      previous = row
    }
    previous = null
    arrayList.sort(comparator)
    new DelegatingClosingIterator[CypherRow](arrayList.iterator().asScala) {
      override def closeMore(): Unit = {
        arrayList = null
        scopedMemoryTracker.close()
        input.close()
      }
    }
  }
}
