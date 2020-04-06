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

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.collection.JavaConverters._

case class PartialSortPipe(source: Pipe,
                           prefixComparator: Comparator[ExecutionContext],
                           suffixComparator: Comparator[ExecutionContext])
                          (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) with OrderedInputPipe {

  class PartialSortReceiver(state: QueryState) extends OrderedChunkReceiver {
    private val buffer = new java.util.ArrayList[ExecutionContext]()

    override def clear(): Unit = {
      buffer.forEach(x => state.memoryTracker.deallocated(x))
      buffer.clear()
    }

    override def isSameChunk(first: ExecutionContext, current: ExecutionContext): Boolean = prefixComparator.compare(first, current) == 0

    override def processRow(row: ExecutionContext): Unit = {
      state.memoryTracker.allocated(row)
      buffer.add(row)
    }

    override def result(): Iterator[ExecutionContext] = {
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
