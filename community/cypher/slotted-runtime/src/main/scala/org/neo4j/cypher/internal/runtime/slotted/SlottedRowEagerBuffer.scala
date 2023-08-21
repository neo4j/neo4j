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
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.kernel.impl.util.collection.EagerBuffer
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker

import java.util.function.IntUnaryOperator

object SlottedRowEagerBuffer {

  /**
   * Returns an eager buffer that deduplicate some estimated heap usage in slotted runtime.
   */
  def apply(
    memoryTracker: MemoryTracker,
    initialChunkSize: Int,
    maxChunkSize: Int,
    growthStrategy: IntUnaryOperator,
    slots: SlotConfiguration
  ): EagerBuffer[CypherRow] = {
    val memEstimator = if (slots.numberOfReferences > 0 && memoryTracker != EmptyMemoryTracker.INSTANCE) {
      DeduplicatedCypherRowMemoryEstimator
    } else {
      EagerBuffer.ChunkMemoryEstimator.createDefault[CypherRow]()
    }
    EagerBuffer.createEagerBuffer(memoryTracker, initialChunkSize, maxChunkSize, growthStrategy, memEstimator)
  }

  /** Hack to avoid some of the worst heap over estimation of slotted runtime */
  object DeduplicatedCypherRowMemoryEstimator extends EagerBuffer.ChunkMemoryEstimator[CypherRow] {

    override def estimateHeapUsage(element: CypherRow, previous: CypherRow): Long = {
      element.deduplicatedEstimatedHeapUsage(previous)
    }
  }
}
