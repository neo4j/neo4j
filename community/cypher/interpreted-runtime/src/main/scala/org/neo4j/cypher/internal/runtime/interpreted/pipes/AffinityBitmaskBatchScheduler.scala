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

import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue

/**
 * A variant of [[BitmaskBatchScheduler]] that uses dependency-set affinity when selecting the batch
 * for a new row, instead of greedy first-fit.
 *
 * The greedy policy places a row into the first open batch whose sequence permits it. A conflict-free
 * row can therefore join a batch that already carries dependencies, inheriting that batch's
 * serialization for its own execution and re-exporting it transitively through last-writer tracking
 * to later rows that share its RAID values (dependency contagion).
 *
 * The affinity policy instead matches rows to batches with compatible dependency sets, see
 * [[AbstractBitmaskBatchScheduler.selectBatchByAffinity]]. Backpressure semantics are identical to the
 * greedy scheduler: -1 is returned only when allocation fails and no open batch passes the greedy
 * criterion.
 *
 * @param batchSize The maximum number of rows per batch
 * @param raidsPerRow The number of RAID expressions per row
 * @param maxBatches The maximum number of batches that can be in formation at once (must be multiple of 64)
 *                   Target: the size of the input queue times 2, rounded up to the next multiple of 64, for saturation.
 */
class AffinityBitmaskBatchScheduler(
  batchSize: Int,
  raidsPerRow: Int,
  maxBatches: Int = 256,
  memoryTracker: MemoryTracker
) extends AbstractBitmaskBatchScheduler(batchSize, raidsPerRow, maxBatches, memoryTracker) {

  /**
   * Insert a row: find dependencies, select a batch by affinity, and update state.
   * @return The index of the batch selected for insertion, or -1 if it was rejected due to too many batches in formation (backpressure)
   */
  def insertRow(rowRaids: Array[AnyValue]): Int = {
    resolveDependencies(rowRaids)
    val selectedBatch = selectBatchByAffinity()
    if (selectedBatch == -1) {
      clearReqDeps()
      return -1
    }
    val wasReady = isReady(selectedBatch)
    val addedDep = linkDependencies(selectedBatch, wasReady)
    recordLastWriter(rowRaids, selectedBatch)
    bumpFillCount(selectedBatch, addedDep, wasReady)
    selectedBatch
  }
}
