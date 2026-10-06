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

import org.neo4j.collection.trackable.HeapTrackingObjectLongHashMap
import org.neo4j.cypher.internal.macros.AssertMacros3.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker
import org.neo4j.util.VisibleForTesting
import org.neo4j.values.AnyValue

import java.lang.Long.numberOfTrailingZeros

/**
 * Shared state and dependency-DAG maintenance for the bitmask batch schedulers.
 *
 * Batches in formation occupy slots of a ring buffer of maxBatches entries, indexed 0 until
 * maxBatches. Dependencies between batches are bitmasks over those slots, ROW_WORDS longs per batch:
 * forwardDeps(b) holds the parents b waits for and reverseDeps(b) the children waiting for b. Four
 * masks of ROW_WORDS longs classify slots as active, open for insertion, ready (no parents left) and
 * filled-and-ready. A RAID value maps to its last writer as (batchSequence << 16) | (slot + 1); the
 * writer is live only while the slot is active and its stored sequence matches, so completing a
 * batch releases all of its RAIDs implicitly.
 *
 * Guarantees: a batch becomes ready only when every parent has completed, so batches sharing a RAID
 * value never execute concurrently; batchSequence is monotonic and a batch only ever depends on
 * lower sequences, so the DAG is acyclic. The scheduler's fixed heap is charged to the memory
 * tracker on construction and released on close.
 *
 * Not thread-safe; driven from a single thread.
 *
 * @param batchSize The maximum number of rows per batch
 * @param raidsPerRow The number of RAID expressions per row
 * @param maxBatches The maximum number of batches that can be in formation at once (must be multiple of 64)
 *                   Target: the size of the input queue times 2, rounded up to the next multiple of 64, for saturation.
 */
abstract class AbstractBitmaskBatchScheduler(
  batchSize: Int,
  raidsPerRow: Int,
  maxBatches: Int,
  memoryTracker: MemoryTracker
) extends BatchScheduler {

  require(maxBatches % 64 == 0, "maxBatches must be a multiple of 64")
  require(maxBatches <= 65535, s"maxBatches must be <= 65535 (16-bit encoding limit), got $maxBatches")

  // --- CONFIGURATION ---
  // E.g. Using maxBatches=256 allows us to use exactly 4 Longs (64 bits * 4) per row.
  final val ROW_WORDS = maxBatches >> 6 // equivalent to / 64

  // --- STATE: DEPENDENCY MATRICES ---
  // forwardDeps(batchIdx)(wordIdx) -> bits representing parents
  protected[this] val forwardDeps = Array.ofDim[Long](maxBatches, ROW_WORDS)

  // reverseDeps(batchIdx)(wordIdx) -> bits representing children
  // Used to quickly notify children when a batch completes
  protected[this] val reverseDeps = Array.ofDim[Long](maxBatches, ROW_WORDS)

  // --- STATE: MASKS ---
  // 1 = Batch is Allocated but not yet Completed
  protected[this] val activeBatchesMask = new Array[Long](ROW_WORDS)
  // 1 = Batch is Allocated and Open for insertion
  protected[this] val openBatchesMask = new Array[Long](ROW_WORDS)
  // 1 = Batch is Allocated and Ready (no dependencies)
  protected[this] val readyBatchesMask = new Array[Long](ROW_WORDS)
  // 1 = Batch is Allocated, Full, and Ready (no dependencies)
  protected[this] val filledReadyBatchesMask = new Array[Long](ROW_WORDS)

  // --- STATE: BATCH METADATA ---
  protected[this] val batchSequence = new Array[Long](maxBatches) // Logical timestamp to prevent cycles
  protected[this] val batchFillCount = new Array[Int](maxBatches)

  // --- STATE: RAID TRACKING (Last Writer Wins) ---
  // Maps RAID -> encoded (batchSequence, batchIndex) that last touched it.
  // Encoding: (sequence << 16) | (index + 1), where index + 1 avoids 0 (the default empty value)
  // The encoding truncates the sequence to 48 bits, which only wraps after 2^48 batch allocations
  // in one query (unreachable in practice).
  // This map doubles as the RESERVATION set: a RAID is reserved exactly when its decoded owner
  // passes the staleness check (owner active and stored sequence matches), so releasing
  // reservations at batch completion is implicit and free.
  protected[this] val raidLastWriter =
    HeapTrackingObjectLongHashMap.createObjectLongHashMap[AnyValue](memoryTracker, Math.max(batchSize, maxBatches))

  // --- INTERNAL COUNTERS ---
  private[this] var globalSequenceCounter: Long = 0L
  private[this] var nextAllocationIndex: Int = 0

  // --- PURGE THRESHOLD ---
  private[this] val raidPurgeThreshold: Long = 2L * maxBatches * batchSize * raidsPerRow
  // An array to temporarily hold RAID keys to be removed during a purge. Reused between purges.
  private[this] var raidKeysToRemove: java.util.ArrayList[AnyValue] = _

  // --- SCRATCH: the row currently being placed, filled by resolveDependencies ---
  // Parents required by the row, as a slot mask. Always all-zero between insertRow calls.
  protected[this] val reqDeps = new Array[Long](ROW_WORDS)
  // Highest live writer sequence among the row's RAIDs and its slot, or -1 when the row has no
  // live dependencies
  protected[this] var maxDependencySeq: Long = -1L
  protected[this] var maxDepWriterIdx: Int = -1
  // Raw encoded last-writer value per RAID index (0 = none), so that a placement can update the
  // last-writer map without a second lookup per RAID. lastWriterScratchFor records which RAID array
  // the scratch was filled for (assertion only).
  protected[this] val lastWriterScratch = new Array[Long](raidsPerRow)
  protected[this] var lastWriterScratchFor: Array[AnyValue] = _

  private[this] val fixedHeapUsage: Long = {
    val matrix = HeapEstimator.shallowSizeOfObjectArray(maxBatches) +
      maxBatches.toLong * HeapEstimator.sizeOfLongArray(ROW_WORDS)
    2L * matrix +
      5L * HeapEstimator.sizeOfLongArray(ROW_WORDS) +
      HeapEstimator.sizeOfLongArray(maxBatches) +
      HeapEstimator.sizeOfIntArray(maxBatches) +
      HeapEstimator.sizeOfLongArray(raidsPerRow)
  }
  memoryTracker.allocateHeap(fixedHeapUsage)

  /**
   * Select a batch for execution.
   * Returns batch index or -1 if none.
   */
  def pollReadyBatch(force: Boolean): Int = {
    val filled = pollFilledReadyBatch()
    if (filled >= 0) {
      return filled
    }
    if (force) {
      // Select a ready batch even if it's not full
      var w = 0
      while (w < ROW_WORDS) {
        val word = readyBatchesMask(w)
        if (word != 0) {
          val bit = numberOfTrailingZeros(word)
          val idx = (w << 6) + bit
          // TODO: Consider more parameters like fill count and dependencies (https://linear.app/neo4j/issue/RUN-1004)
          readyBatchesMask(w) &= ~(1L << idx)
          openBatchesMask(w) &= ~(1L << idx)
          return idx
        }
        w += 1
      }
    }
    -1
  }

  /**
   * The non-force scan shared by every scheduler's pollReadyBatch: pop the first full, ready batch,
   * clearing it from both the filled-ready and ready masks. Returns its index, or -1 if none.
   */
  protected def pollFilledReadyBatch(): Int = {
    var w = 0
    while (w < ROW_WORDS) {
      val word = filledReadyBatchesMask(w)
      if (word != 0) {
        val bit = numberOfTrailingZeros(word)
        val idx = (w << 6) + bit
        filledReadyBatchesMask(w) &= ~(1L << idx)
        readyBatchesMask(w) &= ~(1L << idx)
        return idx
      }
      w += 1
    }
    -1
  }

  /**
   * Triggered when a ready batch is selected for execution.
   */
  def dispatchBatch(dispatchedBatchIdx: Int): Unit = {
    checkOnlyWhenAssertionsAreEnabled(
      {
        val wordIdx = dispatchedBatchIdx >> 6
        (activeBatchesMask(wordIdx) & (1L << dispatchedBatchIdx)) != 0 &&
        (openBatchesMask(wordIdx) & (1L << dispatchedBatchIdx)) == 0 &&
        (readyBatchesMask(wordIdx) & (1L << dispatchedBatchIdx)) == 0 &&
        (filledReadyBatchesMask(wordIdx) & (1L << dispatchedBatchIdx)) == 0
      },
      "Illegal batch state for batch " + dispatchedBatchIdx
    )
  }

  /**
   * Triggered when a batch finishes execution.
   */
  def completeBatch(completedBatchIdx: Int): Unit = {
    checkOnlyWhenAssertionsAreEnabled(
      {
        val wordIndex = completedBatchIdx >> 6
        (activeBatchesMask(wordIndex) & (1L << completedBatchIdx)) != 0 &&
        (openBatchesMask(wordIndex) & (1L << completedBatchIdx)) == 0 &&
        (readyBatchesMask(wordIndex) & (1L << completedBatchIdx)) == 0 &&
        (filledReadyBatchesMask(wordIndex) & (1L << completedBatchIdx)) == 0
      },
      "Illegal state on complete for batch " + completedBatchIdx
    )

    // Clear from masks
    val completedWordIdx = completedBatchIdx >> 6
    val completedBitMask = ~(1L << completedBatchIdx)
    activeBatchesMask(completedWordIdx) &= completedBitMask

    // Periodic purge of stale RAID entries to bound memory growth
    if (raidLastWriter.size() > raidPurgeThreshold) {
      purgeStaleRaidEntries()
    }

    releaseBatchReservations(completedBatchIdx)

    // Notify children
    var w = 0
    while (w < ROW_WORDS) {
      var childrenWord = reverseDeps(completedBatchIdx)(w)

      // Reset reverse deps for this batch immediately (cleanup)
      reverseDeps(completedBatchIdx)(w) = 0

      while (childrenWord != 0) {
        val childBit = numberOfTrailingZeros(childrenWord)
        val childIdx = (w << 6) + childBit

        // 1. Remove dependency from child
        // The child's dependency on 'completedBatchIdx' is located in child's forwardDeps
        // at word 'completedWordIdx', bit 'completedBatchIdx'
        forwardDeps(childIdx)(completedWordIdx) &= completedBitMask

        // 2. Check if child is NOW ready
        if (checkIfBatchReady(childIdx)) {
          // Set bit in ready mask
          val wordIdx = childIdx >> 6
          readyBatchesMask(wordIdx) |= (1L << childIdx)
          if ((openBatchesMask(wordIdx) & (1L << childIdx)) == 0) {
            filledReadyBatchesMask(wordIdx) |= (1L << childIdx)
          }
        }

        childrenWord &= (childrenWord - 1)
      }
      w += 1
    }
  }

  override def close(): Unit = {
    raidLastWriter.close()
    memoryTracker.releaseHeap(fixedHeapUsage)
  }

  private def purgeStaleRaidEntries(): Unit = {
    // Reuse the array holding temporary keys to remove between purges
    if (raidKeysToRemove == null) {
      raidKeysToRemove = new java.util.ArrayList[AnyValue]()
    }
    val keysToRemove = raidKeysToRemove
    var i = 0
    val cachedSize = keysToRemove.size()
    raidLastWriter.forEachKeyValue((raid: AnyValue, encoded: Long) => {
      val writerIdx = (encoded & 0xffffL).toInt - 1
      val writerStoredSeq = encoded >>> 16
      if (
        writerIdx == -1 ||
        (activeBatchesMask(writerIdx >> 6) & (1L << writerIdx)) == 0 ||
        batchSequence(writerIdx) != writerStoredSeq
      ) {
        if (i < cachedSize) {
          keysToRemove.set(i, raid)
          i += 1
        } else {
          keysToRemove.add(raid)
        }
      }
    })
    var j = 0
    var key: AnyValue = null
    val size = keysToRemove.size()
    while (
      j < size && {
        key = keysToRemove.get(j)
        key != null
      }
    ) {
      raidLastWriter.removeKey(key)
      keysToRemove.set(j, null) // Clear reference to avoid holding on to memory
      j += 1
    }
  }

  // --- PLACEMENT MECHANICS (shared by every insertion path) ---

  /**
   * Resolve the live batches that own the given RAIDs: sets the parent bits in reqDeps, records the
   * newest writer in maxDependencySeq/maxDepWriterIdx, and stashes each RAID's encoded last-writer
   * value in lastWriterScratch. A writer is live when its slot is active and its stored sequence
   * matches (the staleness check).
   */
  final protected def resolveDependencies(raids: Array[AnyValue]): Unit = {
    maxDependencySeq = -1L
    maxDepWriterIdx = -1
    lastWriterScratchFor = raids
    var i = 0
    while (i < raids.length) {
      // Empty value is 0 in ObjectLongHashMap, so the slot is stored offset by +1
      val encoded = raidLastWriter.get(raids(i))
      lastWriterScratch(i) = encoded
      val writerIdx = (encoded & 0xffffL).toInt - 1
      val writerStoredSeq = encoded >>> 16

      if (
        writerIdx != -1 &&
        (activeBatchesMask(writerIdx >> 6) & (1L << writerIdx)) != 0 &&
        batchSequence(writerIdx) == writerStoredSeq
      ) {
        if (writerStoredSeq > maxDependencySeq) {
          maxDependencySeq = writerStoredSeq
          maxDepWriterIdx = writerIdx
        }
        reqDeps(writerIdx >> 6) |= (1L << writerIdx) // implicit modulo 64
      }
      i += 1
    }
  }

  /**
   * Select a batch for the row whose dependencies were just resolved, preferring placements that
   * add no dependency edges.
   *
   * A row without live dependencies takes the first clean open batch with room (ready and open, so
   * joining adds no edges in either direction), then a fresh allocation, and under slot pressure any
   * open batch with room.
   *
   * A row with live dependencies takes its newest writer if that batch is open with room (the
   * dominant dependency becomes a removed self-dependency), then an open batch whose forward
   * dependencies already cover reqDeps (joining adds no new edges), then a fresh allocation, and
   * under slot pressure any open batch with room whose sequence is at least maxDependencySeq.
   *
   * @return the selected slot, or -1 when every slot is active
   */
  final protected def selectBatchByAffinity(): Int = {
    var selectedBatch = -1
    if (maxDepWriterIdx == -1) {
      selectedBatch = firstCleanOpenBatchWithRoom()
      if (selectedBatch == -1) {
        selectedBatch = allocateNewBatch()
      }
      if (selectedBatch == -1) {
        selectedBatch = firstOpenWithRoomAndSeq(-1L)
      }
    } else {
      if (
        (openBatchesMask(maxDepWriterIdx >> 6) & (1L << maxDepWriterIdx)) != 0 &&
        batchFillCount(maxDepWriterIdx) < batchSize
      ) {
        selectedBatch = maxDepWriterIdx
      }
      if (selectedBatch == -1) {
        var w = 0
        while (w < ROW_WORDS && selectedBatch == -1) {
          var word = openBatchesMask(w)
          while (word != 0 && selectedBatch == -1) {
            val bit = numberOfTrailingZeros(word)
            val idx = (w << 6) + bit
            if (
              batchSequence(idx) >= maxDependencySeq &&
              batchFillCount(idx) < batchSize &&
              reqDepsCoveredBy(idx)
            ) {
              selectedBatch = idx
            } else {
              word &= ~(1L << bit)
            }
          }
          w += 1
        }
      }
      if (selectedBatch == -1) {
        selectedBatch = allocateNewBatch()
      }
      if (selectedBatch == -1) {
        selectedBatch = firstOpenWithRoomAndSeq(maxDependencySeq)
      }
    }
    selectedBatch
  }

  /**
   * Add the resolved dependencies (reqDeps, minus the batch itself) as edges into selectedBatch:
   * forwardDeps of the batch, reverseDeps of each new parent, and the ready mask when the batch
   * stops being ready. Clears reqDeps.
   * @return whether at least one new edge was added
   */
  final protected def linkDependencies(selectedBatch: Int, wasReady: Boolean): Boolean = {
    // A batch should not depend on itself
    reqDeps(selectedBatch >> 6) &= ~(1L << selectedBatch)

    var addedDep = false
    var w = 0
    while (w < ROW_WORDS) {
      if (reqDeps(w) != 0) {
        val oldDeps = forwardDeps(selectedBatch)(w)
        val newDeps = oldDeps | reqDeps(w)

        if (oldDeps != newDeps) {
          forwardDeps(selectedBatch)(w) = newDeps
          addedDep = true

          // Mark selectedBatch as a child of every new parent
          var diff = newDeps ^ oldDeps // bits that changed from 0 to 1
          while (diff != 0) {
            val parentBit = numberOfTrailingZeros(diff)
            val parentIdx = (w << 6) + parentBit
            reverseDeps(parentIdx)(selectedBatch >> 6) |= (1L << selectedBatch)
            diff &= (diff - 1)
          }
        }
        reqDeps(w) = 0
      }
      w += 1
    }

    if (wasReady && addedDep) {
      readyBatchesMask(selectedBatch >> 6) &= ~(1L << selectedBatch)
    }
    addedDep
  }

  /**
   * Discard the resolved dependencies of a row that could not be placed.
   */
  final protected def clearReqDeps(): Unit = {
    var w = 0
    while (w < ROW_WORDS) {
      reqDeps(w) = 0
      w += 1
    }
  }

  /**
   * Make idx the last writer of every given RAID.
   */
  final protected def recordLastWriter(raids: Array[AnyValue], idx: Int): Unit = {
    val encodedOwner = (batchSequence(idx) << 16) | (idx + 1).toLong
    var i = 0
    while (i < raids.length) {
      raidLastWriter.put(raids(i), encodedOwner)
      i += 1
    }
  }

  /**
   * Count one more row in idx, sealing the batch when it becomes full.
   */
  final protected def bumpFillCount(idx: Int, addedDep: Boolean, wasReady: Boolean): Unit = {
    val newFillCount = batchFillCount(idx) + 1
    batchFillCount(idx) = newFillCount
    if (newFillCount >= batchSize) {
      val wordIndex = idx >> 6
      openBatchesMask(wordIndex) &= ~(1L << idx)
      if (wasReady && !addedDep) {
        filledReadyBatchesMask(wordIndex) |= (1L << idx)
      }
    }
  }

  /**
   * First batch that is both ready (zero forward dependencies) and open, with room.
   * Joining it adds no dependencies in either direction.
   */
  final protected def firstCleanOpenBatchWithRoom(): Int = {
    var w = 0
    while (w < ROW_WORDS) {
      var word = readyBatchesMask(w) & openBatchesMask(w)
      while (word != 0) {
        val bit = numberOfTrailingZeros(word)
        val idx = (w << 6) + bit
        if (batchFillCount(idx) < batchSize) {
          return idx
        }
        word &= ~(1L << bit)
      }
      w += 1
    }
    -1
  }

  /**
   * First open batch with room whose sequence is at least minSeq (the cycle-prevention rule).
   */
  final protected def firstOpenWithRoomAndSeq(minSeq: Long): Int = {
    var w = 0
    while (w < ROW_WORDS) {
      var word = openBatchesMask(w)
      while (word != 0) {
        val bit = numberOfTrailingZeros(word)
        val idx = (w << 6) + bit
        if (batchSequence(idx) >= minSeq && batchFillCount(idx) < batchSize) {
          return idx
        }
        word &= ~(1L << bit)
      }
      w += 1
    }
    -1
  }

  // Checks (reqDeps \ {idx}) subset-of forwardDeps(idx), over ROW_WORDS longs
  private def reqDepsCoveredBy(idx: Int): Boolean = {
    val selfWord = idx >> 6
    val selfBit = 1L << idx
    val fwd = forwardDeps(idx)
    var k = 0
    while (k < ROW_WORDS) {
      var req = reqDeps(k)
      if (k == selfWord) {
        req &= ~selfBit
      }
      if ((req & ~fwd(k)) != 0) {
        return false
      }
      k += 1
    }
    true
  }

  // --- HELPERS ---

  // Returns the index of the newly allocated batch, or -1 if all batches are active (saturation)
  protected def allocateNewBatch(): Int = {
    // Simple Ring Buffer allocation
    // Start from nextAllocationIndex until we find an empty slot. Wrap around if necessary.
    // If we cannot find an empty slot, backpressure is needed until some batches can be completed.
    var attempts = 0
    var idx = nextAllocationIndex
    while ((activeBatchesMask(idx >> 6) & (1L << idx)) != 0) {
      idx = (idx + 1) % maxBatches
      attempts += 1
      if (attempts >= maxBatches) {
        return -1
      }
    }

    // Update the allocation index for the next allocation
    nextAllocationIndex = (idx + 1) % maxBatches

    // Reset State
    globalSequenceCounter += 1
    batchSequence(idx) = globalSequenceCounter
    batchFillCount(idx) = 0

    onBatchAllocated(idx)

    // Clear dependencies (array fill is fast for small ranges)
    var w = 0
    while (w < ROW_WORDS) {
      forwardDeps(idx)(w) = 0
      reverseDeps(idx)(w) = 0
      w += 1
    }

    // Mark as Active, Open and Ready initially (it has 0 deps until we add them)
    val wIdx = idx >> 6
    activeBatchesMask(wIdx) |= (1L << idx)
    openBatchesMask(wIdx) |= (1L << idx)
    readyBatchesMask(wIdx) |= (1L << idx)

    idx
  }

  // Helper to check if a specific batch has zero dependencies remaining
  @inline private def checkIfBatchReady(batchIdx: Int): Boolean = {
    var k = 0
    while (k < ROW_WORDS) {
      if (forwardDeps(batchIdx)(k) != 0) return false
      k += 1
    }
    true
  }

  // Helper to check if a batch is currently marked ready
  protected def isReady(batchIdx: Int): Boolean = {
    val wIdx = batchIdx >> 6
    (readyBatchesMask(wIdx) & (1L << batchIdx)) != 0
  }

  /**
   * Hook invoked from [[allocateNewBatch]] after a slot's sequence and fill count are reset and
   * before its dependency rows are cleared. No-op by default.
   */
  protected def onBatchAllocated(idx: Int): Unit = ()

  /**
   * Hook invoked from [[completeBatch]] after the batch's active bit is cleared and stale RAID
   * entries are purged, and before its children are notified. No-op by default.
   */
  protected def releaseBatchReservations(completedBatchIdx: Int): Unit = ()

  @VisibleForTesting
  private[pipes] def raidMapSize: Long = raidLastWriter.size()
}
