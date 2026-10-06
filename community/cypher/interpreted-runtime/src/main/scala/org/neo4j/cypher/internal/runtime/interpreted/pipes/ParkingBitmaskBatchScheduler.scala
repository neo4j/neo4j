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

import org.neo4j.collection.trackable.HeapTrackingArrayDeque
import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.collection.trackable.HeapTrackingUnifiedMap
import org.neo4j.cypher.internal.macros.AssertMacros3.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ParkingBitmaskBatchScheduler.ParkedEntry
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker
import org.neo4j.util.VisibleForTesting
import org.neo4j.values.AnyValue

import java.lang.Long.numberOfTrailingZeros

/**
 * A variant of [[BitmaskBatchScheduler]] that resolves RAID conflicts in time instead of space:
 * an admission controller in front of batch formation, rather than a dependency-DAG builder.
 *
 * The greedy and affinity policies must place every arriving row into some batch immediately, so
 * a live conflict must be encoded as a dependency edge between batches. Edges chain transitively
 * (dependency contagion), and on scattered input with even sparse conflicts the whole workload
 * degenerates into serial execution. But the executing set only ever needs a handful of mutually
 * RAID-disjoint batches at any instant. This scheduler therefore keeps dispatched batches
 * conflict-free by construction:
 *
 *  - A row whose RAIDs are all unreserved joins a clean open batch and reserves its RAIDs.
 *  - A row with exactly one RAID owned by an open batch with room joins that batch
 *    (self-absorption), which keeps same-key rows together without creating edges.
 *  - Any other conflicted row is PARKED: held outside all batches in per-RAID FIFO queues,
 *    and admitted later, when completions have released its blockers.
 *
 * Why parking cannot be emulated by just opening more batches in formation (WAIT-FOR vs
 * WAIT-BEHIND): a conflicted row placed into any batch at arrival, even a fresh one with a
 * precise dependency edge, waits FOR its blocker structurally, and that structure propagates.
 * The placed row becomes the last writer of its RAIDs, so with multiple RAIDs per row every
 * batched-but-blocked row exports its batch's blockedness through its other keys: the next
 * arrival on such a key acquires an edge to the blocked batch and the chain grows (contagion).
 * Batches also dispatch as a unit, so grouping conflicted rows unions their blocker sets, while
 * isolating them one-per-batch degenerates into single-row commits and consumes formation slots
 * whose dependency-matrix cost grows quadratically with maxBatches. A parked row instead waits
 * BEHIND its keys in time: it never touches last-writer tracking, so its conflict cannot
 * propagate to anyone (later arrivals on the same keys simply queue behind it), and its
 * placement is decided only at admission, after the conflict has expired, when the row is
 * typically clean and can be packed into batches that are both full and edge-free. Arrival-time
 * placement freezes membership and edges against conflicts that no longer exist by dispatch
 * time; late binding is the fundamental advantage, not the buffering itself (in the healthy
 * regime the parking lot holds well under one batch worth of rows).
 *
 * Ordering and liveness are structural. Every parked row carries a ticket (its input sequence
 * number) and is enqueued on ALL of its RAIDs' queues in ticket order, and an arriving row may
 * not claim a RAID that has parked waiters (the arrival fence). A parked row is only admitted
 * when it is at the head of every queue it is on, so same-RAID rows execute in input order, and
 * the globally oldest parked row is always at the head of all of its queues, blocked only by
 * active batches, which guarantees progress. As a defense in depth, a row that is repeatedly
 * evaluated and still blocked (strikes) is force-placed using dependency edges, exactly like the
 * affinity scheduler would have placed it. The dependency-DAG machinery is retained for these
 * forced placements, for draining parked rows when the input is exhausted, and for the hot-RAID
 * guard below, so the safety invariant (batches sharing a RAID never execute concurrently) is
 * inherited unchanged.
 *
 * Degradation is explicit, never silent:
 *  - Parking budget full: insertRow returns BACKPRESSURE so the iterator throttles input until
 *    completions release reservations (the whole stream keeps benefiting from parking at a
 *    bounded memory footprint).
 *  - Hot-RAID guard: when one RAID value accumulates more than hotQueueCap parked waiters
 *    (a supernode), parked rows are drained from the GLOBAL ticket head using dependency edges
 *    until the pressure is relieved. Draining from the global head is what makes this order-safe:
 *    the oldest parked row never has a parked predecessor on any of its RAIDs. (Draining the hot
 *    queue's own head instead would be unsafe for multi-RAID rows, whose other RAID can have an
 *    earlier-ticket waiter.)
 *
 * Memory: parked row objects live in the iterator (see [[ParkedRowSink]]); this
 * scheduler owns the queue structures and retained RAID arrays, all heap-tracked.
 *
 * @param batchSize The maximum number of rows per batch
 * @param raidsPerRow The number of RAID expressions per row
 * @param maxBatches The maximum number of batches that can be in formation at once (must be a
 *                   multiple of 64)
 * @param parkingBudget The maximum number of rows parked at any one time (> 0)
 * @param memoryTracker Tracker for scheduler-owned heap
 * @param strikeLimit Number of blocked head-of-queue evaluations before a parked row is
 *                    force-placed with dependency edges
 * @param hotQueueCap Maximum parked waiters on a single RAID value before the hot-RAID guard
 *                    starts draining from the global ticket head
 */
class ParkingBitmaskBatchScheduler(
  batchSize: Int,
  raidsPerRow: Int,
  maxBatches: Int = 256,
  parkingBudget: Int,
  memoryTracker: MemoryTracker,
  strikeLimit: Int = ParkingBitmaskBatchScheduler.DEFAULT_STRIKE_LIMIT,
  hotQueueCap: Int = 0
) extends AbstractBitmaskBatchScheduler(batchSize, raidsPerRow, maxBatches, memoryTracker) {

  require(parkingBudget > 0, s"parkingBudget must be > 0, got $parkingBudget")

  // Long arithmetic: batchSize * 4 overflows Int for user-expressible batch sizes
  private[this] val effectiveHotQueueCap: Int =
    if (hotQueueCap > 0) hotQueueCap
    else Math.toIntExact(Math.min(Math.max(64L, batchSize.toLong * 4L), parkingBudget.toLong))

  // --- STATE: PARKING ---
  // RAID -> parked waiters in ticket order. A lone waiter is stored as the ParkedEntry itself and
  // is promoted to a FIFO deque on the second waiter, so the dominant single-waiter case allocates
  // no queue structure. Contains only non-empty values: a RAID is removed as soon as its last
  // entry leaves, which bounds the map by parked entries * raids.
  private[this] val parkQueues: HeapTrackingUnifiedMap[AnyValue, AnyRef] =
    HeapTrackingCollections.newMap(memoryTracker)

  // Global intrusive doubly-linked list of parked entries in ticket order (head = oldest).
  // The head is the drain cursor: it is structurally at the head of all of its queues.
  private[this] var parkedHead: ParkedEntry = _
  private[this] var parkedTail: ParkedEntry = _
  private[this] var parkedRowCount: Int = 0

  // Free park slots in the iterator's parked-row storage
  private[this] val freeSlots = {
    val slots = new Array[Int](parkingBudget)
    var s = 0
    while (s < parkingBudget) {
      slots(s) = parkingBudget - 1 - s
      s += 1
    }
    slots
  }
  private[this] var freeSlotsTop: Int = parkingBudget

  // RAIDs released by completions whose park queues have not been re-evaluated yet
  private[this] val wakeQueue: HeapTrackingArrayDeque[AnyValue] =
    HeapTrackingCollections.newArrayDeque(memoryTracker)

  // Per-batch list of RAIDs the batch owns, used only to wake park queues at completion.
  // Deduplicated on append (a RAID is appended only when its last writer changes to this batch).
  private[this] val batchRaids = new Array[HeapTrackingArrayDeque[AnyValue]](maxBatches)

  // Number of park queues currently exceeding effectiveHotQueueCap
  private[this] var overCapQueues: Int = 0

  private[this] val parkingFixedHeap: Long =
    HeapEstimator.sizeOfIntArray(parkingBudget) + HeapEstimator.shallowSizeOfObjectArray(maxBatches)
  memoryTracker.allocateHeap(parkingFixedHeap)

  // --- INTERNAL COUNTERS ---
  private[this] var ticketCounter: Long = 0L
  private[this] var parkedEntriesHeap: Long = 0L

  // Completion epoch for strike accounting: a still-blocked parked row accrues at most one strike
  // per completion, however many duplicate wakes that completion produces for its RAID queues
  private[this] var wakeEpoch: Long = 0L

  // --- INSTRUMENTATION (for tests and analysis) ---
  private[this] var _parkedPeak: Int = 0
  private[this] var _reAdmissionChecks: Long = 0L
  private[this] var _strikeouts: Long = 0L
  private[this] var _forcePlacedRows: Long = 0L
  private[this] var _hotDrainedRows: Long = 0L

  // Scratch state filled by resolveOwners for the zero-edge admission decision
  private[this] var resolvedBlockedOwner: Boolean = false
  private[this] var resolvedOpenOwnerIdx: Int = -1
  private[this] var resolvedOpenOwnerCount: Int = 0

  override def parkingCapacity: Int = parkingBudget

  override def hasParkedRows: Boolean = parkedRowCount > 0

  /**
   * Insert a row: zero-edge placements and parking only. Rows that cannot be placed without
   * dependency edges are parked (or rejected with BACKPRESSURE when the parking budget is full).
   */
  def insertRow(rowRaids: Array[AnyValue]): Int = {
    val raids = dedupRaids(rowRaids)
    val ticket = ticketCounter
    ticketCounter += 1

    // 1. Arrival fence: a row may not overtake parked rows on any of its RAIDs, even when the
    // RAID itself is currently unreserved, or per-key input order would break. The fence only
    // matters when something is parked, so the empty-lot hot path skips the map probes entirely.
    if (parkedRowCount > 0) {
      var i = 0
      while (i < raids.length) {
        if (parkQueues.containsKey(raids(i))) {
          return park(rowRaids, raids, ticket)
        }
        i += 1
      }
    }

    // 2. Resolve live reservations
    resolveOwners(raids)

    if (resolvedBlockedOwner || resolvedOpenOwnerCount >= 2) {
      // Some RAID is owned by a full, ready or executing batch, or joining one open owner would
      // need an edge to another open owner. Resolve the conflict in time instead.
      return park(rowRaids, raids, ticket)
    }

    // 3. Zero-edge placement
    if (resolvedOpenOwnerCount == 1) {
      // Self-absorption: the single open owner batch absorbs the row, no edges needed
      val idx = resolvedOpenOwnerIdx
      recordZeroEdgePlacement(raids, idx)
      return idx
    }

    // No live owners: a clean open batch, or a fresh allocation
    val cleanIdx = firstCleanOpenBatchWithRoom()
    if (cleanIdx >= 0) {
      recordZeroEdgePlacement(raids, cleanIdx)
      return cleanIdx
    }
    val freshIdx = allocateNewBatch()
    if (freshIdx >= 0) {
      recordZeroEdgePlacement(raids, freshIdx)
      return freshIdx
    }
    // All batch slots are active. There is nothing to park FOR (no live conflict), so waiting
    // for completions to free a slot is the correct response.
    BatchScheduler.BACKPRESSURE
  }

  /**
   * Move admissible parked rows into batches.
   */
  override def admitParked(force: Boolean, sink: ParkedRowSink): Unit = {
    // Hot-RAID guard first: drain from the global ticket head with dependency edges until no
    // queue is over the cap. This runs regardless of force, it is an explicit degradation valve.
    if (overCapQueues > 0) {
      if (!drainWhileOverCap(sink)) {
        return
      }
    }
    if (force) {
      drainAll(sink)
    } else {
      admitZeroEdge(sink)
    }
  }

  /**
   * Select a batch for execution.
   * Returns batch index or -1 if none.
   */
  override def pollReadyBatch(force: Boolean): Int = {
    val filled = pollFilledReadyBatch()
    if (filled >= 0) {
      return filled
    }
    if (force) {
      // Select the fullest ready batch even if it's not full, to avoid a proliferation of tiny
      // batches (and thus serialized commits) on the drain path
      var bestIdx = -1
      var bestFill = -1
      var w = 0
      while (w < ROW_WORDS) {
        var word = readyBatchesMask(w)
        while (word != 0) {
          val bit = numberOfTrailingZeros(word)
          val idx = (w << 6) + bit
          if (batchFillCount(idx) > bestFill) {
            bestFill = batchFillCount(idx)
            bestIdx = idx
          }
          word &= ~(1L << bit)
        }
        w += 1
      }
      if (bestIdx >= 0) {
        val wIdx = bestIdx >> 6
        readyBatchesMask(wIdx) &= ~(1L << bestIdx)
        openBatchesMask(wIdx) &= ~(1L << bestIdx)
        return bestIdx
      }
    }
    -1
  }

  /**
   * Wake the park queues waiting on the RAIDs the completed batch owned. Its reservations are
   * already released implicitly, through the staleness check.
   */
  override protected def releaseBatchReservations(completedBatchIdx: Int): Unit = {
    // Each completion starts a new strike epoch, so that duplicate wakes from one completion
    // strike a still-blocked entry at most once
    wakeEpoch += 1
    // Wake park queues waiting on RAIDs this batch owned
    val ownedRaids = batchRaids(completedBatchIdx)
    if (ownedRaids != null) {
      while (!ownedRaids.isEmpty) {
        val raid = ownedRaids.pollFirst()
        if (parkQueues.containsKey(raid)) {
          wakeQueue.addLast(raid)
        }
      }
      // The emptied deque stays in its slot for reuse by the next batch allocated there
    }
  }

  override def close(): Unit = {
    val queueIterator = parkQueues.values().iterator()
    while (queueIterator.hasNext) {
      queueIterator.next() match {
        case _: ParkedEntry => // lone waiters own no queue structure
        case waiters        => waiters.asInstanceOf[HeapTrackingArrayDeque[ParkedEntry]].close()
      }
    }
    parkQueues.close()
    wakeQueue.close()
    var i = 0
    while (i < maxBatches) {
      if (batchRaids(i) != null) {
        batchRaids(i).close()
        batchRaids(i) = null
      }
      i += 1
    }
    parkedHead = null
    parkedTail = null
    if (parkedEntriesHeap > 0) {
      memoryTracker.releaseHeap(parkedEntriesHeap)
      parkedEntriesHeap = 0
    }
    memoryTracker.releaseHeap(parkingFixedHeap)
    super.close()
  }

  // --- PARKING INTERNALS ---

  /** A parkQueues value is either a lone [[ParkedEntry]] or a deque of entries in ticket order. */
  @inline private def headOf(waiters: AnyRef): ParkedEntry = waiters match {
    case entry: ParkedEntry => entry
    case queue              => queue.asInstanceOf[HeapTrackingArrayDeque[ParkedEntry]].peekFirst()
  }

  private def park(rowRaids: Array[AnyValue], dedupedRaids: Array[AnyValue], ticket: Long): Int = {
    if (parkedRowCount >= parkingBudget) {
      return BatchScheduler.BACKPRESSURE
    }
    // The caller reuses its rowRaids array between insertRow calls, so retain a copy unless
    // dedupRaids already created one
    val raids =
      if (dedupedRaids eq rowRaids) java.util.Arrays.copyOf(dedupedRaids, dedupedRaids.length) else dedupedRaids
    freeSlotsTop -= 1
    val slot = freeSlots(freeSlotsTop)
    val entry = new ParkedEntry(slot, ticket, raids)
    var i = 0
    while (i < raids.length) {
      val raid = raids(i)
      parkQueues.get(raid) match {
        case null =>
          // Lone waiter: store the entry itself, no queue structure needed
          parkQueues.put(raid, entry)
        case lone: ParkedEntry =>
          // Second waiter: promote to a FIFO deque
          val queue = HeapTrackingCollections.newArrayDeque[ParkedEntry](memoryTracker)
          queue.addLast(lone)
          queue.addLast(entry)
          parkQueues.put(raid, queue)
          if (queue.size() == effectiveHotQueueCap + 1) {
            overCapQueues += 1
          }
        case waiters =>
          val queue = waiters.asInstanceOf[HeapTrackingArrayDeque[ParkedEntry]]
          queue.addLast(entry)
          if (queue.size() == effectiveHotQueueCap + 1) {
            overCapQueues += 1
          }
      }
      i += 1
    }
    if (parkedTail == null) {
      parkedHead = entry
      parkedTail = entry
    } else {
      parkedTail.globalNext = entry
      entry.globalPrev = parkedTail
      parkedTail = entry
    }
    parkedRowCount += 1
    if (parkedRowCount > _parkedPeak) {
      _parkedPeak = parkedRowCount
    }
    val entryHeap = ParkingBitmaskBatchScheduler.PARKED_ENTRY_SHALLOW_SIZE +
      HeapEstimator.shallowSizeOfObjectArray(raids.length)
    memoryTracker.allocateHeap(entryHeap)
    parkedEntriesHeap += entryHeap
    BatchScheduler.encodeParked(slot)
  }

  /**
   * Remove an entry from all of its queues and the global list. The entry must be at the head of
   * every queue it is on (which is structurally true for every admission and drain site).
   */
  private def removeParked(entry: ParkedEntry): Unit = {
    var i = 0
    while (i < entry.raids.length) {
      val raid = entry.raids(i)
      val waiters = parkQueues.get(raid)
      checkOnlyWhenAssertionsAreEnabled(
        waiters != null && (headOf(waiters) eq entry),
        "Parked entry must be removed at the head of all of its queues"
      )
      waiters match {
        case _: ParkedEntry =>
          parkQueues.remove(raid)
        case _ =>
          val queue = waiters.asInstanceOf[HeapTrackingArrayDeque[ParkedEntry]]
          queue.pollFirst()
          if (queue.size() == effectiveHotQueueCap) {
            overCapQueues -= 1
          }
          if (queue.isEmpty) {
            parkQueues.remove(raid)
            queue.close()
          }
      }
      i += 1
    }
    if (entry.globalPrev == null) {
      parkedHead = entry.globalNext
    } else {
      entry.globalPrev.globalNext = entry.globalNext
    }
    if (entry.globalNext == null) {
      parkedTail = entry.globalPrev
    } else {
      entry.globalNext.globalPrev = entry.globalPrev
    }
    entry.globalPrev = null
    entry.globalNext = null
    freeSlots(freeSlotsTop) = entry.slot
    freeSlotsTop += 1
    parkedRowCount -= 1
    val entryHeap = ParkingBitmaskBatchScheduler.PARKED_ENTRY_SHALLOW_SIZE +
      HeapEstimator.shallowSizeOfObjectArray(entry.raids.length)
    memoryTracker.releaseHeap(entryHeap)
    parkedEntriesHeap -= entryHeap
  }

  /**
   * Zero-edge admission after completions: evaluate the heads of woken queues, oldest ticket
   * first, and place every entry that no longer needs dependency edges. Placing an entry can
   * cascade (the next entry in the same queue may join the same open batch).
   */
  private def admitZeroEdge(sink: ParkedRowSink): Unit = {
    while (!wakeQueue.isEmpty) {
      val raid = wakeQueue.peekFirst()
      val waiters = parkQueues.get(raid)
      if (waiters == null) {
        // All entries for this RAID left the parking lot through other queues or drains
        wakeQueue.pollFirst()
      } else {
        val entry = headOf(waiters)
        if (!isHeadOfAllQueues(entry)) {
          // An earlier-ticket entry on another of this entry's RAIDs must go first. That entry's
          // blocker is an active batch, whose completion will wake this queue again.
          wakeQueue.pollFirst()
        } else {
          _reAdmissionChecks += 1
          resolveOwners(entry.raids)
          if (resolvedBlockedOwner || resolvedOpenOwnerCount >= 2) {
            // Still conflicted. Strike at most once per completion epoch (duplicate wakes for one
            // completion must not burn several strikes), and after strikeLimit blocked evaluations
            // force the placement with dependency edges (order-safe: the entry is head of all its
            // queues)
            if (entry.lastStrikeEpoch != wakeEpoch) {
              entry.lastStrikeEpoch = wakeEpoch
              entry.strikes += 1
            }
            if (entry.strikes >= strikeLimit) {
              val idx = insertWithDependencies(entry.raids)
              if (idx < 0) {
                return
              }
              _strikeouts += 1
              _forcePlacedRows += 1
              removeParked(entry)
              sink.place(entry.slot, idx)
            } else {
              wakeQueue.pollFirst()
            }
          } else {
            val idx =
              if (resolvedOpenOwnerCount == 1) {
                resolvedOpenOwnerIdx
              } else {
                val cleanIdx = firstCleanOpenBatchWithRoom()
                if (cleanIdx >= 0) cleanIdx else allocateNewBatch()
              }
            if (idx < 0) {
              // No batch slot available. Stop and resume after the next completion.
              return
            }
            recordZeroEdgePlacement(entry.raids, idx)
            removeParked(entry)
            sink.place(entry.slot, idx)
            // Deliberately not polling the wake queue: the next head of this RAID's queue may
            // now be admissible into the same open batch (cascade)
          }
        }
      }
    }
  }

  /**
   * Drain every parked row into batches using dependency edges, in global ticket order.
   * Used when the input is exhausted and as the liveness fallback. May stop early on batch-slot
   * exhaustion and is resumed by subsequent calls.
   */
  private def drainAll(sink: ParkedRowSink): Unit = {
    while (parkedHead != null) {
      val entry = parkedHead
      val idx = insertWithDependencies(entry.raids)
      if (idx < 0) {
        return
      }
      _forcePlacedRows += 1
      removeParked(entry)
      sink.place(entry.slot, idx)
    }
  }

  /**
   * Hot-RAID guard: drain from the global ticket head until no queue is over the cap.
   * Returns false when it stopped early on batch-slot exhaustion.
   */
  private def drainWhileOverCap(sink: ParkedRowSink): Boolean = {
    while (overCapQueues > 0 && parkedHead != null) {
      val entry = parkedHead
      val idx = insertWithDependencies(entry.raids)
      if (idx < 0) {
        return false
      }
      _hotDrainedRows += 1
      _forcePlacedRows += 1
      removeParked(entry)
      sink.place(entry.slot, idx)
    }
    true
  }

  @inline private def isHeadOfAllQueues(entry: ParkedEntry): Boolean = {
    var i = 0
    while (i < entry.raids.length) {
      val waiters = parkQueues.get(entry.raids(i))
      if (waiters == null || (headOf(waiters) ne entry)) {
        return false
      }
      i += 1
    }
    true
  }

  // --- PLACEMENT INTERNALS ---

  /**
   * Deduplicate the RAID array in the (rare) case the same value appears more than once in one
   * row, so that parking never enqueues an entry twice on the same queue.
   */
  private def dedupRaids(rowRaids: Array[AnyValue]): Array[AnyValue] = {
    if (rowRaids.length == 1) {
      return rowRaids
    }
    var duplicates = 0
    var i = 1
    while (i < rowRaids.length) {
      var j = 0
      var isDuplicate = false
      while (j < i && !isDuplicate) {
        if (rowRaids(i) == rowRaids(j)) {
          isDuplicate = true
        }
        j += 1
      }
      if (isDuplicate) {
        duplicates += 1
      }
      i += 1
    }
    if (duplicates == 0) {
      return rowRaids
    }
    val deduped = new Array[AnyValue](rowRaids.length - duplicates)
    var k = 0
    i = 0
    while (i < rowRaids.length) {
      var j = 0
      var isDuplicate = false
      while (j < i && !isDuplicate) {
        if (rowRaids(i) == rowRaids(j)) {
          isDuplicate = true
        }
        j += 1
      }
      if (!isDuplicate) {
        deduped(k) = rowRaids(i)
        k += 1
      }
      i += 1
    }
    deduped
  }

  /**
   * Resolve which live batches own the given RAIDs, into the resolved* scratch fields.
   * An owner is live when it is active and its stored sequence matches (the staleness check).
   */
  private def resolveOwners(raids: Array[AnyValue]): Unit = {
    resolvedBlockedOwner = false
    resolvedOpenOwnerIdx = -1
    resolvedOpenOwnerCount = 0
    lastWriterScratchFor = raids
    var i = 0
    while (i < raids.length) {
      val encoded = raidLastWriter.get(raids(i))
      lastWriterScratch(i) = encoded
      val writerIdx = (encoded & 0xffffL).toInt - 1
      val writerStoredSeq = encoded >>> 16
      if (
        writerIdx != -1 &&
        (activeBatchesMask(writerIdx >> 6) & (1L << writerIdx)) != 0 &&
        batchSequence(writerIdx) == writerStoredSeq
      ) {
        if (
          (openBatchesMask(writerIdx >> 6) & (1L << writerIdx)) != 0 &&
          batchFillCount(writerIdx) < batchSize
        ) {
          if (writerIdx != resolvedOpenOwnerIdx) {
            if (resolvedOpenOwnerIdx == -1) {
              resolvedOpenOwnerIdx = writerIdx
              resolvedOpenOwnerCount = 1
            } else {
              resolvedOpenOwnerCount += 1
            }
          }
        } else {
          resolvedBlockedOwner = true
        }
      }
      i += 1
    }
  }

  /**
   * Record a placement that adds no dependency edges: last-writer tracking, owned-RAID list for
   * completion wakes, and fill-count maintenance.
   */
  private def recordZeroEdgePlacement(raids: Array[AnyValue], idx: Int): Unit = {
    updateRaidTracking(raids, idx)
    bumpFillCount(idx, addedDep = false, wasReady = isReady(idx))
  }

  private def updateRaidTracking(raids: Array[AnyValue], idx: Int): Unit = {
    checkOnlyWhenAssertionsAreEnabled(
      lastWriterScratchFor eq raids,
      "lastWriterScratch was not resolved for the raids being tracked"
    )
    val encodedOwner = (batchSequence(idx) << 16) | (idx + 1).toLong
    var i = 0
    while (i < raids.length) {
      // The previous stored value was stashed by resolveOwners or resolveDependencies
      if (lastWriterScratch(i) != encodedOwner) {
        val raid = raids(i)
        raidLastWriter.put(raid, encodedOwner)
        var owned = batchRaids(idx)
        if (owned == null) {
          owned = HeapTrackingCollections.newArrayDeque[AnyValue](memoryTracker)
          batchRaids(idx) = owned
        }
        owned.addLast(raid)
      }
      i += 1
    }
  }

  /**
   * Place a row using dependency edges, exactly like [[AffinityBitmaskBatchScheduler]] places a
   * dependent row. Used for strike-outs, drains and the hot-RAID guard.
   * @return the selected batch index, or -1 when all batch slots are active
   */
  private def insertWithDependencies(raids: Array[AnyValue]): Int = {
    resolveDependencies(raids)
    val selectedBatch = selectBatchByAffinity()
    if (selectedBatch == -1) {
      clearReqDeps()
      return -1
    }
    val wasReady = isReady(selectedBatch)
    val addedDep = linkDependencies(selectedBatch, wasReady)
    updateRaidTracking(raids, selectedBatch)
    bumpFillCount(selectedBatch, addedDep, wasReady)
    selectedBatch
  }

  // --- HELPERS ---

  /**
   * Allocation hook: the parking scheduler tracks an owned-RAID list per batch slot, which must
   * have been consumed at completion before the shared ring buffer reuses the slot.
   */
  override protected def onBatchAllocated(idx: Int): Unit = {
    // Ring-reuse safety: the owned-RAID list must have been consumed at completion (the emptied
    // deque itself is kept for reuse)
    checkOnlyWhenAssertionsAreEnabled(
      batchRaids(idx) == null || batchRaids(idx).isEmpty,
      s"Owned-RAID list not consumed before slot reuse of batch $idx"
    )
  }

  @VisibleForTesting
  private[pipes] def parkedCount: Int = parkedRowCount

  @VisibleForTesting
  private[pipes] def parkedPeak: Int = _parkedPeak

  @VisibleForTesting
  private[pipes] def reAdmissionChecks: Long = _reAdmissionChecks

  @VisibleForTesting
  private[pipes] def strikeouts: Long = _strikeouts

  @VisibleForTesting
  private[pipes] def forcePlacedRows: Long = _forcePlacedRows

  @VisibleForTesting
  private[pipes] def hotDrainedRows: Long = _hotDrainedRows
}

object ParkingBitmaskBatchScheduler {

  final val DEFAULT_STRIKE_LIMIT = 4

  // Cap on the derived parking budget, bounding the iterator's parked-row slot array
  final val MAX_DERIVED_PARKING_BUDGET: Int = 1 << 20

  final private val PARKED_ENTRY_SHALLOW_SIZE: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[ParkedEntry])

  /**
   * The parking budget to use: the configured value when positive, otherwise a quarter of the
   * formation window (maxBatches * batchSize) rows, which simulation showed dominates larger
   * budgets in the collapse regime and is irrelevant in the healthy regime.
   */
  def effectiveParkingBudget(configured: Int, maxBatches: Int, batchSize: Int): Int = {
    if (configured > 0) {
      configured
    } else {
      val window = maxBatches.toLong * batchSize
      Math.toIntExact(Math.min(Math.max(64L, window / 4), MAX_DERIVED_PARKING_BUDGET.toLong))
    }
  }

  /**
   * A row held outside all batches, waiting for its blocking batches to complete.
   *
   * Entries also form an intrusive doubly linked list in global ticket order (globalPrev/globalNext).
   * It preserves ticket-ordered drains from the head while allowing constant-time unlink from the
   * middle, which is the common removal: zero-edge admission removes queue heads that sit anywhere
   * in the global order. The two link fields are covered by the shallow-size heap estimate.
   *
   * @param slot index into the iterator's parked-row storage
   * @param ticket the row's input sequence number (admission order per RAID)
   * @param raids the row's deduplicated RAID values, copied at park time (the caller reuses its
   *              rowRaids array between insertRow calls)
   */
  final private[pipes] class ParkedEntry(
    val slot: Int,
    val ticket: Long,
    val raids: Array[AnyValue]
  ) {
    var strikes: Int = 0
    // Last completion epoch this entry was struck in, so one completion strikes it at most once
    var lastStrikeEpoch: Long = -1L
    var globalPrev: ParkedEntry = _
    var globalNext: ParkedEntry = _
  }
}
