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
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ConcurrentTransactionsDeadlockPreventionLogic.BatchState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DependencyTrackingTransactionBatch.WAIT_MARKER_BATCH
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionRetryLogic.RetryState
import org.neo4j.kernel.impl.util.collection.EagerBuffer
import org.neo4j.kernel.impl.util.collection.EagerBuffer.createEagerBuffer
import org.neo4j.memory.MemoryTracker
import org.neo4j.util.VisibleForTesting
import org.neo4j.values.AnyValue

import java.lang.Long.numberOfTrailingZeros

import scala.concurrent.duration.Duration

/**
 * Interface for deadlock prevention logic in concurrent transactions.
 */
trait TransactionDeadlockPreventionLogic {

  /**
   * Creates a batch iterator that groups rows into batches.
   * The iterator should yield batches in an order that prevents deadlocks.
   */
  def batchIterator(
    input: ClosingIterator[CypherRow],
    state: QueryState,
    batchSize: Long,
    memoryTracker: MemoryTracker
  ): ClosingIterator[TransactionBatch]

  /**
   * Called to check if this logic requires onBatchCompleted to be called.
   * This is used to eliminate unnecessary peeking of the output queue.
   */
  def requiresBatchCompletedNotification: Boolean

  /**
   * Called when a batch has completed execution (committed or failed),
   * but only if requiresBatchCompletedNotification returns true.
   * This allows the logic to update its internal state and potentially unblock waiting batches.
   * NOTE: We can expect multiple calls to onBatchCompleted for the same batch, so the implementation should be idempotent.
   */
  def onBatchCompleted(batch: TransactionBatch): Unit
}

/**
 * No-op implementation used when deadlock prevention is disabled.
 */
object NoopTransactionDeadlockPreventionLogic extends TransactionDeadlockPreventionLogic {

  override def batchIterator(
    input: ClosingIterator[CypherRow],
    state: QueryState,
    batchSize: Long,
    memoryTracker: MemoryTracker
  ): ClosingIterator[TransactionBatch] = {
    input.eagerGrouped(batchSize, memoryTracker).map(TransactionBatch(_))
  }

  override def requiresBatchCompletedNotification: Boolean = false

  override def onBatchCompleted(batch: TransactionBatch): Unit = {
    // No-op
  }
}

/**
 * Deadlock prevention logic that sorts incoming rows into batches based on resource acquisition IDs (RAIDs).
 *
 * The algorithm:
 * - Assigns a canonical ID (CRAID) to each batch based on the first row's canonical expression value
 * - Maintains a distinct set of RAIDs for each batch
 * - Groups rows into batches to minimize RAID overlaps between concurrent batches
 * - Maintains a dependency graph (DAG) based on overlapping RAID sets
 * - Batches without dependencies are handed off for concurrent execution
 * - Batches with dependencies wait until their dependencies complete
 *
 * ==Unified RAID Map vs. Separate Resource Group Maps==
 *
 * All RAID expressions are tracked in a single unified map, regardless of which expression produced each value.
 * This means that if two expressions (e.g. `aid` and `bid`) produce the same value in different batches,
 * a dependency is recorded between those batches even if the expressions reference independent resource types
 * (e.g. different node labels with separate unique indexes).
 *
 * This is a deliberate design choice that favors robustness over maximum concurrency:
 *
 * '''The cost: false dependencies'''
 * When RAID expressions reference independent resource types (e.g. `MERGE (a:A {id: aid})`
 * and `MERGE (b:B {id: bid})` with distinct labels `A` and `B`), the same property value `"v3"` maps
 * to different physical nodes `A(id="v3")` and `B(id="v3")`. In this case the unified map records a
 * false dependency: batches that share the value `"v3"` across `aid` and `bid` are serialized even
 * though they operate on different nodes and could safely run concurrently. This reduces achievable
 * parallelism when value ranges overlap across expressions that happen to be independent.
 *
 * In practice, the false dependency overhead is often negligible: unique property value ranges across
 * different label indexes tend to be non-overlapping (e.g. user IDs vs. product codes), in which case
 * the unified map produces no false dependencies and achieves the same concurrency as separate maps
 * would.
 *
 * '''An alternative design: grouped RAID maps'''
 * A more precise approach would be to use separate RAID maps per ''resource group'', where each group
 * tracks expressions that share a lock namespace. The `disjointBy` specification would change from a flat
 * `Seq[Expression]` to a `Seq[Seq[Expression]]`, where each inner sequence is one resource group.
 * Two batches would be dependent only if they overlap within at least one group, not across groups.
 *
 * For example, `disjointBy = Seq(Seq("aid", "bid"), Seq("cid"))` would track `aid` and `bid` together
 * (they share label `N`), while `cid` (referencing label `C`) gets its own independent map. This
 * eliminates false cross-group dependencies while preserving safety within each group.
 *
 * Correctly constructed resource groups are strictly more expressive than the unified map: placing all
 * expressions in a single group (`Seq(Seq("aid", "bid", "cid"))`) replicates the current unified
 * behavior exactly, while splitting into multiple groups can only improve concurrency. The unified map
 * has no inherent safety advantage over correctly constructed resource groups. The same-label scenario
 * described above (e.g. `aid` and `bid` both referencing label `N`) is handled correctly by putting those
 * expressions in the same group.
 *
 * '''Why the unified map is still the default today:'''
 * The unified map is the most conservative, and therefore most robust, configuration achievable
 * within value-based dependency tracking (see inherent limitations below). Its key property is that it requires ''no planner analysis
 * at all'' and cannot be misconfigured. It is not susceptible to incorrect grouping decisions, whether
 * from a planner bug, an incomplete analysis, or (as in the current state of the code) a complete
 * absence of resource group analysis.
 * Any mistake in group assignment, such as placing two expressions that share a lock namespace into
 * separate groups, silently introduces potential deadlocks that would be difficult to diagnose.
 * It would also be an additional complexity for users specifying RAID expressions through Cypher syntax,
 * who would need to understand lock namespaces and label disjointness to construct correct resource
 * groups, an error-prone task that, if done incorrectly, silently introduces deadlock risk.
 * Furthermore, extending the Cypher syntax to express resource groups adds grammar and parser complexity
 * that is difficult to justify until the performance benefit is demonstrated.
 *
 * '''Requirements for safely adopting grouped RAID maps:'''
 * To safely emit grouped RAID expressions, the planner (or a rewriter) would need to:
 *  1. Analyze the `MERGE`/`CREATE` pattern to determine which unique indexes are used.
 *  2. Determine the label sets involved in each index lookup.
 *  3. Prove that the label sets across groups are disjoint,  either through static node type
 *     constraints, label-exclusivity constraints, or by inspecting the query plan to confirm that
 *     no single node variable can acquire labels from more than one group.
 *  4. If disjointness cannot be proven, fall back to a single unified group (the current behavior).
 *
 * '''Inherent limitation of value-based dependency tracking:'''
 * Note that neither the unified map nor grouped maps can detect all possible deadlocks. Both approaches
 * track ''property values'', but locks are acquired on ''physical node IDs''. When a node carries
 * multiple labels and is indexed under different properties on different indexes (e.g. `:A(id)` and
 * `:B(code)`), two batches may reach the same physical node through entirely different property values.
 * No value-based overlap exists, so neither design detects the dependency. For example:
 *  - Batch 0: `MERGE (a:A {id: "v1"})` locks node N (which has labels `:A:B`, properties `id="v1"`, `code="v2"`)
 *  - Batch 1: `MERGE (b:B {code: "v2"})` locks the same node N through a different index
 *  - RAID values `{"v1"}` and `{"v2"}` have no overlap and deadlock is possible regardless of map design.
 *
 * This limitation is inherent to any scheme that tracks dependency via expression values rather than
 * resolved physical entity IDs. Resolving to physical IDs at scheduling time would require index
 * lookups, negating the performance benefit of value-based tracking. In practice, `ON ERROR RETRY`
 * serves as a safety net for these rare edge cases.
 *
 * See https://linear.app/neo4j/issue/RUN-1011 for discussion on revisiting this design with grouped RAID maps.
 *
 * @param disjointBy Array of RAID expressions, where the first is the canonical ID expression (CRAID)
 */
class ConcurrentTransactionsDeadlockPreventionLogic(disjointBy: Array[Expression], maxBatchesInFormation: Int = 256)
    extends TransactionDeadlockPreventionLogic {
  require(disjointBy.length >= 1)

  // The array of Resource Acquisition ID (RAID) expressions
  private[this] val raidExpressions = disjointBy

  // Reference to the batch iterator (for completion notifications)
  private[this] var _batchIterator: DeadlockPreventingBatchIterator = _

  override def batchIterator(
    input: ClosingIterator[CypherRow],
    state: QueryState,
    batchSize: Long,
    memoryTracker: MemoryTracker
  ): ClosingIterator[TransactionBatch] = {
    val iterator = new DeadlockPreventingBatchIterator(
      input,
      state,
      Math.toIntExact(batchSize),
      memoryTracker,
      raidExpressions,
      maxBatchesInFormation
    )
    _batchIterator = iterator
    iterator
  }

  override def requiresBatchCompletedNotification: Boolean = true

  override def onBatchCompleted(batch: TransactionBatch): Unit = {
    val iterator = _batchIterator
    if (iterator != null) {
      iterator.onBatchCompleted(batch)
    }
  }
}

object ConcurrentTransactionsDeadlockPreventionLogic {
  sealed trait BatchState

  object BatchState {
    case object InFormation extends BatchState
    case object InFlight extends BatchState
    case object Completed extends BatchState
    case object Closed extends BatchState
  }
}

/**
 * Batch implementation for dependency tracking
 */

private[pipes] class DependencyTrackingTransactionBatch(
  val id: Long,
  val index: Int,
  val rows: EagerBuffer[CypherRow],
  var state: BatchState
) extends RetryableTransactionBatch with AutoCloseable {

  // Used in the retry priority queue to compare batches scheduled for retry
  private[this] var _retryState: RetryState = _
  private[this] var _nextRetryState: RetryState = _ // Used by tasks to see if we shouldRetryAgain on failure

  def addRow(row: CypherRow): Unit = {
    rows.add(row)
  }

  def size: Long = rows.size()

  def isFull(batchSize: Int): Boolean = rows.size() >= batchSize

  def dispatch(): DependencyTrackingTransactionBatch = {
    setState(BatchState.InFlight)
    this
  }

  override def close(): Unit = {
    // Note: rows (EagerBuffer) is NOT closed here - it is consumed by the iterator
  }

  private[pipes] def setState(newState: BatchState): Unit = {
    checkOnlyWhenAssertionsAreEnabled(
      {
        state match {
          case BatchState.InFormation if newState eq BatchState.InFlight =>
            true
          case BatchState.InFlight if newState eq BatchState.Completed =>
            true
          case _ =>
            false
        }
      },
      s"${getClass.getSimpleName}: Illegal state transition from $state to $newState"
    )
    if (DebugSupport.DEBUG_BATCH_FORMATION) {
      DebugSupport.BATCH_FORMATION.log("State changed to %s on batch %s", newState, this)
    }
    state = newState
  }

  override def toString: String = {
    val retryStr = if (_retryState == null) ""
    else {
      require(_nextRetryState != null)
      s", retryState=${_retryState}, nextRetryState=${_nextRetryState}"
    }
    s"Batch(#${id}, index=$index, rowCount=${rows.size()}, state=$state$retryStr)"
  }

  // ---------------------------------------------------------------------------
  // TransactionBatch
  override def computeNextRetryState(retryLogic: TransactionRetryLogic): RetryableTransactionBatch = {
    if (_nextRetryState == null) {
      _nextRetryState = retryLogic.newRetryState()
    }
    _retryState = _nextRetryState
    _nextRetryState = _nextRetryState.computeNextRetryState()
    this
  }

  override def nanosUntilRetry(): Long = {
    _retryState.nanosUntilRetry()
  }

  override def shouldRetryAgain(): Boolean = {
    _nextRetryState == null || _nextRetryState.shouldRetryAgain()
  }

  override def retriedCount: Int = {
    if (_retryState == null) 0 else _retryState.retryCount + 1
  }

  override def retryTimeout: Duration = {
    if (_retryState == null) {
      throw new UnsupportedOperationException("retryTimeout is not supported for a fresh transaction batch")
    }
    _retryState.retryTimeout
  }

  override def retryState: RetryState = _retryState
}

object DependencyTrackingTransactionBatch {

  final val WAIT_MARKER_BATCH: DependencyTrackingTransactionBatch =
    new DependencyTrackingTransactionBatch(-1L, -1, null, BatchState.Completed) {
      override def isMarker: Boolean = true
    }
}

/**
 * Iterator that groups rows into batches using deadlock prevention logic.
 *
 * The iterator maintains batches in formation and yields them when:
 * - A batch is full and has no dependencies on in-flight batches
 * - The input queue is full and we need to yield batches without dependencies
 * - Input is exhausted
 *
 * Thread safety: This iterator is accessed from the main thread for iteration,
 * and from worker threads for batch completion notifications.
 */
private[pipes] class DeadlockPreventingBatchIterator(
  source: ClosingIterator[CypherRow],
  state: QueryState,
  batchSize: Int,
  memoryTracker: MemoryTracker,
  raidExpressions: Array[Expression],
  maxBatchesInFormation: Int
) extends ClosingIterator[TransactionBatch] {

  type Batch = DependencyTrackingTransactionBatch

  private[this] val scheduler =
    new BitmaskBatchScheduler(batchSize, raidExpressions.length, maxBatchesInFormation, memoryTracker)
  private[this] var batchCounter: Long = _

  // All batches in formation
  private val batchesInFormation: Array[Batch] = new Array[Batch](maxBatchesInFormation)
  private[this] var batchesInFormationCount: Int = 0
  private[this] var batchesInFlightCount: Int = 0

  private[this] var pendingRow: CypherRow = _

  // Flag to indicate if source is exhausted
  private[this] var sourceExhausted: Boolean = false
  private[this] var nextReadyBatch: Batch = _

  override protected[this] def innerHasNext: Boolean = {
    if (nextReadyBatch == null) {
      nextReadyBatch = formBatches()
      if (nextReadyBatch == null) {
        if (hasInFlightBatches && (hasBatchesInFormation || !sourceExhausted)) {
          // If we have in-flight batches and no ready batch, we need to wait for some to complete to
          // free up dependencies or free up batch slots.
          // We signal this by returning WAIT_MARKER_BATCH
          nextReadyBatch = WAIT_MARKER_BATCH
          if (DebugSupport.DEBUG_BATCH_FORMATION) {
            DebugSupport.BATCH_FORMATION.log("%s: waiting for in-flight batches to complete", this)
          }
        } else if (hasBatchesInFormation) {
          throw new IllegalStateException(
            s"No ready batches and no in-flight batches, but $batchesInFormationCount batches still in formation"
          )
        }
      }
    }
    nextReadyBatch != null
  }

  override def next(): TransactionBatch = {
    if (!innerHasNext) {
      throw new NoSuchElementException("next on empty iterator")
    }
    val batchToYield = nextReadyBatch
    nextReadyBatch = null
    if (batchToYield != WAIT_MARKER_BATCH) {
      dispatchBatch(batchToYield)
    } else {
      batchToYield
    }
  }

  /**
   * Called when a batch has completed execution, from the main thread, so does not need to be thread-safe.
   * NOTE: We can expect multiple calls to onBatchCompleted for the same batch, so the implementation should be idempotent.
   */
  def onBatchCompleted(batch: TransactionBatch): Unit = {
    val batchToComplete = batch.asInstanceOf[Batch]
    if (batchToComplete.state ne BatchState.Completed) {
      require(
        batchToComplete.state eq BatchState.InFlight,
        s"Illegal state on batch completed: ${batchToComplete.state} for $batchToComplete"
      )
      completeBatch(batchToComplete)
    }
  }

  override protected[this] def closeMore(): Unit = {
    source.close()
    pendingRow = null
    // Close all batches in formation
    var i = 0
    while (i < maxBatchesInFormation) {
      val batch = batchesInFormation(i)
      if (batch != null) {
        batch.rows.close()
        batch.close()
        batchesInFormation(i) = null
        batchesInFormationCount -= 1
      }
      i += 1
    }
    scheduler.close()
  }

  @inline private def hasInFlightBatches: Boolean = {
    batchesInFlightCount > 0
  }

  @inline private def hasBatchesInFormation: Boolean = {
    batchesInFormationCount > 0
  }

  /**
   * Pull rows from the source and form batches until either:
   * 1. We have at least one ready batch, or
   * 2. The source is exhausted, or
   * 3. We have reached a limit of maxBatchesInFormation
   *
   * In case 2 and 3 we either to wait for some in-flight batches to complete by returning null,
   * or else force select a batch that can execute but is not filled yet.
   *
   * @return the next ready batch or null
   */
  private def formBatches(): Batch = {
    var readyBatch = findNextReadyBatch(force = sourceExhausted)
    while (readyBatch == null && !sourceExhausted) {
      if (pendingRow != null || source.hasNext) {
        val targetBatch = processNextRow()
        if (targetBatch != null) {
          // The row was added to a batch, so we can check for ready batches again before pulling more rows
          readyBatch = findNextReadyBatch(force = false)
        } else {
          // We could not insert the row because we have too many batches in formation.
          // We also have no ready batch, so either wait for in-flight batches to complete
          // or force-select a batch that can execute but is not filled yet.
          if (hasInFlightBatches) {
            return null
          } else {
            // Force-select an executable batch even though it may not be filled yet
            readyBatch = findNextReadyBatch(force = true)
            if (readyBatch == null && hasBatchesInFormation) {
              // This should not happen - if we have no in-flight batches, we should be able to select a batch to execute
              throw new IllegalStateException(
                s"Deadlock prevention failed: No ready batch found after processing row and no in-flight batches, but $batchesInFormationCount batches still in formation"
              )
            }
          }
        }
      } else {
        // We have no more input rows
        sourceExhausted = true
        if (DebugSupport.DEBUG_BATCH_FORMATION) {
          DebugSupport.BATCH_FORMATION.log("%s: source exhausted!", this)
        }
        // Force-select an executable batch even though it may not be filled yet
        readyBatch = findNextReadyBatch(force = true)
      }
    }
    readyBatch
  }

  // TODO: Should we have a different method/criterion for force, where we also consider fill size and dependencies of the batch?
  private def findNextReadyBatch(force: Boolean): Batch = {
    val batchIdx = scheduler.pollReadyBatch(force)
    if (batchIdx >= 0) {
      val b = batchesInFormation(batchIdx)
      b
    } else {
      null
    }
  }

  /**
   * Dispatch a batch
   */
  private def dispatchBatch(batch: Batch): Batch = {
    scheduler.dispatchBatch(batch.index)
    batchesInFormation(batch.index) = null
    batchesInFormationCount -= 1
    batchesInFlightCount += 1
    batch.dispatch()
  }

  /**
   * Complete a batch and update dependencies.
   */
  private def completeBatch(batch: Batch): Unit = {
    batch.setState(BatchState.Completed)
    batchesInFlightCount -= 1
    // Close the batch metadata (but not the rows - they were already consumed)
    batch.close()
    scheduler.completeBatch(batch.index)
  }

  /**
   * Process the next row from the source and add it to an appropriate batch.
   */
  private def processNextRow(): Batch = {
    val row =
      if (pendingRow != null) {
        val _row = pendingRow
        pendingRow = null
        _row
      } else {
        source.next()
      }

    // Extract RAID values from the row
    val rowRaids = new Array[AnyValue](raidExpressions.length)
    var i = 0
    while (i < raidExpressions.length) {
      rowRaids(i) = raidExpressions(i).apply(row, state)
      i += 1
    }

    // Find the best batch for this row
    val targetBatch = findOrCreateBatch(rowRaids)
    if (targetBatch != null) {
      // Add the row to the batch
      targetBatch.addRow(row)
    } else {
      pendingRow = row
    }
    targetBatch
  }

  /**
   * Find the best existing batch for a row, or create a new one.
   */
  private def findOrCreateBatch(rowRaids: Array[AnyValue]): Batch = {
    val batchIdx = scheduler.insertRow(rowRaids)
    if (batchIdx >= 0) {
      var batch = batchesInFormation(batchIdx)
      if (batch == null) {
        val rows = createEagerBuffer[CypherRow](memoryTracker, Math.min(batchSize, 1024))
        batch = new Batch(batchCounter, batchIdx, rows, BatchState.InFormation)
        batchCounter += 1
        batchesInFormation(batchIdx) = batch
        batchesInFormationCount += 1
      }
      batch
    } else {
      null
    }
  }

  override def toString: String = {
    val inFlightStr = if (hasInFlightBatches) " (has in-flight batches)" else ""
    val sourceExhaustedStr = if (sourceExhausted) " (source exhausted)" else ""
    s"BatchIterator[${System.identityHashCode(this).toHexString}](#batches_in_formation: $batchesInFormationCount, #total_batches: $batchCounter$inFlightStr$sourceExhaustedStr)"
  }
}

/**
 * @param batchSize The maximum number of rows per batch
 * @param raidsPerRow The number of RAID expressions per row
 * @param maxBatches The maximum number of batches that can be in formation at once (must be multiple of 64)
 *                   Target: the size of the input queue times 2, rounded up to the next multiple of 64, for saturation.
 */
class BitmaskBatchScheduler(batchSize: Int, raidsPerRow: Int, maxBatches: Int = 256, memoryTracker: MemoryTracker)
    extends AutoCloseable {

  require(maxBatches % 64 == 0, "maxBatches must be a multiple of 64")
  require(maxBatches <= 65535, s"maxBatches must be <= 65535 (16-bit encoding limit), got $maxBatches")

  // --- CONFIGURATION ---
  // E.g. Using maxBatches=256 allows us to use exactly 4 Longs (64 bits * 4) per row.
  // This fits well in cache lines.
  final val ROW_WORDS = maxBatches >> 6 // equivalent to / 64

  // --- STATE: DEPENDENCY MATRICES ---
  // Cache-friendly when maxBatches is small (e.g. 256 = 4 Longs per row, fits in a cache line).
  // Performance degrades for larger maxBatches (e.g. from ~1024-2048 batches, dependency arrays
  // span multiple cache lines and L1/L2 pressure increases).
  // forwardDeps(batchIdx)(wordIdx) -> bits representing parents
  private[this] val forwardDeps = Array.ofDim[Long](maxBatches, ROW_WORDS)

  // reverseDeps(batchIdx)(wordIdx) -> bits representing children
  // Used to quickly notify children when a batch completes
  private[this] val reverseDeps = Array.ofDim[Long](maxBatches, ROW_WORDS)

  // --- STATE: MASKS ---
  // 1 = Batch is Allocated but not yet Completed
  private[this] val activeBatchesMask = new Array[Long](ROW_WORDS)
  // 1 = Batch is Allocated and Open for insertion
  private[this] val openBatchesMask = new Array[Long](ROW_WORDS)
  // 1 = Batch is Allocated and Ready (no dependencies)
  private[this] val readyBatchesMask = new Array[Long](ROW_WORDS)
  // 1 = Batch is Allocated, Full, and Ready (no dependencies)
  private[this] val filledReadyBatchesMask = new Array[Long](ROW_WORDS)

  // --- STATE: BATCH METADATA ---
  private[this] val batchSequence = new Array[Long](maxBatches) // Logical timestamp to prevent cycles
  private[this] val batchFillCount = new Array[Int](maxBatches)

  // --- STATE: RAID TRACKING (Last Writer Wins) ---
  // Maps RAID -> encoded (batchSequence, batchIndex) that last touched it.
  // Encoding: (sequence << 16) | (index + 1), where index + 1 avoids 0 (the default empty value)
  private[this] val raidLastWriter =
    HeapTrackingObjectLongHashMap.createObjectLongHashMap[AnyValue](memoryTracker, Math.max(batchSize, maxBatches))

  // --- INTERNAL COUNTERS ---
  private[this] var globalSequenceCounter: Long = 0L
  private[this] var nextAllocationIndex: Int = 0

  // --- PURGE THRESHOLD ---
  private[this] val raidPurgeThreshold: Long = 2L * maxBatches * batchSize * raidsPerRow
  // An array to temporarily hold RAID keys to be removed during a purge. Reused between purges.
  private[this] var raidKeysToRemove: java.util.ArrayList[AnyValue] = _

  private[this] val reqDeps = new Array[Long](ROW_WORDS)

  /**
   * PHASE A: Insert Row
   * This is the tightest loop. It finds dependencies, selects a batch, and updates state.
   * @return The index of the batch selected for insertion, or -1 if it was rejected due to too many batches in formation (backpressure)
   */
  def insertRow(rowRaids: Array[AnyValue]): Int = {
    // 1. Calculate Dependencies for this new row
    var maxDependencySeq: Long = -1L

    // NOTE: We reuse the field reqDeps, so this is not thread-safe
    var i = 0
    while (i < rowRaids.length) {
      val raid = rowRaids(i)
      // NOTE: Empty value is 0 in ObjectLongHashMap, so we offset by -1 to use 0-based indexing
      val encoded = raidLastWriter.get(raid)
      val writerIdx = (encoded & 0xffffL).toInt - 1
      val writerStoredSeq = encoded >>> 16

      if (
        writerIdx != -1 &&
        (activeBatchesMask(writerIdx >> 6) & (1L << writerIdx)) != 0 &&
        batchSequence(writerIdx) == writerStoredSeq
      ) {
        val writerSeq = batchSequence(writerIdx)
        if (writerSeq > maxDependencySeq) {
          maxDependencySeq = writerSeq
        }

        // Set bit in temp mask
        val wordIndex = writerIdx >> 6 // equivalent to / 64
        reqDeps(wordIndex) |= (1L << writerIdx) // implicit modulo 64
      }
      i += 1
    }

    // 2. Find Candidate Batch
    // Strategy: Prefer the oldest OPEN batch where Batch.Seq > MaxDep.Seq
    var selectedBatch = -1

    var w = 0
    while (w < ROW_WORDS && selectedBatch == -1) {
      var word = openBatchesMask(w)
      while (word != 0) {
        val bit = numberOfTrailingZeros(word)
        val idx = (w << 6) + bit

        // Validation: Must be at least as new as the dependencies to avoid cycles.
        // Equal is allowed because it means the dependency is the candidate itself (self-dep removed in step 4).
        if (batchSequence(idx) >= maxDependencySeq) {
          if (batchFillCount(idx) < batchSize) {
            selectedBatch = idx
            // break inner loop
            word = 0
          }
        }

        // clear bit and continue if not selected
        if (selectedBatch == -1) {
          word &= ~(1L << bit)
        }
      }
      w += 1
    }

    // 3. Allocate New if needed
    if (selectedBatch == -1) {
      selectedBatch = allocateNewBatch()
      if (selectedBatch == -1) {
        // All batches are active - backpressure needed
        // Clear reqDeps before returning
        var cw = 0
        while (cw < ROW_WORDS) {
          reqDeps(cw) = 0
          cw += 1
        }
        return -1
      }
      // NOTE: The new batch always has higher sequence, so it satisfies maxDependencySeq
    }

    // 4. Update Graph State
    // Remove self-dependency (a batch should not depend on itself)
    val selfWordIndex = selectedBatch >> 6
    reqDeps(selfWordIndex) &= ~(1L << selectedBatch)

    val wasReady = isReady(selectedBatch)
    var addedDep = false

    w = 0
    while (w < ROW_WORDS) {
      if (reqDeps(w) != 0) {
        val oldDeps = forwardDeps(selectedBatch)(w)
        val newDeps = oldDeps | reqDeps(w)

        if (oldDeps != newDeps) {
          forwardDeps(selectedBatch)(w) = newDeps
          addedDep = true

          // Update Reverse Index: Who depends on the parent?
          // We need to mark 'selectedBatch' as a child of every parent in reqDeps
          var diff = newDeps ^ oldDeps // bits that changed from 0 to 1
          while (diff != 0) {
            val parentBit = numberOfTrailingZeros(diff)
            val parentIdx = (w << 6) + parentBit

            // Mark selectedBatch in the parent's reverse dependency row
            reverseDeps(parentIdx)(selectedBatch >> 6) |= (1L << selectedBatch)

            diff &= (diff - 1)
          }
        }
      }
      // Clear reqDeps before next call
      reqDeps(w) = 0
      w += 1
    }

    // If it was ready, but we added a dependency, remove from ready mask
    if (wasReady && addedDep) {
      val wordIndex = selectedBatch >> 6
      readyBatchesMask(wordIndex) &= ~(1L << selectedBatch)
    }

    // 5. Update ID Tracking
    i = 0
    while (i < rowRaids.length) {
      val raid = rowRaids(i)
      // NOTE: Empty value is 0 in ObjectLongHashMap, so we offset the batch index inside the map by +1
      // Encode (sequence << 16) | (index + 1)
      raidLastWriter.put(raid, (batchSequence(selectedBatch) << 16) | (selectedBatch + 1).toLong)
      i += 1
    }

    val fillCount = batchFillCount(selectedBatch)
    val newFillCount = fillCount + 1
    batchFillCount(selectedBatch) = newFillCount
    if (newFillCount >= batchSize) {
      val wordIndex = selectedBatch >> 6
      // Batch is full, clear open bit
      openBatchesMask(wordIndex) &= ~(1L << selectedBatch) // Clear open bit
      if (wasReady && !addedDep) {
        filledReadyBatchesMask(wordIndex) |= (1L << selectedBatch)
      }
    }
    selectedBatch
  }

  /**
   * PHASE B: Select Batch for Execution
   * Returns batch index or -1 if none.
   */
  def pollReadyBatch(force: Boolean): Int = {
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
    if (force) {
      // Select a ready batch even if it's not full
      w = 0
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
   * PHASE C: Dispatch Batch
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
   * PHASE D: Complete Batch
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

  // --- HELPERS ---

  // Returns the index of the newly allocated batch, or -1 if all batches are active (saturation)
  private def allocateNewBatch(): Int = {
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
  @inline private def isReady(batchIdx: Int): Boolean = {
    val wIdx = batchIdx >> 6
    (readyBatchesMask(wIdx) & (1L << batchIdx)) != 0
  }

  @VisibleForTesting
  private[pipes] def raidMapSize: Long = raidLastWriter.size()
}
