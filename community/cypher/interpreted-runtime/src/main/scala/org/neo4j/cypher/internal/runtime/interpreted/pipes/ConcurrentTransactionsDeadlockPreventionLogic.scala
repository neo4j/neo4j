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

import org.neo4j.cypher.internal.macros.AssertMacros3.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.TransactionBatchSchedulerConfig
import org.neo4j.cypher.internal.runtime.debug.events.Debug
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.debug.events.BatchFormation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ConcurrentTransactionsDeadlockPreventionLogic.BatchState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DependencyTrackingTransactionBatch.WAIT_MARKER_BATCH
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionRetryLogic.RetryState
import org.neo4j.kernel.impl.util.collection.EagerBuffer
import org.neo4j.kernel.impl.util.collection.EagerBuffer.createEagerBuffer
import org.neo4j.memory.DefaultScopedMemoryTracker
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker
import org.neo4j.memory.ScopedMemoryTracker
import org.neo4j.values.AnyValue

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
 * - Evaluates the RAID expressions for each row; all expressions are treated symmetrically
 *   (the order of the expressions has no effect on batch formation or scheduling)
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
 * @param disjointBy Array of RAID expressions
 * @param maxBatchesInFormation The maximum number of batches that can be in formation at once
 * @param schedulerConfig Selects the batch formation and scheduling policy (greedy, dependency-set
 *                        affinity, or reservation-based parking)
 * @param parkingBudget The maximum number of rows the parking scheduler may hold outside batches
 *                      (0 derives a default from the formation window; ignored by the other
 *                      schedulers)
 */
class ConcurrentTransactionsDeadlockPreventionLogic(
  disjointBy: Array[Expression],
  maxBatchesInFormation: Int = 256,
  schedulerConfig: TransactionBatchSchedulerConfig = TransactionBatchSchedulerConfig.DEFAULT,
  parkingBudget: Int = 0
) extends TransactionDeadlockPreventionLogic {
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
      maxBatchesInFormation,
      schedulerConfig,
      parkingBudget
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
 * Outcomes of processing one input row against the scheduler.
 */
private[pipes] object RowStatus {
  final val INSERTED = 0
  final val PARKED = 1
  final val BACKPRESSURE = 2
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
    Debug.log(BatchFormation.StateChanged(newState, this.toString))
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
 * Thread safety: This iterator is driven entirely from the main thread, both for iteration and for
 * batch completion notifications. Worker threads only publish results to the output queue, which the
 * main thread drains before calling onBatchCompleted, so the iterator's state needs no synchronization.
 */
private[pipes] class DeadlockPreventingBatchIterator(
  source: ClosingIterator[CypherRow],
  state: QueryState,
  batchSize: Int,
  memoryTracker: MemoryTracker,
  raidExpressions: Array[Expression],
  maxBatchesInFormation: Int,
  schedulerConfig: TransactionBatchSchedulerConfig = TransactionBatchSchedulerConfig.DEFAULT,
  parkingBudget: Int = 0
) extends ClosingIterator[TransactionBatch] {

  type Batch = DependencyTrackingTransactionBatch

  private[this] val scheduler: BatchScheduler = schedulerConfig match {
    case TransactionBatchSchedulerConfig.Affinity =>
      new AffinityBitmaskBatchScheduler(batchSize, raidExpressions.length, maxBatchesInFormation, memoryTracker)
    case TransactionBatchSchedulerConfig.Greedy =>
      new BitmaskBatchScheduler(batchSize, raidExpressions.length, maxBatchesInFormation, memoryTracker)
    case TransactionBatchSchedulerConfig.Parking =>
      new ParkingBitmaskBatchScheduler(
        batchSize,
        raidExpressions.length,
        maxBatchesInFormation,
        ParkingBitmaskBatchScheduler.effectiveParkingBudget(parkingBudget, maxBatchesInFormation, batchSize),
        memoryTracker
      )
  }
  private[this] var batchCounter: Long = _

  // All batches in formation
  private val batchesInFormation: Array[Batch] = new Array[Batch](maxBatchesInFormation)
  private[this] var batchesInFormationCount: Int = 0
  private[this] var batchesInFlightCount: Int = 0

  private[this] var pendingRow: CypherRow = _

  // Scratch buffer for the current row's RAID values, reused across rows to avoid a per-row array
  // allocation. Schedulers must copy it if they retain RAIDs (parking does). When a row is held back
  // as pendingRow, its values stay valid in the scratch so the retry never re-evaluates the RAID
  // expressions.
  private[this] val rowRaidsScratch = new Array[AnyValue](raidExpressions.length)
  private[this] var pendingRowRaidsValid: Boolean = false

  // True while the scheduler is backpressured and no batch has completed since the last failed
  // insert attempt. Nothing can change until a completion arrives, so batch formation skips the
  // redundant re-attempts (and their failing full scans) that each produce cycle would otherwise
  // trigger.
  private[this] var waitingForCompletion: Boolean = false

  // Rows parked by the scheduler live here, indexed by the park slot the scheduler assigned.
  // The scheduler owns the admission queues, the iterator owns the row objects and their heap.
  private[this] var parkedRows: Array[CypherRow] = _
  private[this] var parkedRowCount: Int = 0
  private[this] var parkedRowsMemoryTracker: ScopedMemoryTracker = _

  private[this] val parkedRowSink: ParkedRowSink = new ParkedRowSink {
    override def place(parkedSlot: Int, batchIdx: Int): Unit = {
      val row = parkedRows(parkedSlot)
      parkedRows(parkedSlot) = null
      parkedRowCount -= 1
      parkedRowsMemoryTracker.releaseHeap(row.estimatedHeapUsage)
      val batch = getOrCreateBatch(batchIdx)
      batch.addRow(row)
    }
  }

  // Flag to indicate if source is exhausted
  private[this] var sourceExhausted: Boolean = false
  private[this] var nextReadyBatch: Batch = _

  override protected[this] def innerHasNext: Boolean = {
    if (nextReadyBatch == null) {
      nextReadyBatch = formBatches()
      if (nextReadyBatch == null) {
        if (hasInFlightBatches && (hasBatchesInFormation || hasParkedRows || !sourceExhausted)) {
          // If we have in-flight batches and no ready batch, we need to wait for some to complete to
          // free up dependencies, free up batch slots, or release parked rows.
          // We signal this by returning WAIT_MARKER_BATCH
          nextReadyBatch = WAIT_MARKER_BATCH
          Debug.log(BatchFormation.WaitingForInFlight(this.toString))
        } else if (hasBatchesInFormation || hasParkedRows) {
          throw new IllegalStateException(
            s"No ready batches and no in-flight batches, but $batchesInFormationCount batches still in formation" +
              s" and $parkedRowCount rows parked"
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
    pendingRowRaidsValid = false
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
    // Release any rows still parked (their heap is tracked by the scoped tracker)
    if (parkedRows != null) {
      java.util.Arrays.fill(parkedRows.asInstanceOf[Array[AnyRef]], null)
      parkedRowCount = 0
      parkedRowsMemoryTracker.close()
      parkedRowsMemoryTracker = null
      parkedRows = null
    }
    scheduler.close()
  }

  @inline private def hasInFlightBatches: Boolean = {
    batchesInFlightCount > 0
  }

  @inline private def hasBatchesInFormation: Boolean = {
    batchesInFormationCount > 0
  }

  @inline private def hasParkedRows: Boolean = {
    parkedRowCount > 0
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
    if (waitingForCompletion) {
      // No completion since the last failed attempt, so the scheduler cannot have gained capacity.
      // Skip the redundant re-attempt.
      return null
    }
    val readyBatch = doFormBatches()
    if (readyBatch == null && hasInFlightBatches) {
      waitingForCompletion = true
    }
    readyBatch
  }

  private def doFormBatches(): Batch = {
    if (sourceExhausted && hasParkedRows) {
      // Resume the drain of parked rows: the drain at source exhaustion (or a liveness fallback)
      // may have stopped early on batch-slot exhaustion, so completions must let it continue.
      scheduler.admitParked(force = true, parkedRowSink)
    }
    var readyBatch = findNextReadyBatch(force = sourceExhausted)
    while (readyBatch == null && !sourceExhausted) {
      if (pendingRow != null || source.hasNext) {
        val status = processNextRow()
        if (status == RowStatus.INSERTED) {
          // The row was added to a batch, so we can check for ready batches again before pulling more rows
          readyBatch = findNextReadyBatch(force = false)
        } else if (status == RowStatus.PARKED) {
          // The scheduler absorbed the row into its parking queues; keep pulling input
        } else {
          // We could not insert the row because we have too many batches in formation or the
          // parking budget is full. We also have no ready batch, so either wait for in-flight
          // batches to complete or force-select a batch that can execute but is not filled yet.
          if (hasInFlightBatches) {
            return null
          } else {
            // Liveness fallback: nothing is in flight, so no completion will release parked rows
            // or batch slots. Place parked rows using dependency edges to make progress.
            if (hasParkedRows) {
              scheduler.admitParked(force = true, parkedRowSink)
            }
            // Force-select an executable batch even though it may not be filled yet
            readyBatch = findNextReadyBatch(force = true)
            if (readyBatch == null && (hasBatchesInFormation || hasParkedRows)) {
              // This should not happen - if we have no in-flight batches, we should be able to select a batch to execute
              throw new IllegalStateException(
                s"Deadlock prevention failed: No ready batch found after processing row and no in-flight batches, " +
                  s"but $batchesInFormationCount batches still in formation and $parkedRowCount rows parked"
              )
            }
          }
        }
      } else {
        // We have no more input rows
        sourceExhausted = true
        Debug.log(BatchFormation.SourceExhausted(this.toString))
        // Drain parked rows into batches using dependency edges (may stop early on slot
        // exhaustion, in which case the resume at the top of this method continues it)
        if (hasParkedRows) {
          scheduler.admitParked(force = true, parkedRowSink)
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
    waitingForCompletion = false
    batch.setState(BatchState.Completed)
    batchesInFlightCount -= 1
    // Close the batch metadata (but not the rows - they were already consumed)
    batch.close()
    scheduler.completeBatch(batch.index)
    if (hasParkedRows) {
      // The completion released this batch's RAID reservations, so parked rows waiting on them
      // may now be admissible without adding dependency edges
      scheduler.admitParked(force = false, parkedRowSink)
    }
  }

  /**
   * Process the next row from the source and hand it to the scheduler: inserted into a batch,
   * parked by the scheduler, or rejected (backpressure, held as pendingRow).
   */
  private def processNextRow(): Int = {
    val row =
      if (pendingRow != null) {
        val _row = pendingRow
        pendingRow = null
        _row
      } else {
        source.next()
      }

    // Extract RAID values from the row into the reused scratch. A backpressured pendingRow's
    // values are still valid in the scratch, so the retry skips the expression re-evaluation.
    val rowRaids = rowRaidsScratch
    if (!pendingRowRaidsValid) {
      var i = 0
      while (i < raidExpressions.length) {
        rowRaids(i) = raidExpressions(i).apply(row, state)
        i += 1
      }
    }
    pendingRowRaidsValid = false

    // Find the best batch for this row
    val result = scheduler.insertRow(rowRaids)
    if (result >= 0) {
      val batch = getOrCreateBatch(result)
      batch.addRow(row)
      RowStatus.INSERTED
    } else if (result <= BatchScheduler.PARKED_BASE) {
      parkRow(BatchScheduler.parkedSlot(result), row)
      RowStatus.PARKED
    } else {
      pendingRow = row
      pendingRowRaidsValid = true
      RowStatus.BACKPRESSURE
    }
  }

  private def parkRow(slot: Int, row: CypherRow): Unit = {
    if (parkedRows == null) {
      parkedRows = new Array[CypherRow](scheduler.parkingCapacity)
      parkedRowsMemoryTracker = new DefaultScopedMemoryTracker(memoryTracker)
      parkedRowsMemoryTracker.allocateHeap(HeapEstimator.shallowSizeOfObjectArray(parkedRows.length))
    }
    parkedRows(slot) = row
    parkedRowCount += 1
    parkedRowsMemoryTracker.allocateHeap(row.estimatedHeapUsage)
  }

  /**
   * Look up the batch at the slot the scheduler selected, creating it on first use.
   */
  private def getOrCreateBatch(batchIdx: Int): Batch = {
    var batch = batchesInFormation(batchIdx)
    if (batch == null) {
      val rows = createEagerBuffer[CypherRow](memoryTracker, Math.min(batchSize, 1024))
      batch = new Batch(batchCounter, batchIdx, rows, BatchState.InFormation)
      batchCounter += 1
      batchesInFormation(batchIdx) = batch
      batchesInFormationCount += 1
    }
    batch
  }

  override def toString: String = {
    val inFlightStr = if (hasInFlightBatches) " (has in-flight batches)" else ""
    val sourceExhaustedStr = if (sourceExhausted) " (source exhausted)" else ""
    s"BatchIterator[${System.identityHashCode(this).toHexString}](#batches_in_formation: $batchesInFormationCount, #total_batches: $batchCounter$inFlightStr$sourceExhaustedStr)"
  }
}

/**
 * Scheduler that groups rows into batches based on their RAID values and tracks dependencies
 * between batches, so that batches sharing RAID values are never executed concurrently.
 *
 * Lifecycle per batch: insertRow (forms batches) -> pollReadyBatch -> dispatchBatch -> completeBatch.
 * Implementations are not thread-safe and are driven from a single thread.
 */
trait BatchScheduler extends AutoCloseable {

  /**
   * Find a batch for a row with the given RAID values and record its dependencies.
   *
   * The caller may reuse the rowRaids array between calls, so a scheduler that needs to retain
   * RAID values beyond this call (parking) must copy them.
   *
   * @return one of three outcomes:
   *         - index >= 0: the batch slot the row was inserted into. The caller must append the
   *           physical row to that batch.
   *         - [[BatchScheduler.BACKPRESSURE]] (-1): the row was rejected (all batch slots active,
   *           or the parking budget is full). The caller must hold the row and retry after
   *           completions.
   *         - <= [[BatchScheduler.PARKED_BASE]] (-2): the row was admitted into the scheduler's
   *           parking queues. The caller must store the row at [[BatchScheduler.parkedSlot]] of
   *           the result and keep it until [[ParkedRowSink.place]] is called for that slot, or
   *           the iterator closes. Only schedulers with parkingCapacity > 0 return this outcome.
   */
  def insertRow(rowRaids: Array[AnyValue]): Int

  /**
   * Maximum number of rows this scheduler may park at any one time (0 for non-parking schedulers).
   * The caller sizes its parked-row storage from this.
   */
  def parkingCapacity: Int = 0

  /**
   * Whether any rows are currently parked in this scheduler.
   */
  def hasParkedRows: Boolean = false

  /**
   * Move admissible parked rows into batches, calling sink.place(slot, batchIdx) once per row.
   * With force=false, only rows that can be placed without adding dependency edges are admitted
   * (called after completeBatch). With force=true, remaining parked rows are placed using
   * dependency edges, like the non-parking schedulers would have placed them (called on drain
   * when the input is exhausted, and as the liveness fallback when nothing else can progress).
   * May stop early when batch slots are exhausted. Safe to call again after completions.
   */
  def admitParked(force: Boolean, sink: ParkedRowSink): Unit = ()

  /**
   * Select a batch for execution.
   * @param force also consider ready batches that are not full yet
   * @return batch index or -1 if none
   */
  def pollReadyBatch(force: Boolean): Int

  /**
   * Triggered when a ready batch is selected for execution.
   */
  def dispatchBatch(dispatchedBatchIdx: Int): Unit

  /**
   * Triggered when a batch finishes execution.
   */
  def completeBatch(completedBatchIdx: Int): Unit

  override def close(): Unit
}

object BatchScheduler {

  /**
   * insertRow result: the row was rejected and the caller must hold it and retry after
   * completions.
   */
  final val BACKPRESSURE: Int = -1

  /**
   * insertRow results at or below this value encode a parking slot: slot = PARKED_BASE - result.
   */
  final val PARKED_BASE: Int = -2

  @inline def encodeParked(slot: Int): Int = PARKED_BASE - slot

  @inline def parkedSlot(result: Int): Int = PARKED_BASE - result
}

/**
 * Callback used by [[BatchScheduler.admitParked]] to move a parked row into a batch.
 * Implementations append the row stored at parkedSlot to the batch at batchIdx and free the
 * slot for reuse.
 */
trait ParkedRowSink {
  def place(parkedSlot: Int, batchIdx: Int): Unit
}

/**
 * @param batchSize The maximum number of rows per batch
 * @param raidsPerRow The number of RAID expressions per row
 * @param maxBatches The maximum number of batches that can be in formation at once (must be multiple of 64)
 *                   Target: the size of the input queue times 2, rounded up to the next multiple of 64, for saturation.
 */
class BitmaskBatchScheduler(batchSize: Int, raidsPerRow: Int, maxBatches: Int = 256, memoryTracker: MemoryTracker)
    extends AbstractBitmaskBatchScheduler(batchSize, raidsPerRow, maxBatches, memoryTracker) {

  /**
   * Insert a row: find dependencies, select the first open batch the cycle-prevention rule permits,
   * and update state.
   * @return The index of the batch selected for insertion, or -1 if it was rejected due to too many batches in formation (backpressure)
   */
  def insertRow(rowRaids: Array[AnyValue]): Int = {
    resolveDependencies(rowRaids)

    // Prefer the oldest open batch with room whose sequence is at least maxDependencySeq. Equal is
    // allowed because it means the dependency is the candidate itself (removed as a self-dependency).
    var selectedBatch = firstOpenWithRoomAndSeq(maxDependencySeq)
    if (selectedBatch == -1) {
      // A new batch always has a higher sequence, so it satisfies maxDependencySeq
      selectedBatch = allocateNewBatch()
      if (selectedBatch == -1) {
        clearReqDeps()
        return -1
      }
    }

    val wasReady = isReady(selectedBatch)
    val addedDep = linkDependencies(selectedBatch, wasReady)
    recordLastWriter(rowRaids, selectedBatch)
    bumpFillCount(selectedBatch, addedDep, wasReady)
    selectedBatch
  }
}
