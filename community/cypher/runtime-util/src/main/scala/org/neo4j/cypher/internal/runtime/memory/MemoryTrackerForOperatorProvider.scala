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
package org.neo4j.cypher.internal.runtime.memory

import org.neo4j.cypher.internal.runtime.memory.TransactionBoundMemoryTrackerForOperatorProvider.TransactionBoundMemoryTracker
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.memory.DefaultScopedMemoryTracker
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.HeapEstimatorCache
import org.neo4j.memory.HeapEstimatorCacheConfig
import org.neo4j.memory.HeapHighWaterMarkTracker
import org.neo4j.memory.HeapMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.memory.RebindableDualScopedMemoryTracker

/**
 * Gives the ability to track memory per operator.
 */
trait MemoryTrackerForOperatorProvider {

  /**
   * Get the memory tracker for the operator with the given id.
   *
   * @param operatorId the id of the operator
   * @param enableScopedHeapEstimatorCache whether to enable a scoped heap estimator cache for this operator for
   *                                       collections that can use it, e.g. HeapTrackingListValueBuilder
   */
  def memoryTrackerForOperator(operatorId: Int, enableScopedHeapEstimatorCache: Boolean = false): MemoryTracker

  /**
   * This is called from generated code (and from Java in tests)
   */
  def memoryTrackerForOperator(operatorId: Int): MemoryTracker =
    memoryTrackerForOperator(operatorId, enableScopedHeapEstimatorCache = false)

  def setInitializationMemoryTracker(memoryTracker: MemoryTracker): Unit = {
    throw new UnsupportedOperationException(
      s"${getClass.getSimpleName} does not support setting an initialization memory tracker"
    )
  }

  def initializationMemoryTracker: MemoryTracker = {
    throw new UnsupportedOperationException(
      s"${getClass.getSimpleName} does not support getting an initialization memory tracker"
    )
  }

  def rebind(newTransactionMemoryTracker: MemoryTracker): Unit = {
    throw new UnsupportedOperationException(
      s"${getClass.getSimpleName} does not support rebinding a transaction memory tracker"
    )
  }

  def closeInner(): Unit = {
    throw new UnsupportedOperationException(
      s"${getClass.getSimpleName} does not support closing the inner transaction memory tracker"
    )
  }
}

object MemoryTrackerForOperatorProvider {

  /**
   * Convert a value returned from [[TrackingQueryMemoryTracker.heapHighWaterMark]]
   * or [[TrackingQueryMemoryTracker.heapHighWaterMarkOfOperator]] to a value to be given to a [[QueryProfile]].
   */
  def memoryAsProfileData(value: Long): Long = value match {
    case HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED => OperatorProfile.NO_DATA
    case x                                                => x
  }
}

/**
 * Doesn't actually track anything.
 */
case object NoOpMemoryTrackerForOperatorProvider extends MemoryTrackerForOperatorProvider {

  override def memoryTrackerForOperator(operatorId: Int, enableScopedHeapEstimatorCache: Boolean): MemoryTracker =
    EmptyMemoryTracker.INSTANCE
  override def rebind(newTransactionMemoryTracker: MemoryTracker): Unit = {}
  override def closeInner(): Unit = {}
}

object TransactionBoundMemoryTrackerForOperatorProvider {

  /**
   * Forward heap allocations and de-allocations to both a transaction memory tracker
   * and a tracker for this scope spanning multiple transactions in the same query.
   *
   * This tracker can be used both for the whole query and a single operator in a query,
   * given the right arguments.
   */
  class TransactionBoundMemoryTracker(
    transactionMemoryTracker: MemoryTracker,
    queryGlobalMemoryTracker: HeapMemoryTracker,
    heapEstimatorCacheConfig: HeapEstimatorCacheConfig,
    enableScopedHeapEstimatorCache: Boolean
  ) extends DefaultScopedMemoryTracker(
        transactionMemoryTracker,
        heapEstimatorCacheConfig.newDefaultHeapEstimatorCache()
      ) {

    override def allocateHeap(bytes: Long): Unit = {
      // Forward to transaction memory tracker
      super.allocateHeap(bytes)
      // Forward to the queryGlobalMemoryTracker
      queryGlobalMemoryTracker.allocateHeap(bytes)
    }

    override def releaseHeap(bytes: Long): Unit = {
      // Forward to transaction memory tracker
      super.releaseHeap(bytes)
      // Forward to the queryGlobalMemoryTracker
      queryGlobalMemoryTracker.releaseHeap(bytes)
    }

    override def getScopedHeapEstimatorCache: HeapEstimatorCache = {
      if (enableScopedHeapEstimatorCache) {
        super.getHeapEstimatorCache.newWithSameSettings()
      } else {
        super.getHeapEstimatorCache
      }
    }
  }
}

/**
 * Tracks memory per operator.
 * Also forwards all allocations to a transactionMemoryTracker.
 * Also forwards all allocations to a queryHeapHighWatermarkTracker, which tracks the heap high water mark of a query,
 * over potentially multiple transactions.
 *
 */
class TransactionBoundMemoryTrackerForOperatorProvider(
  val transactionMemoryTracker: MemoryTracker,
  queryHeapHighWatermarkTracker: TrackingQueryMemoryTracker,
  heapEstimatorCacheConfig: HeapEstimatorCacheConfig
) extends TransactionBoundMemoryTracker(
      transactionMemoryTracker,
      queryHeapHighWatermarkTracker,
      heapEstimatorCacheConfig,
      enableScopedHeapEstimatorCache = false
    )
    with MemoryTrackerForOperatorProvider {

  override def memoryTrackerForOperator(operatorId: Int, enableScopedHeapEstimatorCache: Boolean): MemoryTracker = {
    // NOTE: This will create a heap estimator cache instance per operator regardless of the value of enableScopedHeapEstimatorCache
    new TransactionBoundMemoryTracker(
      transactionMemoryTracker,
      queryHeapHighWatermarkTracker.memoryTrackerForOperator(operatorId),
      heapEstimatorCacheConfig,
      enableScopedHeapEstimatorCache
    )
  }
}

/**
 * Tracks memory of operators inside a transactional scope of one lane in the parallel runtime,
 * where each lane opens and closes inner transactions while the query runs
 * (CALL IN TRANSACTIONS executed by the parallel runtime).
 *
 * All operator trackers of the scope share one [[RebindableDualScopedMemoryTracker]]:
 *
 *  - While an inner transaction is bound (between [[rebind]] and [[closeInner]]), allocations and
 *    releases are routed to that inner transaction's memory tracker, so the memory is attributed to
 *    the inner transaction and counted against its memory limit.
 *  - When no inner transaction is bound (query build, between batches, query cleanup), calls fall
 *    through to the outer [[WorkerThreadDelegatingMemoryTracker]], which delegates to the calling
 *    worker thread's execution context memory tracker, or to the initialization memory tracker on
 *    non-worker threads. That attributes the memory to the outer transaction.
 *
 * Thread model: an instance belongs to one lane and one transactional scope. All calls while the
 * query runs happen under the lane lock, and query build and cleanup run single threaded before
 * workers start and after all workers have finished. The rebindable tracker is not thread safe and
 * relies on this.
 */
class ParallelTransactionBoundMemoryTrackerForOperatorProvider(
  heapEstimatorCacheConfig: HeapEstimatorCacheConfig
) extends MemoryTrackerForOperatorProvider {

  private[this] val outerDelegating: WorkerThreadDelegatingMemoryTracker =
    new WorkerThreadDelegatingMemoryTracker

  private[this] val rebindable: RebindableDualScopedMemoryTracker =
    new RebindableDualScopedMemoryTracker(outerDelegating)

  override def memoryTrackerForOperator(operatorId: Int, enableScopedHeapEstimatorCache: Boolean): MemoryTracker = {
    // Per-operator scoped layer over the shared rebindable tracker, mirroring the layering of
    // TransactionBoundMemoryTrackerForOperatorProvider on the serial path. It carries the operator's
    // heap estimator cache. A lane is single threaded, so the cache needs no further isolation even
    // when a scoped heap estimator cache is requested.
    new DefaultScopedMemoryTracker(rebindable, heapEstimatorCacheConfig.newDefaultHeapEstimatorCache())
  }

  override def setInitializationMemoryTracker(memoryTracker: MemoryTracker): Unit =
    outerDelegating.setInitializationMemoryTracker(memoryTracker)

  override def initializationMemoryTracker: MemoryTracker =
    outerDelegating.initializationMemoryTracker

  override def rebind(newTransactionMemoryTracker: MemoryTracker): Unit =
    rebindable.setInnerDelegate(newTransactionMemoryTracker)

  override def closeInner(): Unit =
    rebindable.closeInner()
}
