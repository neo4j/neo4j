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
