/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.runtime.memory

import org.neo4j.cypher.internal.config.CUSTOM_MEMORY_TRACKING
import org.neo4j.cypher.internal.config.MEMORY_TRACKING
import org.neo4j.cypher.internal.config.MemoryTracking
import org.neo4j.cypher.internal.config.NO_TRACKING
import org.neo4j.cypher.internal.runtime.GrowingArray
import org.neo4j.cypher.internal.runtime.memory.TrackingQueryMemoryTracker.MemoryTrackerPerOperator
import org.neo4j.cypher.internal.runtime.memory.TrackingQueryMemoryTracker.OperatorMemoryTracker
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.HeapHighWaterMarkTracker
import org.neo4j.memory.HeapMemoryTracker
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.MemoryTracker

/**
 * Tracks the heap high water mark for one Cypher query.
 * One Cypher query might use multiple transactions to execute,
 * which is why the memory per query needs to be tracked in addition to the memory per transaction.
 *
 * This class is used by all [[MemoryTrackerForOperatorProvider]] that are usually bound to a transaction.
 */
trait QueryMemoryTracker
    extends TransactionSpanningMemoryTrackerForOperatorProvider
    with HeapMemoryTracker {

  /**
   * Return a new [[MemoryTrackerForOperatorProvider]] that wraps the given transactionMemoryTracker,
   * but also reports to this [[QueryMemoryTracker]]. Use this method to obtain memory trackers for operators that
   * execute in a specific transaction (possibly one of multiple transactions in a query) as part of the query
   * tracked by this [[QueryMemoryTracker]].
   */
  def newMemoryTrackerForOperatorProvider(transactionMemoryTracker: MemoryTracker): MemoryTrackerForOperatorProvider
}

object QueryMemoryTracker {

  def apply(memoryTracking: MemoryTracking): QueryMemoryTracker = {
    memoryTracking match {
      case NO_TRACKING                       => NoOpQueryMemoryTracker
      case MEMORY_TRACKING                   => new TrackingQueryMemoryTracker
      case CUSTOM_MEMORY_TRACKING(decorator) => new CustomTrackingQueryMemoryTracker(decorator)
    }
  }
}

object TrackingQueryMemoryTracker {

  /**
   * Tracks memory of one operator.
   * This tracker is not bound to any transaction.
   */
  class OperatorMemoryTracker(queryMemoryTracker: TrackingQueryMemoryTracker)
      extends DelegatingScopedHeapMemoryTracker(queryMemoryTracker)

  /**
   * Basically an efficient `Map[OperatorId, OperatorMemoryTracker]`.
   *
   * @param memoryTracker a memory tracker used to track the memory of this collection.
   */
  class MemoryTrackerPerOperator(memoryTracker: HeapMemoryTracker)
      extends GrowingArray[OperatorMemoryTracker](memoryTracker)

}

/**
 * Tracks the heap high water mark for one Cypher query using a [[LocalMemoryTracker]].
 * Provides operator memory trackers that are not bound to any transaction.
 */
class TrackingQueryMemoryTracker extends QueryMemoryTracker {

  /**
   * The memory is also tracked by the transactions executing the query, so using a LocalMemoryTracker
   * that is not bounded and not connected to any memory pool is ok.
   */
  private[this] val memoryTracker = new LocalMemoryTracker()
  private[this] val memoryTrackerPerOperator: MemoryTrackerPerOperator = new MemoryTrackerPerOperator(this)
  private[this] val newTracker = () => new OperatorMemoryTracker(this)

  override def heapHighWaterMark(): Long = memoryTracker.heapHighWaterMark()

  override def allocateHeap(bytes: Long): Unit = memoryTracker.allocateHeap(bytes)

  override def releaseHeap(bytes: Long): Unit = memoryTracker.releaseHeap(bytes)

  override def newMemoryTrackerForOperatorProvider(transactionMemoryTracker: MemoryTracker)
    : MemoryTrackerForOperatorProvider =
    new TransactionBoundMemoryTrackerForOperatorProvider(transactionMemoryTracker, this)

  override def heapHighWaterMarkOfOperator(operatorId: Int): Long = {
    if (memoryTrackerPerOperator.isDefinedAt(operatorId)) {
      memoryTrackerPerOperator.get(operatorId).heapHighWaterMark
    } else {
      HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED
    }
  }

  override private[memory] def memoryTrackerForOperator(operatorId: Int): OperatorMemoryTracker = {
    memoryTrackerPerOperator.computeIfAbsent(operatorId, newTracker)
  }
}

/**
 * Applies a decorator on each transaction memory tracker that gets passed to [[newMemoryTrackerForOperatorProvider]].
 */
class CustomTrackingQueryMemoryTracker(transactionMemoryTrackerDecorator: MemoryTracker => MemoryTracker)
    extends TrackingQueryMemoryTracker {

  override def newMemoryTrackerForOperatorProvider(transactionMemoryTracker: MemoryTracker)
    : MemoryTrackerForOperatorProvider =
    new TransactionBoundMemoryTrackerForOperatorProvider(
      transactionMemoryTrackerDecorator(transactionMemoryTracker),
      this
    )
}

/**
 * Doesn't actually track anything.
 */
case object NoOpQueryMemoryTracker extends QueryMemoryTracker {

  override def heapHighWaterMark(): Long = HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED

  override def allocateHeap(bytes: Long): Unit = ()

  override def releaseHeap(bytes: Long): Unit = ()

  override def newMemoryTrackerForOperatorProvider(transactionMemoryTracker: MemoryTracker)
    : MemoryTrackerForOperatorProvider = NoOpMemoryTrackerForOperatorProvider

  override def heapHighWaterMarkOfOperator(operatorId: Int): Long = HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED

  override private[memory] def memoryTrackerForOperator(operatorId: Int): HeapMemoryTracker =
    EmptyMemoryTracker.INSTANCE
}
