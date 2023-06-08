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

import org.neo4j.cypher.internal.config.CUSTOM_MEMORY_TRACKING
import org.neo4j.cypher.internal.config.MEMORY_TRACKING
import org.neo4j.cypher.internal.config.MemoryTracking
import org.neo4j.cypher.internal.config.NO_TRACKING
import org.neo4j.cypher.internal.runtime.GrowingArray
import org.neo4j.cypher.internal.runtime.memory.TrackingQueryMemoryTracker.MemoryTrackerPerOperator
import org.neo4j.cypher.internal.runtime.memory.TrackingQueryMemoryTracker.OperatorMemoryTracker
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.ExecutionContextMemoryTrackerProvider
import org.neo4j.memory.HeapHighWaterMarkTracker
import org.neo4j.memory.HeapMemoryTracker
import org.neo4j.memory.IsScopedMemoryTracker
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.MemoryTracker

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.LongAdder

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

/**
 * Tracks the heap high water mark for one Cypher query running with the parallel runtime.
 */
class ParallelTrackingQueryMemoryTracker extends QueryMemoryTracker {
  override def heapHighWaterMark(): Long = HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED

  override def allocateHeap(bytes: Long): Unit = ???

  override def releaseHeap(bytes: Long): Unit = ???

  override def newMemoryTrackerForOperatorProvider(transactionMemoryTracker: MemoryTracker)
    : MemoryTrackerForOperatorProvider = new WorkerThreadDelegatingMemoryTracker(transactionMemoryTracker)

  override def heapHighWaterMarkOfOperator(operatorId: Int): Long = HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED

  override private[memory] def memoryTrackerForOperator(operatorId: Int): HeapMemoryTracker = ???
}

class WorkerThreadDelegatingMemoryTracker(val transactionMemoryTracker: MemoryTracker)
    extends MemoryTracker with MemoryTrackerForOperatorProvider {

  override def usedNativeMemory(): Long = {
    delegateMemoryTracker.usedNativeMemory()
  }

  override def estimatedHeapMemory(): Long = {
    delegateMemoryTracker.estimatedHeapMemory()
  }

  override def allocateNative(bytes: Long): Unit = {
    delegateMemoryTracker.allocateNative(bytes)
  }

  override def releaseNative(bytes: Long): Unit = {
    delegateMemoryTracker.releaseNative(bytes)
  }

  override def allocateHeap(bytes: Long): Unit = {
    delegateMemoryTracker.allocateHeap(bytes)
  }

  override def releaseHeap(bytes: Long): Unit = {
    delegateMemoryTracker.releaseHeap(bytes)
  }

  override def heapHighWaterMark(): Long = {
    delegateMemoryTracker.heapHighWaterMark()
  }

  override def reset(): Unit = {
    delegateMemoryTracker.reset()
  }

  override def getScopedMemoryTracker: MemoryTracker = {
    new ConcurrentScopedMemoryTracker(this)
  }

  private def delegateMemoryTracker: MemoryTracker = {
    Thread.currentThread() match {
      case workerThread: ExecutionContextMemoryTrackerProvider =>
        workerThread.executionContextMemoryTracker()
      case _ =>
        require(transactionMemoryTracker != null, "transactionMemoryTracker should not be null")
        transactionMemoryTracker
    }
  }

  override def memoryTrackerForOperator(operatorId: Int): MemoryTracker = {
    // NOTE: We currently do not support tracking query heap usage high water mark per operator
    this
  }
}

class ConcurrentScopedMemoryTracker(delegate: MemoryTracker) extends IsScopedMemoryTracker {
  private[this] val trackedNative: LongAdder = new LongAdder
  private[this] val trackedHeap: LongAdder = new LongAdder
  private[this] val _isClosed: AtomicBoolean = new AtomicBoolean(false)

  override def usedNativeMemory: Long = trackedNative.sum()
  override def estimatedHeapMemory: Long = trackedHeap.sum()

  override def allocateNative(bytes: Long): Unit = {
    throwIfClosed()
    delegate.allocateNative(bytes)
    trackedNative.add(bytes)
  }

  override def releaseNative(bytes: Long): Unit = {
    throwIfClosed()
    delegate.releaseNative(bytes)
    trackedNative.add(-bytes)
  }

  override def allocateHeap(bytes: Long): Unit = {
    throwIfClosed()
    delegate.allocateHeap(bytes)
    trackedHeap.add(bytes)
  }

  override def releaseHeap(bytes: Long): Unit = {
    throwIfClosed()
    delegate.releaseHeap(bytes)
    trackedHeap.add(-bytes)
  }

  private def throwIfClosed(): Unit = {
    if (isClosed) throw new IllegalStateException("Should not use a closed ScopedMemoryTracker")
  }
  override def heapHighWaterMark = throw new UnsupportedOperationException

  override def reset(): Unit = {
    val nativeUsage = trackedNative.sumThenReset()
    // NOTE: Native memory usage is often zero, so avoid call when we can since it synchronizes directly with
    //       the transaction memory pool.
    if (nativeUsage != 0L) {
      delegate.releaseNative(nativeUsage)
    }
    val heapUsage = trackedHeap.sumThenReset()
    if (heapUsage != 0L) {
      delegate.releaseHeap(heapUsage)
    }
  }

  override def close(): Unit = {
    // On a parent ScopedMemoryTracker, only release memory if that parent was not already closed.
    if (
      !delegate.isInstanceOf[IsScopedMemoryTracker] || !(delegate.asInstanceOf[
        IsScopedMemoryTracker
      ]).isClosed
    ) {
      reset()
    }
    _isClosed.set(true)
  }

  override def getScopedMemoryTracker: MemoryTracker = {
    new ConcurrentScopedMemoryTracker(this)
  }

  override def isClosed: Boolean = _isClosed.get()
}
