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

import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap
import org.neo4j.collection.trackable.HeapTrackingConcurrentLongObjectHashMap
import org.neo4j.cypher.internal.config.CUSTOM_MEMORY_TRACKING
import org.neo4j.cypher.internal.config.MEMORY_TRACKING
import org.neo4j.cypher.internal.config.MemoryTracking
import org.neo4j.cypher.internal.config.NO_TRACKING
import org.neo4j.cypher.internal.runtime.GrowingArray
import org.neo4j.cypher.internal.runtime.debug.DebugSupport.DEBUG_MEMORY_TRACKING
import org.neo4j.cypher.internal.runtime.memory.TrackingQueryMemoryTracker.MemoryTrackerPerOperator
import org.neo4j.cypher.internal.runtime.memory.TrackingQueryMemoryTracker.OperatorMemoryTracker
import org.neo4j.memory.HeapHighWaterMarkTracker
import org.neo4j.memory.HeapMemoryTracker
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.memory.ScopedMemoryTracker
import org.neo4j.util.VisibleForTesting

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
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
    with HeapHighWaterMarkTracker {

  /**
   * Return a new [[MemoryTrackerForOperatorProvider]] that wraps the given transactionMemoryTracker,
   * but also reports to this [[QueryMemoryTracker]]. Use this method to obtain memory trackers for operators that
   * execute in a specific transaction (possibly one of multiple transactions in a query) as part of the query
   * tracked by this [[QueryMemoryTracker]].
   */
  def newMemoryTrackerForOperatorProvider(transactionMemoryTracker: MemoryTracker): MemoryTrackerForOperatorProvider

  def debugPrintSummary(): Unit = {}
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
class TrackingQueryMemoryTracker extends QueryMemoryTracker with HeapMemoryTracker {

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

  /**
   * Get the memory tracker for the operator with the given id.
   * This memory tracker is not bound to any transaction.
   *
   * @param operatorId the id of the operator
   */
  private[memory] def memoryTrackerForOperator(operatorId: Int): OperatorMemoryTracker = {
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

  override def newMemoryTrackerForOperatorProvider(transactionMemoryTracker: MemoryTracker)
    : MemoryTrackerForOperatorProvider = NoOpMemoryTrackerForOperatorProvider

  override def heapHighWaterMarkOfOperator(operatorId: Int): Long = HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED

}

/**
 * Tracks the heap high water mark for one Cypher query running with the parallel runtime.
 */
class ParallelTrackingQueryMemoryTracker extends QueryMemoryTracker {

  private[this] val debugMemoryTracker = if (DEBUG_MEMORY_TRACKING) {
    new ParallelDebugMemoryTracker(new WorkerThreadDelegatingMemoryTracker)
  } else {
    null
  }

  override def heapHighWaterMark(): Long = HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED

  override def newMemoryTrackerForOperatorProvider(transactionMemoryTracker: MemoryTracker)
    : MemoryTrackerForOperatorProvider = {
    if (DEBUG_MEMORY_TRACKING) {
      debugMemoryTracker
    } else {
      new WorkerThreadDelegatingMemoryTracker
    }
  }

  override def heapHighWaterMarkOfOperator(operatorId: Int): Long = HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED

  override def debugPrintSummary(): Unit = {
    if (DEBUG_MEMORY_TRACKING) {
      debugMemoryTracker.debugPrintSummary()
    }
  }
}

/**
 * A memory tracker used when running with `PROFILE` in parallel runtime.
 *
 * Keeps track of per-operator heap-usage which adds a performance overhead so this class should only be used for PROFILE queries.
 */
class ProfilingParallelTrackingQueryMemoryTracker(memoryTracker: MemoryTracker) extends QueryMemoryTracker
    with MemoryTrackerForOperatorProvider {

  private val memoryPerOperator: HeapTrackingConcurrentLongObjectHashMap[MemoryTracker] =
    HeapTrackingConcurrentLongObjectHashMap.newMap(memoryTracker)
  private lazy val delegate = new WorkerThreadDelegatingMemoryTracker

  override def newMemoryTrackerForOperatorProvider(transactionMemoryTracker: MemoryTracker)
    : MemoryTrackerForOperatorProvider =
    this

  override def heapHighWaterMarkOfOperator(operatorId: Int): Long = {
    if (memoryPerOperator.containsKey(operatorId)) {
      memoryPerOperator.get(operatorId).heapHighWaterMark()
    } else {
      HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED
    }
  }

  override def heapHighWaterMark(): Long = {
    var count = 0L
    memoryPerOperator.forEachValue(v => {
      count += v.heapHighWaterMark()
    })
    count
  }

  override def memoryTrackerForOperator(operatorId: Int): MemoryTracker = memoryPerOperator.computeIfAbsent(
    operatorId,
    i =>
      new ProfilingParallelHighWaterMarkTrackingWorkerMemoryTracker(delegate)
  )

  override def setInitializationMemoryTracker(memoryTracker: MemoryTracker): Unit =
    delegate.setInitializationMemoryTracker(memoryTracker)
}

/**
 * A memory tracker and MemoryTrackerForOperatorProvider that delegates all calls to the
 * thread local execution context memory tracker if the current thread is a Cypher worker thread,
 * or otherwise to a dedicated execution context memory tracker used for query initialization.
 */
class WorkerThreadDelegatingMemoryTracker extends MemoryTracker with MemoryTrackerForOperatorProvider {

  private[this] var _initializationMemoryTracker: MemoryTracker = _

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

  // NOTE: We assume that getting a scoped memory tracker from WorkerThreadDelegatingMemoryTracker
  //       needs to be able to support a concurrent use-case,
  //       e.g. by a heap tracking concurrent collection used for hash join or aggregation.
  override def getScopedMemoryTracker: MemoryTracker = {
    new ParallelScopedMemoryTracker(this)
  }

  private def delegateMemoryTracker: MemoryTracker = {
    Thread.currentThread() match {
      case workerThread: ExecutionContextMemoryTrackerProvider =>
        workerThread.executionContextMemoryTracker()
      case _ =>
        // NOTE: Here we assume that the thread is the calling thread that owns the transaction and started
        //       the query execution. Usually this happens before any workers are started,
        //       but this owner thread will be able to use a dedicated ExecutionContextMemoryTracker
        //       from the initialization query state, which should also be safe to use concurrently with worker threads.
        //       If any other threads than the owner thread or the worker threads are interacting
        //       with this memory tracker concurrently, that would not be safe from race conditions,
        //       but that is generally not supported with the execution state in the parallel runtime and not expected.
        _initializationMemoryTracker
    }
  }

  override def memoryTrackerForOperator(operatorId: Int): MemoryTracker = {
    // NOTE: We currently do not support tracking query heap usage high water mark per operator
    this
  }

  override def setInitializationMemoryTracker(memoryTracker: MemoryTracker): Unit = {
    _initializationMemoryTracker = memoryTracker
  }

  @VisibleForTesting
  def initializationMemoryTracker: MemoryTracker = {
    _initializationMemoryTracker
  }
}

private class ProfilingParallelHighWaterMarkTrackingWorkerMemoryTracker(
  delegate: WorkerThreadDelegatingMemoryTracker
) extends MemoryTracker {

  private val heapUsage = new LongAdder()
  private val highWaterMark = new AtomicLong()

  override def allocateHeap(bytes: Long): Unit = {
    delegate.allocateHeap(bytes)
    heapUsage.add(bytes)
    computeNewHighWaterMark()
  }

  override def releaseHeap(bytes: Long): Unit = {
    heapUsage.add(-bytes)
    delegate.releaseHeap(bytes)
  }

  private def computeNewHighWaterMark(): Unit = {
    var current = -1L
    var newValue = -1L
    do {
      newValue = heapUsage.sum()
      current = highWaterMark.get()
      if (current >= newValue) {
        return
      }
    } while (!highWaterMark.weakCompareAndSetVolatile(current, newValue))
  }

  override def heapHighWaterMark(): Long = {
    highWaterMark.get()
  }

  override def reset(): Unit = {
    delegate.reset()
    heapUsage.reset()
  }

  override def getScopedMemoryTracker: MemoryTracker = new ParallelScopedMemoryTracker(this)

  override def usedNativeMemory(): Long = delegate.usedNativeMemory()

  override def estimatedHeapMemory(): Long = delegate.estimatedHeapMemory()

  override def allocateNative(bytes: Long): Unit = delegate.allocateNative(bytes)

  override def releaseNative(bytes: Long): Unit = delegate.releaseNative(bytes)

}

class ParallelDebugMemoryTracker(delegate: MemoryTracker with MemoryTrackerForOperatorProvider) extends MemoryTracker
    with MemoryTrackerForOperatorProvider {

  private case class Allocation(
    allocationCount: Long,
    releaseCount: Long,
    allocations: Set[String],
    releases: Set[String]
  )

  private[this] val allocations = new ConcurrentHashMap[Long, Allocation]()

  override def usedNativeMemory(): Long = delegate.usedNativeMemory()
  override def estimatedHeapMemory(): Long = delegate.estimatedHeapMemory()
  override def allocateNative(bytes: Long): Unit = delegate.allocateNative(bytes)
  override def releaseNative(bytes: Long): Unit = delegate.releaseNative(bytes)

  override def allocateHeap(bytes: Long): Unit = {
    allocations.compute(
      bytes,
      (_: Long, oldAllocation: Allocation) => {
        val allocation = if (oldAllocation != null) {
          oldAllocation
        } else {
          val newAllocation = Allocation(0, 0, Set.empty, Set.empty)
          newAllocation
        }
        allocation.copy(
          allocationCount = allocation.allocationCount + 1,
          allocations = allocation.allocations + stackTraceKey(new Throwable(s"allocateHeap($bytes)"))
        )
      }
    )
    delegate.allocateHeap(bytes)
  }

  override def releaseHeap(bytes: Long): Unit = {
    allocations.compute(
      bytes,
      (_: Long, oldAllocation: Allocation) => {
        val allocation =
          if (oldAllocation != null) {
            oldAllocation
          } else {
            val newAllocation = Allocation(0, 0, Set.empty, Set.empty)
            newAllocation
          }
        allocation.copy(
          releaseCount = allocation.releaseCount + 1,
          releases = allocation.releases + stackTraceKey(new Throwable(s"releaseHeap($bytes)"))
        )
      }
    )
    delegate.releaseHeap(bytes)
  }
  override def heapHighWaterMark(): Long = delegate.heapHighWaterMark()
  override def reset(): Unit = delegate.reset()
  override def getScopedMemoryTracker: MemoryTracker = delegate.getScopedMemoryTracker

  private def stackTraceKey(t: Throwable): String = {
    // Make a key of the stack trace
    // Do not include suppressed or intermediate causes
    val nl = System.lineSeparator()
    val sb = new StringBuilder()
    sb ++= t.getClass.getCanonicalName
    sb ++= nl
    sb ++= t.getMessage.hashCode.toString
    sb ++= nl
    sb ++= t.getStackTrace.mkString(nl)
    sb.result()
  }

  override def memoryTrackerForOperator(operatorId: Int): MemoryTracker = {
    this
  }

  override def setInitializationMemoryTracker(memoryTracker: MemoryTracker): Unit = {
    delegate.setInitializationMemoryTracker(memoryTracker)
  }

  def debugPrintSummary(): Unit = {
    var foundMismatch = false
    allocations.forEach((bytes, allocation) => {
      if (allocation.allocationCount != allocation.releaseCount) {
        foundMismatch = true
        printf(
          "* Mismatched heap allocation of %s bytes: allocationCount=%s releaseCount=%s\n  Allocations:\n%s\n\n  Releases:\n%s\n\n",
          bytes,
          allocation.allocationCount,
          allocation.releaseCount,
          allocation.allocations.mkString("\n--------\n"),
          allocation.releases.mkString("\n--------\n")
        )
      }
    })
    if (!foundMismatch) {
      print("Heap allocations OK\n")
    }
  }
}

/**
 * This is used by concurrent heap tracking collections (that are shared between workers) in the parallel runtime,
 * that also need a scoped memory tracker to be able to release all the tracked memory at once on close.
 * Since we do not know which worker will call close we track the scoped allocations using LongAdders.
 *
 * NOTE: The worker that calls close need to have exclusive access to the tracker guaranteed by
 *       external mechanisms, as the sums could otherwise be incorrect if it was allowed to race with
 *       other allocate/release calls.
 *
 * @param delegate The delegate needs to be thread-safe, typically an instance of WorkerThreadDelegatingMemoryTracker
 */
private class ParallelScopedMemoryTracker(delegate: MemoryTracker) extends ScopedMemoryTracker {
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
  override def heapHighWaterMark: Long = throw new UnsupportedOperationException

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
      !delegate.isInstanceOf[ScopedMemoryTracker] || !delegate.asInstanceOf[
        ScopedMemoryTracker
      ].isClosed
    ) {
      reset()
    }
    _isClosed.set(true)
  }

  override def getScopedMemoryTracker: MemoryTracker = {
    new ParallelScopedMemoryTracker(this)
  }

  override def isClosed: Boolean = _isClosed.get()
}
