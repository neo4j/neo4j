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

import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.macros.AssertMacros3.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.macros.ControlFlowMacros3.doWhile
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.JavaIteratorAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.createRetryLogic
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.evaluateBatchSize
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.evaluateConcurrency
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.handleRetry
import org.neo4j.cypher.internal.runtime.memory.TransactionWorkerThreadDelegatingMemoryTracker
import org.neo4j.exceptions.CypherExecutionInterruptedException
import org.neo4j.kernel.impl.util.collection.EagerBuffer
import org.neo4j.memory.MemoryTracker
import org.neo4j.scheduler.CallableExecutor

import java.util
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport

abstract class AbstractConcurrentTransactionsPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  concurrency: Option[Expression],
  onErrorBehaviour: InTransactionsOnErrorBehaviour,
  retryPolicy: TransactionRetryPolicy,
  disjointBy: Seq[Expression]
) extends PipeWithSource(source) {

  private[this] val disjointByArray = disjointBy.toArray

  protected def withStatus(output: ClosingIterator[CypherRow], status: TransactionStatus): ClosingIterator[CypherRow]
  protected def nullRows(value: EagerBuffer[CypherRow], state: QueryState): ClosingIterator[CypherRow]

  protected def createTask(
    innerPipe: TransactionPipeWrapper,
    batch: TransactionBatch,
    memoryTracker: MemoryTracker,
    state: QueryState,
    outputQueue: ArrayBlockingQueue[TaskOutputResult],
    activeTaskCount: AtomicInteger
  ): Runnable

  final override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    // Make sure that the accumulation of page cache statistics for inner transactions
    // in ExecutingQuery becomes thread-safe
    state.query.transactionalContext.kernelExecutingQuery.upgradeToConcurrentAccess()

    val retryLogic = createRetryLogic(onErrorBehaviour, retryPolicy, state)
    val innerPipeInTx = TransactionPipeWrapper(onErrorBehaviour, id, inner, concurrentAccess = true, retryLogic)
    val batchSizeLong = evaluateBatchSize(batchSize, state)
    val concurrencyLong = evaluateConcurrency(concurrency, state)

    val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)

    val deadlockPreventionLogic: TransactionDeadlockPreventionLogic = if (disjointByArray.nonEmpty) {
      // Use concurrency times 2 rounded up to the nearest multiple of 64 as the max batches in formation
      val maxBatchesInFormation = ((concurrencyLong.toInt * 2) + 63) & ~63 // TODO: Make this configurable?
      new ConcurrentTransactionsDeadlockPreventionLogic(disjointByArray, maxBatchesInFormation)
    } else {
      NoopTransactionDeadlockPreventionLogic
    }

    val inputBatchIterator = deadlockPreventionLogic.batchIterator(input, state, batchSizeLong, memoryTracker)

    retryLogic match {
      case Some(retryLogic) =>
        new RetryConcurrentTransactionsIterator(
          concurrencyLong,
          inputBatchIterator,
          innerPipeInTx,
          memoryTracker,
          state,
          retryLogic,
          deadlockPreventionLogic
        )
      case None =>
        new ConcurrentTransactionsIterator(
          concurrencyLong,
          inputBatchIterator,
          innerPipeInTx,
          memoryTracker,
          state,
          deadlockPreventionLogic
        )
    }
  }

  private class ConcurrentTransactionsIterator(
    maxConcurrency: Long,
    input: ClosingIterator[TransactionBatch],
    innerPipe: TransactionPipeWrapper,
    memoryTracker: MemoryTracker,
    queryState: QueryState,
    deadlockPreventionLogic: TransactionDeadlockPreventionLogic
  ) extends PrefetchingIterator[CypherRow] {
    protected val inputQueueMaxCapacity: Int = maxConcurrency.toInt
    private val outputQueueMaxCapacity: Int = maxConcurrency.toInt

    protected val inputQueue: util.ArrayDeque[TransactionBatch] =
      new util.ArrayDeque[TransactionBatch](inputQueueMaxCapacity)

    protected val outputQueue: ArrayBlockingQueue[TaskOutputResult] =
      new ArrayBlockingQueue[TaskOutputResult](outputQueueMaxCapacity)

    private var currentOutputIterator: ClosingIterator[CypherRow] = _
    private val executorService: CallableExecutor = queryState.transactionWorkerExecutor.get

    private[this] var pendingTaskCount: Int = 0 // NOTE: Only read and written by the main thread
    private[this] val activeTaskCount: AtomicInteger = new AtomicInteger(0)

    final override def closeMore(): Unit = {
      input.close()
      if (currentOutputIterator != null) {
        currentOutputIterator.close()
      }
      drainOutputQueue()
    }

    final override def produceNext(): Option[CypherRow] = {
      logMessageWithVerboseStatus("-- PRODUCE NEXT --")

      maybeEnqueueTasks()
      doWhile {
        if (!hasAvailableOutputRow) {
          // TODO: Maybe remove the separate awaitPendingRetries method and just enter this if to call pollOutputQueue
          if (pendingTaskCount > 0) {
            if (DebugSupport.DEBUG_CONCURRENT_TRANSACTIONS) {
              logMessage(s"Waiting on output queue pendingTaskCount=$pendingTaskCount")
            }
            val taskResult = pollOutputQueue() // NOTE: blocking operation!
            if (taskResult != null) {
              pendingTaskCount -= 1
              TransactionPipeWrapper.updateStatisticsAndProfileInformation(taskResult.status, queryState)
              val error = taskResult.nonRecoverableError
              if (error != null && shouldReportError(error)) {
                try {
                  drainOutputQueue(error)
                } finally {
                  throw error
                }
              }
              processTaskResult(taskResult)
              currentOutputIterator = taskResult.outputIterator
            }
          } else if (!awaitPendingRetries() && !hasAvailableInput) {
            logMessage("No more rows to prefetch. Iterator will finish on next call to .next")
            return None
          }
          maybeEnqueueTasks()
        }
      }(!hasAvailableOutputRow)
      logMessage("Outputting row")
      Some(currentOutputIterator.next())
    }

    final protected def hasPendingTasks: Boolean = pendingTaskCount > 0

    // NOTE: protected methods not declared final can be overridden by RetryConcurrentTransactionsIterator
    protected def awaitPendingRetries(): Boolean = false

    protected def pollOutputQueue(): TaskOutputResult = {
      // TODO: Make sure we respect outer transaction termination/timeout here as well!
      val taskResult = outputQueue.take() // NOTE: blocking operation!
      taskResult
    }

    protected def processTaskResult(taskResult: TaskOutputResult): Unit = {
      // Notify deadlock prevention logic that the batch completed (if not being retried)
      if (taskResult.completedBatch != null) {
        deadlockPreventionLogic.onBatchCompleted(taskResult.completedBatch)
      }
    }

    final protected def drainOutputQueue(error: Throwable = null): Unit = {
      while (pendingTaskCount > 0) {
        val taskOutputResult = outputQueue.take()
        val newError = taskOutputResult.nonRecoverableError
        if (DebugSupport.DEBUG_CONCURRENT_TRANSACTIONS) {
          DebugSupport.CONCURRENT_TRANSACTIONS.log(
            "Drained %s",
            if (newError != null) newError else "<committed>"
          )
        }
        if (error != null && newError != null && newError != error && shouldReportError(newError)) {
          error.addSuppressed(newError)
        }
        pendingTaskCount -= 1
      }
    }

    private def shouldReportError(error: Throwable): Boolean = {
      !error.isInstanceOf[CypherExecutionInterruptedException]
    }

    private def maybeEnqueueTasks(): Unit = {
      doWhile {
        ensureActiveTasks()
      }(saturateInputQueue())
    }

    private def saturateInputQueue(): Boolean = {
      var addedToQueue: Boolean = false

      if (inputQueue.size() < inputQueueMaxCapacity) {
        checkAndUpdateCompletedBatches()
        if (input.hasNext) {
          val batch = input.next()
          if (!batch.isMarker) {
            inputQueue.add(batch)
            addedToQueue = true
            logMessage("Queued an input batch")
          }
        }
      }

      addedToQueue
    }

    private def checkAndUpdateCompletedBatches(): Unit = {
      if (deadlockPreventionLogic.requiresBatchCompletedNotification && pendingTaskCount > 0) {
        outputQueue.forEach { taskOutputResult =>
          if (taskOutputResult.completedBatch != null) {
            // NOTE: It is ok to call onBatchCompleted multiple times for the same batch as it is idempotent.
            deadlockPreventionLogic.onBatchCompleted(taskOutputResult.completedBatch)
          }
        }
      }
    }

    final protected def ensureActiveTasks(): Boolean = {
      if (hasAvailableInput) {
        if (activeTaskCount.get() < maxConcurrency.toInt) {
          if (activeTaskCount.getAndIncrement() < maxConcurrency.toInt) {
            val input = nextAvailableInput()
            if (input != null && !input.isMarker) {
              executeTask(input)
              pendingTaskCount += 1
              logMessage("Created new task")
              return true
            }
            activeTaskCount.getAndDecrement()
          }
        }
      }
      false
    }

    protected def nextAvailableInput(): TransactionBatch = {
      if (!inputQueue.isEmpty) {
        return inputQueue.poll()
      }
      if (input.hasNext) {
        return input.next()
      }
      throw new NoSuchElementException()
    }

    private def executeTask(batch: TransactionBatch): Unit = {
      executorService.execute(createTask(
        innerPipe,
        batch,
        memoryTracker,
        queryState,
        outputQueue,
        activeTaskCount
      ))
    }

    protected def hasAvailableInput: Boolean = {
      !inputQueue.isEmpty || input.hasNext
    }

    private def hasPendingOutput: Boolean = {
      hasAvailableOutputRow || pendingTaskCount > 0
    }

    private def hasAvailableOutputRow: Boolean = {
      currentOutputIterator != null && currentOutputIterator.hasNext
    }

    protected def logMessage(message: String, verbose: Boolean = false): Unit = {
      if (DebugSupport.DEBUG_CONCURRENT_TRANSACTIONS) {
        def doLogMessage(message: String): Unit =
          DebugSupport.CONCURRENT_TRANSACTIONS.log(String.format("[%s] %s", this, message))

        doLogMessage(message)

        if (verbose) {
          if (hasAvailableInput) {
            if (input.hasNext) {
              doLogMessage("Pending input is a NEW BATCH")
            } else if (!inputQueue.isEmpty) {
              doLogMessage("Pending input is a QUEUED BATCH")
            }
          } else {
            doLogMessage("Pending input NOT AVAILABLE")
          }

          if (hasPendingOutput) {
            if (currentOutputIterator != null && currentOutputIterator.hasNext) {
              doLogMessage("Pending output is READY")
            }
          } else {
            doLogMessage("Pending output NOT AVAILABLE")
          }

          doLogMessage(s"Have $pendingTaskCount pending tasks")
        }
      }
    }

    private def logMessageWithVerboseStatus(message: String): Unit = {
      logMessage(message, verbose = true)
    }

    override def toString: String = {
      String.format("%s", Thread.currentThread().getName)
    }
  }

  private class RetryConcurrentTransactionsIterator(
    maxConcurrency: Long,
    input: ClosingIterator[TransactionBatch],
    innerPipe: TransactionPipeWrapper,
    memoryTracker: MemoryTracker,
    queryState: QueryState,
    retryLogic: TransactionRetryLogic,
    deadlockPreventionLogic: TransactionDeadlockPreventionLogic
  ) extends ConcurrentTransactionsIterator(
        maxConcurrency,
        input,
        innerPipe,
        memoryTracker,
        queryState,
        deadlockPreventionLogic
      ) {

    // We have a limit to the retry queue size, so we switch to prioritize retry batches over new input batches
    // to avoid running out of heap in case we get a lot of failed batches.
    private[this] val retryQueueSizeLimit = inputQueueMaxCapacity // TODO: Make this configurable

    private[this] val retryQueue =
      new util.PriorityQueue[RetryableTransactionBatch](inputQueueMaxCapacity, RetryableTransactionBatch.comparator)

    override protected def hasAvailableInput: Boolean = {
      !inputQueue.isEmpty || input.hasNext || !retryQueue.isEmpty
    }

    override protected def nextAvailableInput(): TransactionBatch = {
      if (!retryQueue.isEmpty) {
        val retryBatch = retryQueue.peek()
        val delay = retryBatch.nanosUntilRetry()
        if (delay <= 0L) {
          return retryQueue.poll()
        } else if (retryQueue.size() >= retryQueueSizeLimit) {
          // If the retry queue is full we should hold off on adding new input batches,
          // so it doesn't grow unbounded if the next input batches also need to be retried.
          return null
        }
      }
      if (!inputQueue.isEmpty) {
        return inputQueue.poll()
      }
      if (input.hasNext) {
        return input.next()
      }
      if (!retryQueue.isEmpty) {
        return null
      }
      throw new NoSuchElementException()
    }

    override protected def awaitPendingRetries(): Boolean = {
      // NOTE: This should only be called when pendingTaskCount is 0!
      require(!hasPendingTasks, "Expected no pending tasks when awaiting retries")
      // TODO: Make sure we respect outer transaction termination/timeout!
      val retryBatch = retryQueue.peek()
      if (retryBatch != null) {
        val delay = retryBatch.nanosUntilRetry()
        if (delay > 0L) {
          if (DebugSupport.DEBUG_CONCURRENT_TRANSACTIONS) {
            logMessage(s"Waiting on retry queue: delay=$delay")
          }
          LockSupport.parkNanos(delay)
          return true
        }
        true
      } else {
        false
      }
    }

    override protected def pollOutputQueue(): TaskOutputResult = {
      val retryBatch = retryQueue.peek()
      if (retryBatch != null) {
        val delay = Math.max(retryBatch.nanosUntilRetry(), 0L)
        // NOTE: Even if the delay is 0 we can poll the output queue here since we are going to make sure that
        //       we have saturated active tasks before we return the next output row.
        val taskResult = {
          if (DebugSupport.DEBUG_CONCURRENT_TRANSACTIONS) {
            logMessage(s"Timed waiting on output queue: delay=$delay")
          }
          // TODO: Make sure we respect outer transaction termination/timeout!
          outputQueue.poll(delay, java.util.concurrent.TimeUnit.NANOSECONDS) // NOTE: blocking operation!
        }
        taskResult
      } else {
        super.pollOutputQueue()
      }
    }

    override protected def processTaskResult(taskResult: TaskOutputResult): Unit = {
      if (DebugSupport.DEBUG_CONCURRENT_TRANSACTIONS) {
        logMessage(s"Processing task result $taskResult")
      }
      if (taskResult.retryBatch != null) {
        val batch = taskResult.retryBatch
        require(taskResult.completedBatch == null)
        taskResult.status match {
          case _: Commit =>
            checkOnlyWhenAssertionsAreEnabled(batch.retriedCount > 0, "Expected the batch to have been retried")

          case _ =>
            val retryableBatch = batch.computeNextRetryState(retryLogic)
            logMessage("Adding batch to retry queue")
            retryQueue.add(retryableBatch)
        }
      } else {
        super.processTaskResult(taskResult)
      }
    }
  }

  abstract protected class AbstractConcurrentTransactionsResultsTask(
    state: QueryState,
    outputQueue: ArrayBlockingQueue[TaskOutputResult],
    activeTaskCount: AtomicInteger
  ) extends Runnable {
    private[this] var contextMemoryTracker: MemoryTracker = null.asInstanceOf[MemoryTracker]

    override def run(): Unit = {
      var outputResult: TaskOutputResult = null
      try {
        initializeMemoryTracker()
        outputResult = consumeBatch()
        DebugSupport.CONCURRENT_TRANSACTIONS_WORKER.log("[%s] Done", this)
      } catch {
        case e: Throwable =>
          DebugSupport.CONCURRENT_TRANSACTIONS_WORKER.log(
            "[%s] %s\n%s",
            this,
            e.toString,
            e.getStackTrace.mkString("\n")
          )
          outputResult = TaskOutputResult(NonRecoverableError, null, nonRecoverableError = e)
          throw e
      } finally {
        try {
          // NOTE: We need to close the memory tracker before putting the result in the output queue,
          // since that could result in the query finishing on the main thread.
          closeMemoryTracker()
        } finally {
          try {
            outputQueue.put(outputResult)
          } finally {
            activeTaskCount.getAndDecrement()
          }
        }
      }
    }

    protected def consumeBatch(): TaskOutputResult

    private def initializeMemoryTracker(): Unit = {
      var memoryTracker = TransactionWorkerThreadDelegatingMemoryTracker.threadLocalExecutionContextMemoryTracker.get
      if (memoryTracker == null) {
        memoryTracker = state.query.transactionalContext.createExecutionContextMemoryTracker(
          state.query.queryConfig.heapEstimatorCacheConfig
        )
        TransactionWorkerThreadDelegatingMemoryTracker.threadLocalExecutionContextMemoryTracker.set(memoryTracker)
      }
      contextMemoryTracker = memoryTracker
    }

    private def closeMemoryTracker(): Unit = {
      val memoryTracker = contextMemoryTracker
      if (memoryTracker != null) {
        try {
          memoryTracker.close()
        } finally {
          TransactionWorkerThreadDelegatingMemoryTracker.threadLocalExecutionContextMemoryTracker.remove()
        }
      }
    }

    override def toString: String = {
      String.format("%-16s", Thread.currentThread().getName)
    }
  }

  protected class ConcurrentTransactionApplyResultsTask(
    innerPipe: TransactionPipeWrapper,
    batch: TransactionBatch,
    memoryTracker: MemoryTracker,
    state: QueryState,
    outputQueue: ArrayBlockingQueue[TaskOutputResult],
    activeTaskCount: AtomicInteger
  ) extends AbstractConcurrentTransactionsResultsTask(
        state,
        outputQueue,
        activeTaskCount
      ) {

    override protected def consumeBatch(): TaskOutputResult = {
      DebugSupport.CONCURRENT_TRANSACTIONS_WORKER.log("[%s] Starting batch of %d rows", this, batch.rows.size)
      val innerResult: TransactionResult = innerPipe.createResults(state, batch, memoryTracker)
      DebugSupport.CONCURRENT_TRANSACTIONS_WORKER.log("[%s] Have results", this)

      val retryDecision = innerResult.retryDecision
      val shouldRetry = RetryDecision.shouldRetry(retryDecision)
      val (transactionStatus, nonRecoverableErrorOrNull) =
        handleRetry(retryDecision, innerResult.status, onErrorBehaviour, batch)

      val resultsWithStatusIteratorOrNull = innerResult.committedResults match {
        case Some(result) =>
          batch.close()
          withStatus(result.autoClosingIterator().asClosingIterator, transactionStatus)
        case _ if nonRecoverableErrorOrNull != null =>
          batch.close()
          null
        case _ if shouldRetry =>
          null
        case _ =>
          // NOTE: nullRows closes batch.rows by using an autoClosingIterator
          withStatus(nullRows(batch.rows, state), transactionStatus)
      }

      // Pass the batch as either retryBatch or completedBatch, and the other one as null
      if (shouldRetry) {
        TaskOutputResult(
          transactionStatus,
          resultsWithStatusIteratorOrNull,
          nonRecoverableErrorOrNull,
          retryBatch = batch,
          completedBatch = null
        )
      } else {
        TaskOutputResult(
          transactionStatus,
          resultsWithStatusIteratorOrNull,
          nonRecoverableErrorOrNull,
          retryBatch = null,
          completedBatch = batch
        )
      }
    }
  }

  protected class ConcurrentTransactionForeachResultsTask(
    innerPipe: TransactionPipeWrapper,
    batch: TransactionBatch,
    memoryTracker: MemoryTracker,
    state: QueryState,
    outputQueue: ArrayBlockingQueue[TaskOutputResult],
    activeTaskCount: AtomicInteger
  ) extends AbstractConcurrentTransactionsResultsTask(
        state,
        outputQueue,
        activeTaskCount
      ) {

    override protected def consumeBatch(): TaskOutputResult = {
      DebugSupport.CONCURRENT_TRANSACTIONS_WORKER.log("[%s] Starting batch of %d rows", this, batch.rows.size)
      val result = innerPipe.consume(state, batch)
      DebugSupport.CONCURRENT_TRANSACTIONS_WORKER.log("[%s] Have results", this)

      val retryDecision = result.retryDecision
      val shouldRetry = RetryDecision.shouldRetry(retryDecision)
      val (transactionStatus, nonRecoverableErrorOrNull) =
        handleRetry(retryDecision, result.status, onErrorBehaviour, batch)

      val resultsWithStatusIteratorOrNull =
        if (nonRecoverableErrorOrNull != null) {
          batch.close()
          null
        } else if (shouldRetry) {
          null
        } else {
          val output = batch.rows.autoClosingIterator().asClosingIterator
          withStatus(output, transactionStatus)
        }
      // Pass the batch as either retryBatch or completedBatch, and the other one as null
      if (shouldRetry) {
        TaskOutputResult(
          transactionStatus,
          resultsWithStatusIteratorOrNull,
          nonRecoverableErrorOrNull,
          retryBatch = batch,
          completedBatch = null
        )
      } else {
        TaskOutputResult(
          transactionStatus,
          resultsWithStatusIteratorOrNull,
          nonRecoverableErrorOrNull,
          retryBatch = null,
          completedBatch = batch
        )
      }
    }
  }

  case class TaskOutputResult(
    status: TransactionStatus,
    outputIterator: ClosingIterator[CypherRow] = null,
    nonRecoverableError: Throwable = null,
    retryBatch: TransactionBatch = null,
    completedBatch: TransactionBatch = null
  ) {

    override def toString: String = {
      s"TaskOutputResult(status=$status, nonRecoverableError=$nonRecoverableError, retryBatch=$retryBatch)"
    }
  }
}
