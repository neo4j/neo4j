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
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.JavaIteratorAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.evaluateBatchSize
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.evaluateConcurrency
import org.neo4j.exceptions.CypherExecutionInterruptedException
import org.neo4j.kernel.impl.util.collection.EagerBuffer
import org.neo4j.memory.MemoryTracker
import org.neo4j.scheduler.CallableExecutor

import java.util
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

abstract class AbstractConcurrentTransactionApplyPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  concurrency: Expression,
  onErrorBehaviour: InTransactionsOnErrorBehaviour
) extends PipeWithSource(source) {
  protected def withStatus(output: ClosingIterator[CypherRow], status: TransactionStatus): ClosingIterator[CypherRow]
  protected def nullRows(value: EagerBuffer[CypherRow], state: QueryState): ClosingIterator[CypherRow]

  final override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val innerPipeInTx = TransactionPipeWrapper(onErrorBehaviour, inner)
    val batchSizeLong = evaluateBatchSize(batchSize, state)
    val concurrencyLong = evaluateConcurrency(concurrency, state)

    val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    val inputBatchIterator = input.eagerGrouped(batchSizeLong, memoryTracker)

    new ConcurrentTransactionsIterator(concurrencyLong, inputBatchIterator, innerPipeInTx, memoryTracker, state)
  }

  private class ConcurrentTransactionsIterator(
    maxConcurrency: Long,
    input: ClosingIterator[EagerBuffer[CypherRow]],
    innerPipe: TransactionPipeWrapper,
    memoryTracker: MemoryTracker,
    queryState: QueryState
  ) extends PrefetchingIterator[CypherRow] {
    private val inputQueueMaxCapacity: Int = maxConcurrency.toInt
    private val outputQueueMaxCapacity: Int = maxConcurrency.toInt

    private val inputQueue: util.ArrayDeque[EagerBuffer[CypherRow]] =
      new util.ArrayDeque[EagerBuffer[CypherRow]](inputQueueMaxCapacity)

    private val outputQueue: ArrayBlockingQueue[TaskOutputResult] =
      new ArrayBlockingQueue[TaskOutputResult](outputQueueMaxCapacity)

    private var currentOutputIterator: ClosingIterator[CypherRow] = _
    private val executorService: CallableExecutor = queryState.transactionWorkerExecutor.get

    private[this] var pendingTaskCount: Int = 0
    private[this] val activeTaskCount: AtomicInteger = new AtomicInteger(0)

    override def closeMore(): Unit = {
      if (input != null) input.close()
      if (currentOutputIterator != null) input.close()
    }

    override def produceNext(): Option[CypherRow] = {
      logMessageWithVerboseStatus("-- PRODUCE NEXT --")

      maybeEnqueueTasks()
      do {
        if (!hasAvailableOutputRow) {
          if (pendingTaskCount > 0) {
            if (DebugSupport.DEBUG_CONCURRENT_TRANSACTIONS) {
              logMessage(s"Waiting on output queue pendingTaskCount=$pendingTaskCount")
            }
            val taskResult = outputQueue.take() // NOTE: blocking operation!
            val error = taskResult.error
            pendingTaskCount -= 1
            if (error != null && shouldReportError(error)) {
              try {
                drainOutputQueue(error)
              } finally {
                throw error
              }
            }
            currentOutputIterator = taskResult.outputIterator
            maybeEnqueueTasks()
          } else {
            logMessage("No more rows to prefetch. Iterator will finish on next call to .next")
            return None
          }
        }
      } while (!hasAvailableOutputRow)
      logMessage("Outputting row")
      Some(currentOutputIterator.next())
    }

    private def drainOutputQueue(error: Throwable): Unit = {
      while (pendingTaskCount > 0) {
        val taskOutputResult = outputQueue.take()
        val newError = taskOutputResult.error
        if (DebugSupport.DEBUG_CONCURRENT_TRANSACTIONS) {
          DebugSupport.CONCURRENT_TRANSACTIONS.log(
            "Drained %s %s",
            taskOutputResult.status,
            if (newError != null) newError else ""
          )
        }
        if (newError != null && newError != error && shouldReportError(newError)) {
          error.addSuppressed(newError)
        }
        pendingTaskCount -= 1
      }
    }

    private def shouldReportError(error: Throwable): Boolean = {
      !error.isInstanceOf[CypherExecutionInterruptedException]
    }

    private def maybeEnqueueTasks(): Unit = {
      do {
        ensureActiveTasks()
      } while (saturateInputQueue())
    }

    private def saturateInputQueue(): Boolean = {
      var addedToQueue: Boolean = false

      if (inputQueue.size() < inputQueueMaxCapacity && input.hasNext) {
        inputQueue.add(input.next())
        addedToQueue = true
        logMessage("Queued an input batch")
      }

      addedToQueue
    }

    private def ensureActiveTasks(): Unit = {
      if (hasAvailableInput) {
        if (activeTaskCount.get() < maxConcurrency.toInt) {
          if (activeTaskCount.getAndAdd(1) < maxConcurrency.toInt) {
            createTask(nextAvailableInput())
            pendingTaskCount += 1
            logMessage("Created new task")
          } else {
            activeTaskCount.getAndAdd(-1)
          }
        }
      }
    }

    private def nextAvailableInput(): EagerBuffer[CypherRow] = {
      if (!inputQueue.isEmpty) {
        return inputQueue.poll()
      }
      if (input.hasNext) {
        return input.next()
      }
      throw new NoSuchElementException()
    }

    private def createTask(batch: EagerBuffer[CypherRow]): Unit = {
      executorService.execute(new ConcurrentTransactionsResultsTask(
        innerPipe,
        batch,
        memoryTracker,
        queryState,
        outputQueue,
        activeTaskCount
      ))
    }

    private def hasAvailableInput: Boolean = {
      !inputQueue.isEmpty || input.hasNext
    }

    private def hasPendingOutput: Boolean = {
      hasAvailableOutputRow || pendingTaskCount > 0
    }

    private def hasAvailableOutputRow: Boolean = {
      currentOutputIterator != null && currentOutputIterator.hasNext
    }

    private def logMessage(message: String, verbose: Boolean = false): Unit = {
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

  private class ConcurrentTransactionsResultsTask(
    innerPipe: TransactionPipeWrapper,
    batch: EagerBuffer[CypherRow],
    memoryTracker: MemoryTracker,
    state: QueryState,
    outputQueue: ArrayBlockingQueue[TaskOutputResult],
    activeTaskCount: AtomicInteger
  ) extends Runnable {

    override def run(): Unit = {
      var outputResult: TaskOutputResult = null
      try {
        DebugSupport.CONCURRENT_TRANSACTIONS_WORKER.log("[%s] Starting batch of %d rows", this, batch.size)
        val innerResult: TransactionResult = innerPipe.createResults(state, batch, memoryTracker)
        DebugSupport.CONCURRENT_TRANSACTIONS_WORKER.log("[%s] Have results", this)

        val output = innerResult.committedResults match {
          case Some(result) =>
            batch.close()
            result.autoClosingIterator().asClosingIterator
          case _ => nullRows(batch, state)
        }
        val outputIterator = withStatus(output, innerResult.status)
        outputResult = TaskOutputResult(innerResult.status, outputIterator)
        DebugSupport.CONCURRENT_TRANSACTIONS_WORKER.log("[%s] Done", this)
      } catch {
        case e: Throwable =>
          DebugSupport.CONCURRENT_TRANSACTIONS_WORKER.log(
            "[%s] %s\n%s",
            this,
            e.toString,
            e.getStackTrace.mkString("\n")
          )
          outputResult = TaskOutputResult(NotRun /*TODO: FIXME*/, null, error = e)
          throw e
      } finally {
        // TODO: Do not handle interrupts
        var interrupted: Boolean = false
        do {
          interrupted = false
          try {
            outputQueue.put(outputResult)
          } catch {
            case _: InterruptedException =>
              interrupted = true
              Thread.interrupted()
          } finally {
            if (!interrupted) {
              activeTaskCount.getAndAdd(-1)
            }
          }
        } while (interrupted)
      }
    }

    override def toString: String = {
      String.format("%-16s", Thread.currentThread().getName)
    }
  }

  case class TaskOutputResult(
    status: TransactionStatus,
    outputIterator: ClosingIterator[CypherRow] = null,
    error: Throwable = null
  )
}
