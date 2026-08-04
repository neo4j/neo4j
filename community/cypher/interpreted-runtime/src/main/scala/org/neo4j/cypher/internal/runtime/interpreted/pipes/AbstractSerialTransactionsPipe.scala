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

import org.neo4j.cypher.internal.logical.plans.TransactionalPlan.RecoveryMode
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.createRetryLogic
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.evaluateBatchSize
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.handleRetry
import org.neo4j.kernel.impl.util.collection.EagerBuffer
import org.neo4j.memory.MemoryTracker

import java.util.concurrent.locks.LockSupport

import scala.annotation.tailrec

abstract class AbstractSerialTransactionsPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  recoveryMode: RecoveryMode,
  retryPolicy: TransactionRetryPolicy
) extends PipeWithSource(source) {

  protected def withStatus(output: ClosingIterator[CypherRow], status: TransactionStatus): ClosingIterator[CypherRow]

  protected def getResult(
    innerPipeInTx: TransactionPipeWrapper,
    state: QueryState,
    batch: TransactionBatch,
    memoryTracker: MemoryTracker
  ): TransactionResult

  protected def produceOutput(
    eagerBuffer: EagerBuffer[CypherRow],
    result: TransactionResult,
    batch: TransactionBatch,
    state: QueryState
  ): ClosingIterator[CypherRow]

  final override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {

    val retryLogic = createRetryLogic(retryPolicy, state)
    val innerPipeInTx = TransactionPipeWrapper(recoveryMode, id, inner, concurrentAccess = false, retryLogic)
    val batchSizeLong = evaluateBatchSize(batchSize, state)
    val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)

    def runBatch(batch: TransactionBatch) = {
      val result = getResult(innerPipeInTx, state, batch, memoryTracker)
      val statistics = result.status.queryStatistics
      if (statistics != null) {
        state.query.addStatistics(statistics)
      }
      result
    }

    @tailrec
    def executeWithRetry(batch: TransactionBatch, retryLogic: TransactionRetryLogic): TransactionResult = {
      val result = runBatch(batch)
      val (status, throwable) = handleRetry(result.retryDecision, result.status, recoveryMode, retryPolicy, batch)

      if (throwable != null) {
        throw throwable
      }

      result.retryDecision match {
        case RetryDecision.ShouldRetry =>
          val nextBatch = batch.computeNextRetryState(retryLogic)
          LockSupport.parkNanos(nextBatch.nanosUntilRetry())
          executeWithRetry(nextBatch, retryLogic)
        case _ =>
          result.copy(status = status)
      }
    }

    input
      .eagerGrouped(batchSizeLong, memoryTracker)
      .flatMap { eagerBuffer =>
        val batch = TransactionBatch(eagerBuffer)
        val innerResult = retryLogic match {
          case Some(value) => executeWithRetry(batch, value)
          case None        => runBatch(batch)
        }

        produceOutput(eagerBuffer, innerResult, batch, state)
      }
  }
}
