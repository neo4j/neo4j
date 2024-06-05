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

import org.neo4j.cypher.internal.RecoverableCypherError
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.EntityTransformer
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.CypherRowEntityTransformer
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.assertTransactionStateIsEmpty
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.logError
import org.neo4j.cypher.internal.runtime.interpreted.profiler.InterpretedProfileInformation
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherExecutionInterruptedException
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.helpers.MathUtil
import org.neo4j.kernel.impl.util.collection.EagerBuffer
import org.neo4j.kernel.impl.util.collection.EagerBuffer.createEagerBuffer
import org.neo4j.memory.MemoryTracker

import scala.util.Try
import scala.util.control.NonFatal

/**
 * Wraps a pipe to execute that pipe in separate transactions.
 * 
 * NOTE! Implementations might keep state that is not safe to re-use between queries. Create a new instance for each query.
 */
trait TransactionPipeWrapper {
  def outerId: Id

  def inner: Pipe

  def concurrentAccess: Boolean

  /**
   * Consumes the inner pipe in a new transaction and discard the resulting rows.
   * 
   * @param state query state
   * @param outerRows outer rows, will not be closed as part of this call
   */
  def consume(state: QueryState, outerRows: EagerBuffer[CypherRow]): TransactionStatus = {
    processBatch(state, outerRows)(_ => ())
  }

  /**
   * Consumes the inner pipe in a new transaction and returns the inner rows.
   *
   * @param state query state
   * @param outerRows outer rows, will not be closed as part of this call
   * @param memoryTracker memory tracker for tracking the buffered resulting rows
   */
  def createResults(
    state: QueryState,
    outerRows: EagerBuffer[CypherRow],
    memoryTracker: MemoryTracker
  ): TransactionResult = {
    val entityTransformer = new CypherRowEntityTransformer(state.query.entityTransformer)
    val innerResult = createEagerBuffer[CypherRow](memoryTracker, math.min(outerRows.size(), 1024).toInt)

    val status = processBatch(state, outerRows) { innerRow =>
      // Row based caching relies on the transaction state to avoid stale reads (see AbstractCachedProperty.apply).
      // Since we do not share the transaction state we must clear the cached properties.
      innerRow.invalidateCachedProperties()
      innerResult.add(entityTransformer.copyWithEntityWrappingValuesRebound(innerRow))
    }

    status match {
      case commit: Commit => TransactionResult(commit, Some(innerResult))
      case other =>
        innerResult.close()
        TransactionResult(other, None)
    }
  }

  protected def processBatch(
    state: QueryState,
    outerRows: EagerBuffer[CypherRow] // Should not be closed
  )(f: CypherRow => Unit): TransactionStatus

  protected def shouldBreak: Boolean

  /**
   * Evaluates inner pipe in a new transaction.
   * 
   * @param state query state
   * @param outerRows buffered outer rows, will not be closed by this method
   * @param f function to apply to inner rows
   */
  protected def createInnerResultsInNewTransaction(
    state: QueryState,
    outerRows: EagerBuffer[CypherRow] // Should not be closed
  )(f: CypherRow => Unit): TransactionStatus = {

    // Ensure that no write happens before a 'CALL { ... } IN TRANSACTIONS'
    assertTransactionStateIsEmpty(state)

    // beginTx()
    val stateWithNewTransaction = state.withNewTransaction(concurrentAccess)
    val innerTxContext = stateWithNewTransaction.query.transactionalContext
    val transactionId = innerTxContext.userTransactionId
    val entityTransformer = new CypherRowEntityTransformer(stateWithNewTransaction.query.entityTransformer)

    var innerIterator: ClosingIterator[CypherRow] = null
    try {
      val batchIterator = outerRows.iterator()
      while (batchIterator.hasNext) {
        val outerRow = batchIterator.next()

        if (shouldBreak) {
          throw CypherExecutionInterruptedException.concurrentBatchTransactionInterrupted()
        }

        outerRow.invalidateCachedProperties()

        val reboundRow = entityTransformer.copyWithEntityWrappingValuesRebound(outerRow)
        val innerState = stateWithNewTransaction.withInitialContext(reboundRow)

        innerIterator = inner.createResults(innerState)
        innerIterator.foreach(f.apply) // Consume result before commit
      }

      val statistics =
        stateWithNewTransaction.getStatistics + QueryStatistics(transactionsStarted = 1, transactionsCommitted = 1)
      val profileInformation = stateWithNewTransaction.profileInformation
      if (profileInformation != null) {
        innerTxContext.kernelStatisticProvider.registerCommitPhaseStatisticsListener(
          profileInformation.commitPhaseStatisticsListenerFor(outerId)
        )
      }
      innerTxContext.commitTransaction()
      Commit(transactionId, statistics, profileInformation)
    } catch {
      case RecoverableCypherError(e) =>
        logError(state, transactionId, e)

        Try(Option(innerIterator).foreach(_.close()))
          .failed
          .foreach(e.addSuppressed)

        try {
          innerTxContext.rollback()
        } catch {
          case NonFatal(rollbackException) =>
            e.addSuppressed(rollbackException)
            throw e
        }
        val statistics = QueryStatistics(transactionsStarted = 1, transactionsRolledBack = 1)
        val profileInformation = stateWithNewTransaction.profileInformation
        Rollback(transactionId, e, statistics, profileInformation)
    } finally {
      innerTxContext.close()
      stateWithNewTransaction.close()
    }
  }
}

class OnErrorContinueTxPipe(val outerId: Id, val inner: Pipe, val concurrentAccess: Boolean)
    extends TransactionPipeWrapper {

  override def processBatch(
    state: QueryState,
    outerRows: EagerBuffer[CypherRow]
  )(f: CypherRow => Unit): TransactionStatus = {
    createInnerResultsInNewTransaction(state, outerRows)(f)
  }

  protected def shouldBreak: Boolean = false
}

// NOTE! Keeps state that is not safe to re-use between queries. Create a new instance for each query.
class OnErrorBreakTxPipe(val outerId: Id, val inner: Pipe, val concurrentAccess: Boolean)
    extends TransactionPipeWrapper {
  @volatile private[this] var break: Boolean = false

  override def shouldBreak: Boolean = { break }

  override def processBatch(
    state: QueryState,
    outerRows: EagerBuffer[CypherRow]
  )(f: CypherRow => Unit): TransactionStatus = {
    if (break) {
      NotRun
    } else {
      createInnerResultsInNewTransaction(state, outerRows)(f) match {
        case commit: Commit => commit
        case rollback: Rollback =>
          break = true
          rollback
        case other => throw new IllegalStateException(s"Unexpected transaction status $other")
      }
    }
  }
}

class OnErrorFailTxPipe(val outerId: Id, val inner: Pipe, val concurrentAccess: Boolean)
    extends TransactionPipeWrapper {
  require(!concurrentAccess) // NOTE: We instead use OnErrorBreakTxPipe in concurrent execution

  override def shouldBreak: Boolean = false

  override def processBatch(
    state: QueryState,
    outerRows: EagerBuffer[CypherRow]
  )(f: CypherRow => Unit): TransactionStatus = {
    createInnerResultsInNewTransaction(state, outerRows)(f) match {
      case commit: Commit     => commit
      case rollback: Rollback => throw rollback.failure
      case other              => throw new IllegalStateException(s"Unexpected transaction status $other")
    }
  }
}

sealed trait TransactionStatus {
  def queryStatistics: QueryStatistics
  def profileInformation: InterpretedProfileInformation
}

case class Commit(
  transactionId: String,
  queryStatistics: QueryStatistics,
  profileInformation: InterpretedProfileInformation
) extends TransactionStatus

case class Rollback(
  transactionId: String,
  failure: Throwable,
  queryStatistics: QueryStatistics,
  profileInformation: InterpretedProfileInformation
) extends TransactionStatus

case object NotRun extends TransactionStatus {
  override def queryStatistics: QueryStatistics = null
  override def profileInformation: InterpretedProfileInformation = null
}

case object NonRecoverableError extends TransactionStatus {
  override def queryStatistics: QueryStatistics = null
  override def profileInformation: InterpretedProfileInformation = null
}

case class TransactionResult(status: TransactionStatus, committedResults: Option[EagerBuffer[CypherRow]])

object TransactionPipeWrapper {

  /**
   * Wrap a pipeline to run in new transactions based on the specified behaviour.
   * 
   * NOTE! Implementations might keep state that is not safe to re-use between queries. Create a new instance for each query.
   */
  def apply(
    error: InTransactionsOnErrorBehaviour,
    outerId: Id,
    inner: Pipe,
    concurrentAccess: Boolean
  ): TransactionPipeWrapper = {
    error match {
      case OnErrorContinue                 => new OnErrorContinueTxPipe(outerId, inner, concurrentAccess)
      case OnErrorBreak                    => new OnErrorBreakTxPipe(outerId, inner, concurrentAccess)
      case OnErrorFail if concurrentAccess =>
        // NOTE: We intentionally use OnErrorBreakTxPipe for OnErrorFail in concurrent execution,
        //       since we need to send the error back to the main thread anyway.
        new OnErrorBreakTxPipe(outerId, inner, concurrentAccess)
      case OnErrorFail => new OnErrorFailTxPipe(outerId, inner, concurrentAccess)
      case other       => throw new UnsupportedOperationException(s"Unsupported error behaviour $other")
    }
  }

  /**
   * Recursively finds entity wrappers and rebinds the entities to the current transaction
   */
  class CypherRowEntityTransformer(entityTransformer: EntityTransformer) {

    def copyWithEntityWrappingValuesRebound(row: CypherRow): CypherRow = {
      if (row.valueExists(entityTransformer.needsRebinding)) {
        row.copyMapped(entityTransformer.rebindEntityWrappingValue)
      } else {
        row
      }
    }
  }

  def evaluateBatchSize(batchSize: Expression, state: QueryState): Long = {
    PipeHelper.evaluateStaticLongOrThrow(batchSize, _ > 0, state, "OF ... ROWS", " Must be a positive integer.")
  }

  def evaluateConcurrency(concurrency: Option[Expression], state: QueryState): Long = {
    val concurrencyLong = concurrency match {
      case Some(c) =>
        PipeHelper.evaluateStaticLongOrThrow(
          c,
          _ => true,
          state,
          "IN ... CONCURRENT TRANSACTIONS",
          ""
        )

      case None =>
        0L
    }
    val numberOfProcessors = Runtime.getRuntime.availableProcessors
    val maxConcurrency = numberOfProcessors * 20
    var effectiveConcurrency = MathUtil.clamp(concurrencyLong, Int.MinValue, maxConcurrency).toInt
    if (effectiveConcurrency <= 0) {
      effectiveConcurrency = Math.max(numberOfProcessors + effectiveConcurrency, 1)
    }
    effectiveConcurrency
  }

  def assertTransactionStateIsEmpty(state: QueryState): Unit = {
    if (state.query.transactionalContext.dataRead.transactionStateHasChanges)
      throw new InternalException("Expected transaction state to be empty when calling transactional subquery.")
  }

  private def logError(state: QueryState, innerTxId: String, t: Throwable): Unit = {
    val outerTxId = state.query.transactionalContext.userTransactionId
    val log = state.query.logProvider.getLog(getClass)
    log.info(s"Recover error in inner transaction $innerTxId (outer transaction $outerTxId)", t)
  }
}
