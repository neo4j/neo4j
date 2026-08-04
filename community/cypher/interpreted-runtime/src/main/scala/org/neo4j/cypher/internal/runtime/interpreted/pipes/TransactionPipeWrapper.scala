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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.RecoverableCypherError
import org.neo4j.cypher.internal.RetryableCypherError
import org.neo4j.cypher.internal.logical.plans.TransactionalPlan.RecoveryMode
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.EntityTransformer
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RetryDecision.NotApplicable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RetryDecision.NotRetryable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RetryDecision.RetryTimeout
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RetryDecision.ShouldRetry
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RetryDecision.shouldRetry
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.CypherRowEntityTransformer
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.assertTransactionStateIsEmpty
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.logError
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionRetryLogic.RetryState
import org.neo4j.cypher.internal.runtime.interpreted.profiler.InterpretedProfileInformation
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherExecutionInterruptedException
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.TransactionRetryAbortedException
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.impl.util.collection.EagerBuffer
import org.neo4j.kernel.impl.util.collection.EagerBuffer.createEagerBuffer
import org.neo4j.memory.MemoryTracker

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration
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
   * @param state     query state
   * @param outerRows outer rows, will not be closed as part of this call
   */
  def consume(state: QueryState, outerRows: TransactionBatch): TransactionResult = {
    val status = processBatch(state, outerRows.rows)(_ => ())
    status match {
      case rollback: Rollback =>
        val retry = decideRetry(rollback.failure, outerRows)
        onRollback(rollback, shouldRetry(retry))
        TransactionResult(rollback, None, retryDecision = retry)
      case _ =>
        TransactionResult(status, None, retryDecision = NotApplicable)
    }
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
    outerRows: TransactionBatch,
    memoryTracker: MemoryTracker
  ): TransactionResult = {
    val entityTransformer = new CypherRowEntityTransformer(state.query.entityTransformer)
    val innerResult = createEagerBuffer[CypherRow](memoryTracker, math.min(outerRows.rows.size(), 1024).toInt)

    val status = processBatch(state, outerRows.rows) { innerRow =>
      // Row based caching relies on the transaction state to avoid stale reads (see AbstractCachedProperty.apply).
      // Since we do not share the transaction state we must clear the cached properties.
      innerRow.invalidateCachedProperties()
      innerResult.add(entityTransformer.copyWithEntityWrappingValuesRebound(innerRow))
    }

    status match {
      case commit: Commit => TransactionResult(commit, Some(innerResult), retryDecision = NotApplicable)
      case rollback: Rollback =>
        innerResult.close()
        val retry = decideRetry(rollback.failure, outerRows)
        onRollback(rollback, retry eq ShouldRetry)
        TransactionResult(rollback, None, retryDecision = retry)
      case other =>
        innerResult.close()
        TransactionResult(other, None, retryDecision = NotApplicable)
    }
  }

  protected def processBatch(
    state: QueryState,
    outerRows: EagerBuffer[CypherRow] // Should not be closed
  )(f: CypherRow => Unit): TransactionStatus

  protected def onRollback(rollback: Rollback, shouldRetry: Boolean): Unit

  protected def shouldBreak: Boolean

  /**
   * If retries are enabled and the given throwable is a retryable error,
   * check the next retry state of the batch and decide if the batch should be retried.
   */
  protected def decideRetry(throwable: Throwable, batch: TransactionBatch): RetryDecision

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
          throw CypherExecutionInterruptedException.concurrentBatchTransactionInterrupted(this.getClass)
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

        if (innerIterator != null) {
          try {
            innerIterator.close()
          } catch {
            case t: Throwable => e.addSuppressed(t)
          }
        }

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

  override protected def onRollback(rollback: Rollback, shouldRetry: Boolean): Unit = {
    // Do nothing
  }

  override protected def shouldBreak: Boolean = false

  override protected def decideRetry(throwable: Throwable, batch: TransactionBatch): RetryDecision = NotApplicable
}

// NOTE! Keeps state that is not safe to re-use between queries. Create a new instance for each query.
class OnErrorBreakTxPipe(val outerId: Id, val inner: Pipe, val concurrentAccess: Boolean)
    extends TransactionPipeWrapper {
  @volatile private[this] var break: Boolean = false

  override protected def shouldBreak: Boolean = { break }

  override protected def decideRetry(throwable: Throwable, batch: TransactionBatch): RetryDecision = NotApplicable

  override protected def processBatch(
    state: QueryState,
    outerRows: EagerBuffer[CypherRow]
  )(f: CypherRow => Unit): TransactionStatus = {
    if (break) {
      NotRun
    } else {
      createInnerResultsInNewTransaction(state, outerRows)(f)
    }
  }

  override protected def onRollback(rollback: Rollback, shouldRetry: Boolean): Unit = {
    if (!shouldRetry) {
      break = true
    }
  }
}

class OnErrorFailTxPipe(val outerId: Id, val inner: Pipe, val concurrentAccess: Boolean)
    extends TransactionPipeWrapper {
  require(!concurrentAccess) // NOTE: We instead use OnErrorBreakTxPipe in concurrent execution

  override def shouldBreak: Boolean = false

  override def decideRetry(throwable: Throwable, batch: TransactionBatch): RetryDecision = NotApplicable

  override def processBatch(
    state: QueryState,
    outerRows: EagerBuffer[CypherRow]
  )(f: CypherRow => Unit): TransactionStatus = {
    createInnerResultsInNewTransaction(state, outerRows)(f)
  }

  override def onRollback(rollback: Rollback, shouldRetry: Boolean): Unit = {
    if (!shouldRetry) {
      throw rollback.failure
    }
  }
}

class OnErrorRetryThenContinueTxPipe(
  outerId: Id,
  inner: Pipe,
  concurrentAccess: Boolean,
  val retryLogic: TransactionRetryLogic
) extends OnErrorContinueTxPipe(outerId, inner, concurrentAccess) with OnErrorRetryTxPipe

class OnErrorRetryThenBreakTxPipe(
  outerId: Id,
  inner: Pipe,
  concurrentAccess: Boolean,
  val retryLogic: TransactionRetryLogic
) extends OnErrorBreakTxPipe(outerId, inner, concurrentAccess) with OnErrorRetryTxPipe

// this extends OnErrorBreakTxPipe on purpose because we don't want exceptions to be thrown directly
class OnErrorRetryThenFailTxPipe(
  outerId: Id,
  inner: Pipe,
  concurrentAccess: Boolean,
  val retryLogic: TransactionRetryLogic
) extends OnErrorBreakTxPipe(outerId, inner, concurrentAccess) with OnErrorRetryTxPipe

trait OnErrorRetryTxPipe {
  self: TransactionPipeWrapper =>

  def retryLogic: TransactionRetryLogic

  override def decideRetry(throwable: Throwable, batch: TransactionBatch): RetryDecision = {
    RetryableCypherError.isRetryable(throwable) match {
      case true if batch.shouldRetryAgain() =>
        ShouldRetry
      case true =>
        RetryTimeout
      case false =>
        NotRetryable
    }
  }
}

enum RetryDecision {
  case ShouldRetry, RetryTimeout, NotRetryable, NotApplicable
}

object RetryDecision {
  def shouldRetry(decision: RetryDecision): Boolean = decision eq ShouldRetry

  def decide(error: Throwable): RetryDecision = {
    if (RetryableCypherError.isRetryable(error)) {
      ShouldRetry // in the absence of an existing RetryState, always retry at least once
    } else {
      NotRetryable
    }
  }

  def decide(error: Throwable, state: RetryState): RetryDecision = {
    if (!RetryableCypherError.isRetryable(error)) {
      NotRetryable
    } else if (state.shouldRetryAgain()) {
      ShouldRetry
    } else {
      RetryTimeout
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

object RollbackFailure {

  def unapply(t: TransactionStatus): Option[Throwable] = t match {
    case Rollback(_, failure, _, _) => Some(failure)
    case _                          => None
  }
}

case object NotRun extends TransactionStatus {
  override def queryStatistics: QueryStatistics = null
  override def profileInformation: InterpretedProfileInformation = null
}

case object NonRecoverableError extends TransactionStatus {
  override def queryStatistics: QueryStatistics = null
  override def profileInformation: InterpretedProfileInformation = null
}

trait TransactionBatch {
  def rows: EagerBuffer[CypherRow]

  def close(): Unit = {
    rows.close()
  }

  def computeNextRetryState(retryLogic: TransactionRetryLogic): RetryableTransactionBatch
  def shouldRetryAgain(): Boolean
  def retriedCount: Int
  def retryTimeout: Duration

  def isMarker: Boolean = false
}

class FreshTransactionBatch(val rows: EagerBuffer[CypherRow]) extends TransactionBatch {

  override def computeNextRetryState(retryLogic: TransactionRetryLogic): RetryableTransactionBatch = {
    val firstState = retryLogic.newRetryState()
    new ImmutableRetryableTransactionBatch(rows, firstState, firstState.computeNextRetryState())
  }

  override def toString: String = s"FreshTransactionBatch(rowCount=${rows.size()})"

  override def shouldRetryAgain(): Boolean =
    // a fresh batch should always be retried at least once if retries are enabled
    true

  override def retriedCount: Int = 0

  override def retryTimeout: Duration =
    throw new UnsupportedOperationException("retryTimeout is not supported for FreshTransactionBatch")
}

trait RetryableTransactionBatch extends TransactionBatch {
  def nanosUntilRetry(): Long
  private[pipes] def retryState: RetryState
}

class ImmutableRetryableTransactionBatch(
  val rows: EagerBuffer[CypherRow],
  override private[pipes] val retryState: RetryState, // Used in the retry priority queue to compare batches scheduled for retry
  private val nextRetryState: RetryState // Used by tasks to see if we shouldRetryAgain on failure
) extends RetryableTransactionBatch {

  override def computeNextRetryState(retryLogic: TransactionRetryLogic): RetryableTransactionBatch = {
    new ImmutableRetryableTransactionBatch(rows, nextRetryState, nextRetryState.computeNextRetryState())
  }

  override def nanosUntilRetry(): Long = {
    retryState.nanosUntilRetry()
  }

  override def shouldRetryAgain(): Boolean = {
    nextRetryState.shouldRetryAgain()
  }

  override def retriedCount: Int = retryState.retryCount + 1

  override def retryTimeout: Duration =
    retryState.retryTimeout

  override def toString: String =
    s"RetryableTransactionBatch(rowCount=${rows.size()}, retryState=$retryState, nextRetryState=$nextRetryState)"
}

object RetryableTransactionBatch {

  def comparator: java.util.Comparator[RetryableTransactionBatch] =
    java.util.Comparator.comparing(_.retryState)
}

object TransactionBatch {

  def apply(rows: EagerBuffer[CypherRow]): TransactionBatch = {
    new FreshTransactionBatch(rows)
  }
}

case class TransactionResult(
  status: TransactionStatus,
  committedResults: Option[EagerBuffer[CypherRow]],
  retryDecision: RetryDecision
)

object TransactionPipeWrapper {

  /**
   * Wrap a pipeline to run in new transactions based on the specified behaviour.
   * 
   * NOTE! Implementations might keep state that is not safe to re-use between queries. Create a new instance for each query.
   */
  def apply(
    recoveryMode: RecoveryMode,
    outerId: Id,
    inner: Pipe,
    concurrentAccess: Boolean,
    retryLogic: Option[TransactionRetryLogic]
  ): TransactionPipeWrapper = {
    (recoveryMode, retryLogic) match {
      case (RecoveryMode.Continue, None) => new OnErrorContinueTxPipe(outerId, inner, concurrentAccess)
      case (RecoveryMode.Continue, Some(retryLogic)) =>
        new OnErrorRetryThenContinueTxPipe(outerId, inner, concurrentAccess, retryLogic)
      case (RecoveryMode.Break, None) => new OnErrorBreakTxPipe(outerId, inner, concurrentAccess)
      case (RecoveryMode.Break, Some(retryLogic)) =>
        new OnErrorRetryThenBreakTxPipe(outerId, inner, concurrentAccess, retryLogic)
      case (RecoveryMode.Fail, None) if concurrentAccess =>
        // NOTE: We intentionally use OnErrorBreakTxPipe for OnErrorFail in concurrent execution,
        //       since we need to send the error back to the main thread anyway.
        new OnErrorBreakTxPipe(outerId, inner, concurrentAccess)
      case (RecoveryMode.Fail, None) => new OnErrorFailTxPipe(outerId, inner, concurrentAccess)
      case (RecoveryMode.Fail, Some(retryLogic)) =>
        new OnErrorRetryThenFailTxPipe(outerId, inner, concurrentAccess, retryLogic)
    }
  }

  /**
   * Recursively finds entity wrappers and rebinds the entities to the current transaction
   */
  // TODO: Remove rebinding here, and transform wrappers to Reference:s
  // Currently, replacing e.g. NodeEntityWrappingNodeValue with NodeReference causes failures downstream.
  // We can for example end up in PathValueBuilder, which assumes that we have NodeValue and not NodeReference.
  // We can also still get entity values with transaction references streaming in and out of procedures.
  // Always copying the row should not be necessary. We could optimize this by first doing a dry-run to detect if anything actually needs to be rebound.
  class CypherRowEntityTransformer(entityTransformer: EntityTransformer) {

    def copyWithEntityWrappingValuesRebound(row: CypherRow): CypherRow =
      row.copyMapped(entityTransformer.rebindEntityWrappingValue)
  }

  def evaluateBatchSize(batchSize: Expression, state: QueryState): Long = {
    PipeHelper.evaluateStaticLongOrThrow(batchSize, 1, state, "OF ... ROWS", " Must be a positive integer.")
  }

  def evaluateConcurrency(concurrency: Option[Expression], state: QueryState): Long = {
    val concurrencyLong = concurrency match {
      case Some(c) =>
        PipeHelper.evaluateStaticLongOrThrow(
          c,
          Long.MinValue,
          state,
          "IN ... CONCURRENT TRANSACTIONS",
          ""
        )

      case None =>
        0L
    }
    val numberOfProcessors = Runtime.getRuntime.availableProcessors
    val maxConcurrency = numberOfProcessors * 20
    var effectiveConcurrency = Math.clamp(concurrencyLong, Int.MinValue, maxConcurrency)
    if (effectiveConcurrency <= 0) {
      effectiveConcurrency = Math.max(numberOfProcessors + effectiveConcurrency, 1)
    }
    effectiveConcurrency
  }

  def assertTransactionStateIsEmpty(state: QueryState): Unit = {
    if (state.query.transactionalContext.dataRead.transactionStateHasChanges)
      throw InternalException.internalError(
        this.getClass.getSimpleName,
        "Expected transaction state to be empty when calling transactional subquery."
      )
  }

  private def logError(state: QueryState, innerTxId: String, t: Throwable): Unit = {
    val outerTxId = state.query.transactionalContext.userTransactionId
    val log = state.query.logProvider.getLog(getClass)
    log.info(s"Recover error in inner transaction $innerTxId (outer transaction $outerTxId)", t)
  }

  def createRetryLogic(
    retryPolicy: TransactionRetryPolicy,
    state: QueryState
  ): Option[TransactionRetryLogic] =
    retryPolicy match {
      case TransactionRetryPolicy.RetryFor(maybeDurationInSeconds) =>
        Some(new ExponentialBackoffRetryLogic(evaluateRetryTimeoutNanos(maybeDurationInSeconds, state)))

      case TransactionRetryPolicy.ImplicitMultiVersionRetry =>
        Some(maxAttemptsRetryLogic(state))

      case TransactionRetryPolicy.DoNotRetry =>
        None
    }

  private def maxAttemptsRetryLogic(state: QueryState): MaxAttemptsRetryLogic = {
    val maxAttempts = state.query.getConfig.get(GraphDatabaseInternalSettings.snapshot_query_retries)
    MaxAttemptsRetryLogic(maxAttempts)
  }

  def evaluateRetryTimeoutNanos(retryTimeout: Option[Expression], state: QueryState): Long = {
    retryTimeout match {
      case Some(t) =>
        PipeHelper.evaluateStaticSecondsToNanosOrThrow(
          t,
          minValue = 0L,
          state,
          "FOR ... SECONDS",
          ""
        )

      case None =>
        state.query.getConfig.get(GraphDatabaseSettings.cypher_default_subquery_transaction_retry_timeout).toNanos
    }
  }

  def updateStatisticsAndProfileInformation(status: TransactionStatus, queryState: QueryState): Unit = {
    val statistics = status.queryStatistics
    if (statistics != null) {
      queryState.query.addStatistics(statistics)
    }
    val profileInformation = status.profileInformation
    if (profileInformation != null) {
      queryState.profileInformation.merge(profileInformation)
    }
  }

  def handleRetry(
    retryDecision: RetryDecision,
    resultStatus: TransactionStatus,
    recoveryMode: RecoveryMode,
    retryPolicy: TransactionRetryPolicy,
    batch: TransactionBatch
  ): (TransactionStatus, Throwable) = {
    // NOTE: It is very important that these match cases correctly propagate non-recoverable errors.
    //       ShouldRetry is intentionally not matched here: it falls through to the default case so the
    //       batch is retried by the caller.
    val explicitRetry = retryPolicy.isExplicit
    (resultStatus, retryDecision, recoveryMode) match {
      case (RollbackFailure(failure), RetryTimeout, RecoveryMode.Fail) if explicitRetry =>
        // Non-recoverable failure
        (
          resultStatus,
          TransactionRetryAbortedException.transactionRetryAborted(
            failure,
            batch.retriedCount,
            batch.retryTimeout.toUnit(TimeUnit.SECONDS)
          )
        )

      case (RollbackFailure(ex: CypherExecutionInterruptedException), _, RecoveryMode.Fail)
        if explicitRetry && ex.status() == Status.Transaction.QueryExecutionFailedOnTransaction =>
        // CypherExecutionInterruptedException means this batch was cancelled due to another batch failing,
        // so we wait for the actual failed batch to be retried in order to throw the failure
        (resultStatus, null)

      case (RollbackFailure(failure), NotRetryable | NotApplicable, RecoveryMode.Fail) if explicitRetry =>
        // Non-recoverable failure
        (resultStatus, failure)

      case (RollbackFailure(failure), NotRetryable | NotApplicable | RetryTimeout, RecoveryMode.Fail) =>
        // Non-recoverable failure. Plain or implicit-MVCC ON ERROR FAIL.
        (resultStatus, failure)

      case (r @ Rollback(_, failure, _, _), RetryTimeout, RecoveryMode.Break | RecoveryMode.Continue)
        if explicitRetry =>
        // Recoverable failure
        (
          r.copy(failure =
            TransactionRetryAbortedException.transactionRetryAborted(
              failure,
              batch.retriedCount,
              batch.retryTimeout.toUnit(TimeUnit.SECONDS)
            )
          ),
          null
        )

      case (NonRecoverableError, _, _) =>
        // Real non-recoverable exception types are not expected to be caught and handled at this level
        throw new IllegalStateException("Unexpected handling of non-recoverable error status")

      case _ =>
        (resultStatus, null)
    }
  }

}
