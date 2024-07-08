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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.NonFatalCypherError
import org.neo4j.cypher.internal.result.AsyncCleanupOnClose
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.exceptions.QueryExecutionTimeoutException
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.util.VisibleForTesting

import java.util
import java.util.Collections

/**
 * This is needed for tests, because closing is usually handled in org.neo4j.cypher.internal.result.ClosingExecutionResult,
 * which we are not using here. We need to close the results to make sure that updates are committed and that cursors are closed.
 */
class ClosingRuntimeTestResult(
  inner: RuntimeResult,
  tx: Transaction,
  txContext: TransactionalContext,
  resourceManager: ResourceManager,
  subscriber: QuerySubscriber,
  assertAllReleased: () => Unit
) extends RuntimeResult {

  private var error: Throwable = _
  private var _pageCacheHits: Long = -1L
  private var _pageCacheMisses: Long = -1L
  private var _isClosed: Boolean = _

  txContext.statement().registerCloseableResource(this)

  @VisibleForTesting
  def getInner: RuntimeResult = inner

  override def fieldNames(): Array[String] = inner.fieldNames()

  override def consumptionState(): RuntimeResult.ConsumptionState = inner.consumptionState()

  override def hasServedRows: Boolean = inner.hasServedRows

  override def queryStatistics(): QueryStatistics = inner.queryStatistics()

  override def heapHighWaterMark(): Long = inner.heapHighWaterMark()

  override def queryProfile(): QueryProfile = inner.queryProfile()

  override def close(): Unit = {
    if (!_isClosed) {
      _isClosed = true
      txContext.statement().unregisterCloseableResource(this)
      inner.close()
      // TODO: Consider sharing implementation with StandardInternalExecutionResult
      inner match {
        case result: AsyncCleanupOnClose =>
          val onFinishedCallback = () => {
            closeResources()
          }
          result.registerOnFinishedCallback(onFinishedCallback)
          inner.cancel()
          try {
            inner.awaitCleanup()
          } catch {
            // For some reason await() throws the same error that has already been passed to onError()
            // in case an error has occurred on request() or cancel().
            // We need to ignore it here, _except_ when there is a timeout we are interested as it could be a
            // problem with the await itself
            case e: QueryExecutionTimeoutException =>
              throw e

            case NonFatalCypherError(e) =>
            // Ignore
          }

        case _ =>
          inner.cancel()
          closeResources()
      }
      assertAllReleased()
    }
  }

  override def request(numberOfRecords: Long): Unit = {
    try {
      inner.request(numberOfRecords)
    } catch {
      case t: Throwable =>
        this.error = t
        QuerySubscriber.safelyOnError(subscriber, t)
        try {
          close()
        } catch {
          case t2: Throwable =>
            if (!(t eq t2)) {
              t.addSuppressed(t2)
            }
            throw t
        }
    }
  }

  override def cancel(): Unit = inner.cancel()

  override def await(): Boolean = {
    if (this.error != null) {
      throw error
    }

    try {
      val moreData = inner.await()
      if (!moreData) {
        close()
      }
      moreData
    } catch {
      case t: Throwable =>
        try {
          close()
        } catch {
          case t2: Throwable =>
            if (t ne t2) {
              t.addSuppressed(t2)
            }
        }
        throw t
    }
  }

  override def notifications(): util.Set[InternalNotification] = Collections.emptySet()

  override def getErrorOrNull: Throwable = this.error

  private def closeResources(): Unit = {
    // Capture page cache statistics before closing
    if (_pageCacheHits == -1L)
      _pageCacheHits = txContext.kernelStatisticProvider().getPageCacheHits
    if (_pageCacheMisses == -1L)
      _pageCacheMisses = txContext.kernelStatisticProvider().getPageCacheMisses

    resourceManager.close()
  }

  def pageCacheHits: Long = _pageCacheHits
  def pageCacheMisses: Long = _pageCacheMisses
}
