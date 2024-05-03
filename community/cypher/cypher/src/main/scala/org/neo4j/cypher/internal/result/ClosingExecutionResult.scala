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
package org.neo4j.cypher.internal.result

import org.neo4j.cypher.internal.NonFatalCypherError
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InternalQueryType
import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.internal.helpers.Exceptions
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.notifications.NotificationImplementation

/**
 * Ensures execution results are closed. This is tricky because we try to be smart about
 * closing results automatically when
 *
 *  1) all result rows have been seen through the reactive API
 *  2) any operator throws an exception
 *
 * In addition we also have special handling for suppressing exceptions thrown on close()
 * after responding to a 4).
 *
 * Finally this class report to the [[innerMonitor]] when the query is closed.
 *
 * @param query metadata about the executing query
 * @param inner the actual result
 * @param innerMonitor monitor to report closing of the query to
 */
class ClosingExecutionResult private (
  val query: ExecutingQuery,
  val inner: InternalExecutionResult,
  innerMonitor: QueryExecutionMonitor,
  subscriber: QuerySubscriber
) extends InternalExecutionResult {

  self =>
  private var errorDeliveredToSubscriber: Throwable = _

  private val monitor = OnlyOnceQueryExecutionMonitor(innerMonitor)

  override def initiate(): Unit = {
    safely { inner.initiate() }

    if (inner.isClosed) {
      monitor.beforeEnd(query, true)
      monitor.endSuccess(query)
    }
  }

  override def fieldNames(): Array[String] = safely { inner.fieldNames() }

  override def executionPlanDescription(): ExecutionPlanDescription =
    safely {
      inner.executionPlanDescription()
    }

  override def close(reason: CloseReason): Unit =
    try {
      reason match {
        case Success => monitor.beforeEnd(query, true)
        case _       => monitor.beforeEnd(query, false)
      }
      inner.close(reason)
      reason match {
        case Success  => monitor.endSuccess(query)
        case Failure  => monitor.endFailure(query, null, Failure.status)
        case Error(t) => monitor.endFailure(query, t)
      }
    } catch {
      case e: Throwable => handleErrorOnClose(reason)(e)
    }

  override def getError: Option[Throwable] = inner.getError

  override def queryType: InternalQueryType = safely { inner.queryType }

  override def notifications: Iterable[NotificationImplementation] = safely { inner.notifications }

  override def executionMode: ExecutionMode = safely { inner.executionMode }

  override def toString: String = inner.toString

  // HELPERS

  private def safely[T](body: => T): T =
    try {
      body
    } catch {
      case e: Throwable => closeAndRethrowOnError(e)
    }

  private def closeAndRethrowOnError[T](t: Throwable): T = {
    try {
      close(Error(t))
    } catch {
      case _: Throwable =>
      // ignore
    }
    throw t
  }

  private def closeAndCallOnError(t: Throwable): Unit = {
    try {
      QuerySubscriber.safelyOnError(subscriber, t)
      this.errorDeliveredToSubscriber = t
      close(Error(t))
    } catch {
      case _: Throwable =>
      // ignore
    }
  }

  private def handleErrorOnClose(closeReason: CloseReason)(thrownDuringClose: Throwable): Unit = {
    closeReason match {
      case Error(thrownBeforeClose) =>
        try {
          Exceptions.chain(thrownBeforeClose, thrownDuringClose)
        } catch {
          case _: Throwable => // Ignore
        }
        monitor.endFailure(query, thrownBeforeClose)

      case _ =>
        monitor.endFailure(query, thrownDuringClose)
    }
    throw thrownDuringClose
  }

  override def isClosed: Boolean = inner.isClosed

  override def request(numberOfRows: Long): Unit =
    try {
      inner.request(numberOfRows)
    } catch {
      case NonFatalCypherError(e) =>
        closeAndCallOnError(e)
    }

  override def cancel(): Unit =
    try {
      inner.cancel()
      if (errorDeliveredToSubscriber == null && inner.getError.isEmpty) {
        monitor.endSuccess(query)
      } else if (errorDeliveredToSubscriber != null && errorDeliveredToSubscriber.isInstanceOf[HasStatus]) {
        monitor.endFailure(query, null, errorDeliveredToSubscriber.asInstanceOf[HasStatus].status())
      } else {
        monitor.endFailure(query, null)
      }
    } catch {
      case NonFatalCypherError(e) => closeAndCallOnError(e)
    }

  override def await(): Boolean = {
    if (errorDeliveredToSubscriber != null) {
      return false
    }
    val hasMore =
      try {
        inner.await()
      } catch {
        case NonFatalCypherError(e) =>
          closeAndCallOnError(e)
          return false
      }

    if (!hasMore) {
      close()
    }
    hasMore
  }

  override def awaitCleanup(): Unit = {
    inner.awaitCleanup()
  }
}

object ClosingExecutionResult {

  def wrapAndInitiate(
    query: ExecutingQuery,
    inner: InternalExecutionResult,
    innerMonitor: QueryExecutionMonitor,
    subscriber: QuerySubscriber
  ): ClosingExecutionResult = {

    val result = new ClosingExecutionResult(query, inner, innerMonitor, subscriber)
    result.initiate()
    result
  }
}
