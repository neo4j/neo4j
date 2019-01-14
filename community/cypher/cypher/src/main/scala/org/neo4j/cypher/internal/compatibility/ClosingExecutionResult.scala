/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility

import java.io.PrintWriter
import java.util
import java.util.NoSuchElementException

import org.neo4j.cypher.exceptionHandler.RunSafely
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.graphdb
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.{Notification, ResourceIterator}
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.QueryExecutionMonitor

/**
  * Ensures execution results are closed. This is tricky because we try to be smart about
  * closing results automatically when
  *
  *  1) all result rows have been seen through iterator
  *  2) all result rows have been seen through visitor
  *  3) all result rows have been seen through dumpToString
  *  4) any operator throws an exception
  *
  * In addition we also have special handling for suppressing exceptions thrown on close()
  * after responding to a 4).
  *
  * Finally this class report to the [[innerMonitor]] when the query is closed.
  *
  * @param query metadata about the executing query
  * @param inner the actual result
  * @param runSafely RunSafely which converts any exception into the public exception space (subtypes of org.neo4j.cypher.CypherException)
  * @param innerMonitor monitor to report closing of the query to
  */
class ClosingExecutionResult private(val query: ExecutingQuery,
                                     val inner: InternalExecutionResult,
                                     runSafely: RunSafely,
                                     innerMonitor: QueryExecutionMonitor) extends InternalExecutionResult {

  self =>

  private val monitor = OnlyOnceQueryExecutionMonitor(innerMonitor)

  override def initiate(): Unit = {
    safely { inner.initiate() }

    if (inner.isClosed)
      monitor.endSuccess(query)
  }

  override def javaIterator: graphdb.ResourceIterator[java.util.Map[String, AnyRef]] = {
    safely {
      val innerIterator = inner.javaIterator
      closeIfEmpty(innerIterator)

      new graphdb.ResourceIterator[java.util.Map[String, AnyRef]] {
        def next(): util.Map[String, AnyRef] = safely {
          if (inner.isClosed) throw new NoSuchElementException
          else {
            val result = innerIterator.next
            closeIfEmpty(innerIterator)
            result
          }
        }

        def hasNext: Boolean = safely {
          if (inner.isClosed) false
          else {
            closeIfEmpty(innerIterator)
            innerIterator.hasNext
          }
        }

        def close(): Unit = self.close()

        def remove(): Unit = safely {
          innerIterator.remove()
        }
      }
    }
  }

  override def fieldNames(): Array[String] = safely { inner.fieldNames() }

  override def queryStatistics(): QueryStatistics = safely { inner.queryStatistics() }

  override def dumpToString(writer: PrintWriter): Unit = safelyAndClose { inner.dumpToString(writer) }

  override def dumpToString(): String = safelyAndClose { inner.dumpToString() }

  override def javaColumnAs[T](column: String): ResourceIterator[T] =
    safely {
      val _inner = inner.javaColumnAs[T](column)
      new ResourceIterator[T] {

        override def hasNext: Boolean =
          safely {
            closeIfEmpty(_inner)
            _inner.hasNext
          }

        override def next(): T =
          safely {
            val result = _inner.next()
            closeIfEmpty(_inner)
            result
          }

        override def close(): Unit = self.close()
      }
    }

  override def executionPlanDescription(): InternalPlanDescription =
    safely {
      inner.executionPlanDescription()
    }

  override def close(reason: CloseReason): Unit = runSafely({
    inner.close(reason)
    reason match {
      case Success => monitor.endSuccess(query)
      case Failure => monitor.endFailure(query, null)
      case Error(t) => monitor.endFailure(query, t)
    }
  })( handleErrorOnClose(reason) )

  override def queryType: InternalQueryType = safely { inner.queryType }

  override def notifications: Iterable[Notification] = safely { inner.notifications }

  override def accept[EX <: Exception](visitor: ResultVisitor[EX]): Unit =
    safelyAndClose {
      inner.accept(visitor)
    }

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit =
    safelyAndClose {
      inner.accept(visitor)
    }

  override def executionMode: ExecutionMode = safely { inner.executionMode }

  override def toString: String = runSafely { inner.toString }

  // HELPERS

  private def safely[T](body: => T): T = runSafely(body)(closeOnError)

  private def safelyAndClose[T](body: => T): T =
    runSafely({
      val x = body
      close(Success)
      x
    })(closeOnError)

  private def closeIfEmpty(iterator: java.util.Iterator[_]): Unit =
    if (!iterator.hasNext) {
      close(Success)
    }

  private def closeOnError(t: Throwable): Unit = {
    try {
      close(Error(t))
    } catch {
      case thrownDuringClose: Throwable =>
        // ignore
    }
  }

  private def handleErrorOnClose(closeReason: CloseReason)(thrownDuringClose: Throwable): Unit =
    closeReason match {
      case Error(thrownBeforeClose) =>
        try {
          thrownBeforeClose.addSuppressed(thrownDuringClose)
        } catch {
          case _: Throwable => // Ignore
        }
        monitor.endFailure(query, thrownBeforeClose)

      case _ =>
        monitor.endFailure(query, thrownDuringClose)
    }

  override def isClosed: Boolean = inner.isClosed
}

object ClosingExecutionResult {
  def wrapAndInitiate(query: ExecutingQuery,
                      inner: InternalExecutionResult,
                      runSafely: RunSafely,
                      innerMonitor: QueryExecutionMonitor): ClosingExecutionResult = {

    val result = new ClosingExecutionResult(query, inner, runSafely, innerMonitor)
    result.initiate()
    result
  }
}
