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

import org.neo4j.cypher.exceptionHandler.RunSafely
import org.neo4j.cypher.internal.runtime.{CloseReason, ExecutionMode, InternalExecutionResult, InternalQueryType}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.graphdb
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.{Notification, ResourceIterator}
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.QueryExecutionMonitor

/**
  * Compatibility version of `ClosingExecutionResult`, which is needed to correctly interface with
  * the 2.3 and 3.1 Cypher execution results.
  */
class CompatibilityClosingExecutionResult(val query: ExecutingQuery,
                                          val inner: CompatibilityInternalExecutionResult,
                                          runSafely: RunSafely)
                                         (implicit innerMonitor: QueryExecutionMonitor) extends InternalExecutionResult {

  self =>

  private val monitor = OnlyOnceQueryExecutionMonitor(innerMonitor)

  // Queries with no columns are queries that do not RETURN anything.
  // In these cases, it's safe to close the results eagerly
  if (inner.fieldNames().isEmpty)
    runSafely {
      closeIfEmpty()
    }

  override def initiate(): Unit = {}

  override def javaIterator: graphdb.ResourceIterator[java.util.Map[String, AnyRef]] = {
    val innerJavaIterator = inner.javaIterator

    runSafely {
      closeIfEmpty()
    }

    new graphdb.ResourceIterator[java.util.Map[String, AnyRef]] {
      def close(): Unit = runSafely {
        endQueryExecution()
        innerJavaIterator.close()
      }

      def next() = runSafely {
        val result = innerJavaIterator.next
        closeIfEmpty()
        result
      }

      def hasNext = runSafely {
        closeIfEmpty()
        innerJavaIterator.hasNext
      }

      def remove(): Unit = runSafely {
        innerJavaIterator.remove()
      }
    }
  }

  override def fieldNames() = runSafely {
    inner.fieldNames()
  }


  override def queryStatistics() = runSafely { inner.queryStatistics() }

  override def dumpToString(writer: PrintWriter): Unit = runSafely {
    inner.dumpToString(writer)
    closeIfEmpty()
  }

  override def dumpToString() = runSafely {
    val result = inner.dumpToString()
    closeIfEmpty()
    result
  }

  override def javaColumnAs[T](column: String) = runSafely {
    val _inner = inner.javaColumnAs[T](column)
    new ResourceIterator[T] {

      override def hasNext: Boolean = runSafely {
        closeIfEmpty()
        _inner.hasNext
      }

      override def next(): T = runSafely {
        val result = _inner.next()
        closeIfEmpty()
        result
      }

      override def close(): Unit = runSafely {
        _inner.close()
        endQueryExecution()
      }
    }
  }

  override def executionPlanDescription(): InternalPlanDescription =
    runSafely {
      inner.executionPlanDescription()
    }

  override def close(): Unit = runSafely {
    inner.close()
    endQueryExecution()
  }

  override def queryType: InternalQueryType = runSafely {
    inner.queryType
  }

  override def notifications: Iterable[Notification] = runSafely { inner.notifications }

  override def accept[EX <: Exception](visitor: ResultVisitor[EX]): Unit = runSafely {
    inner.accept(visitor)
    endQueryExecution()
  }

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = runSafely {
    inner.accept(visitor)
    endQueryExecution()
  }

  override def toString: String = runSafely {
    inner.toString()
  }

  private def closeIfEmpty(): Unit = {
    if (!inner.hasNext) {
      endQueryExecution()
    }
  }

  private def endQueryExecution(): Unit = {
    monitor.endSuccess(query) // this method is expected to be idempotent
  }

  override def executionMode: ExecutionMode = runSafely(inner.executionMode)

  override def isClosed: Boolean = throw new UnsupportedOperationException("This method should not be called by users of a ClosingExecutionResult")

  override def close(reason: CloseReason): Unit = throw new UnsupportedOperationException("This method should not be called by users of a ClosingExecutionResult")
}
