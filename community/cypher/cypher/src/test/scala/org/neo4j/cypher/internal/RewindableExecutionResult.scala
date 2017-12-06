/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal

import java.io.PrintWriter

import org.neo4j.cypher.exceptionHandler
import org.neo4j.cypher.internal.compatibility.ClosingExecutionResult
import org.neo4j.cypher.internal.compatibility.v2_3.{ExecutionResultWrapper => ExecutionResultWrapperFor2_3, exceptionHandler => exceptionHandlerFor2_3}
import org.neo4j.cypher.internal.compatibility.v3_1.{ExecutionResultWrapper => ExecutionResultWrapperFor3_1, exceptionHandler => exceptionHandlerFor3_1}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan._
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.result.QueryResult
import org.neo4j.graphdb.{Notification, ResourceIterator, Result}

object RewindableExecutionResult {

  private def current(inner: InternalExecutionResult): InternalExecutionResult =
    inner match {
      case other: PipeExecutionResult =>
        exceptionHandler.runSafely {
          new PipeExecutionResult(other.result.toEager, other.columns.toArray, other.state, other.executionPlanBuilder,
                                  other.executionMode, READ_WRITE)
        }
      case other: StandardInternalExecutionResult =>
        exceptionHandler.runSafely {
          other.toEagerResultForTestingOnly()
        }

      case _ =>
        inner
    }

  private case class CachingExecutionResultWrapper(inner: InternalExecutionResult) extends InternalExecutionResult {
    private val cache = inner.toList
    override def toList: List[Map[String, Any]] = cache

    override def columnAs[T](column: String): Iterator[T] = inner.columnAs(column)
    override def javaColumnAs[T](column: String): ResourceIterator[T] = inner.javaColumnAs(column)
    override def javaIterator: ResourceIterator[java.util.Map[String, Any]] = inner.javaIterator
    override def dumpToString(writer: PrintWriter): Unit = inner.dumpToString(writer)
    override def dumpToString(): String = inner.dumpToString()
    override def queryStatistics(): QueryStatistics = inner.queryStatistics()
    override def planDescriptionRequested: Boolean = inner.planDescriptionRequested
    override def executionPlanDescription(): InternalPlanDescription = inner.executionPlanDescription()
    override def queryType: InternalQueryType = inner.queryType
    override def executionMode: ExecutionMode = inner.executionMode
    override def notifications: Iterable[Notification] = inner.notifications
    override def accept[E <: Exception](visitor: Result.ResultVisitor[E]): Unit = inner.accept(visitor)
    override def withNotifications(notification: Notification*): InternalExecutionResult = inner.withNotifications(notification: _*)
    override def hasNext: Boolean = inner.hasNext
    override def next(): Map[String, Any] = inner.next()
    override def fieldNames(): Array[String] = inner.fieldNames()
    override def accept[E <: Exception](visitor: QueryResult.QueryResultVisitor[E]): Unit = inner.accept(visitor)
    override def close(): Unit = inner.close()
  }

  def apply(in: Result): InternalExecutionResult = {
    val internal = in.asInstanceOf[ExecutionResult].internalExecutionResult
      .asInstanceOf[ClosingExecutionResult].inner
    apply(internal)
  }

  def apply(internal: InternalExecutionResult) : InternalExecutionResult = {
    internal match {
      case e:ExecutionResultWrapperFor3_1 =>
        exceptionHandlerFor3_1.runSafely(CachingExecutionResultWrapper(e))
      case e:ExecutionResultWrapperFor2_3 =>
        exceptionHandlerFor2_3.runSafely(CachingExecutionResultWrapper(e))
      case _ => exceptionHandler.runSafely(current(internal))
    }
  }
}
