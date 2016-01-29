/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util

import org.neo4j.cypher.internal.compatibility.{ExecutionResultWrapperFor2_3, ExecutionResultWrapperFor3_0, exceptionHandlerFor2_3, exceptionHandlerFor3_0}
import org.neo4j.cypher.internal.compiler.v2_3
import org.neo4j.cypher.internal.compiler.v3_0._
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{AcceptingExecutionResult, InternalExecutionResult, READ_WRITE, _}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.{Planner, Runtime}
import org.neo4j.cypher.internal.compiler.v3_0.spi.InternalResultVisitor
import org.neo4j.cypher.internal.frontend.v3_0.notification.InternalNotification
import org.neo4j.cypher.{ExecutionResult, InternalException}
import org.neo4j.graphdb.{QueryExecutionType, ResourceIterator}

object RewindableExecutionResult {
  private def current(inner: InternalExecutionResult, planner: PlannerName, runtime: RuntimeName): InternalExecutionResult =
    inner match {
      case other: PipeExecutionResult =>
        exceptionHandlerFor3_0.runSafely {
          new PipeExecutionResult(other.result.toEager, other.columns, other.state, other.executionPlanBuilder,
            other.executionMode, READ_WRITE) {
            override def executionPlanDescription(): InternalPlanDescription = super.executionPlanDescription()
              .addArgument(Planner(planner.name)).addArgument(Runtime(runtime.name))
          }
        }
      case other: AcceptingExecutionResult =>
        exceptionHandlerFor3_0.runSafely {
          other.toEagerIterableResult(planner, runtime)
        }

      case _ =>
        inner
    }

  private def compatibility(inner: v2_3.executionplan.InternalExecutionResult, planner: v2_3.PlannerName, runtime: v2_3.RuntimeName): InternalExecutionResult = {
    val result: v2_3.executionplan.InternalExecutionResult = inner match {
      case other: v2_3.PipeExecutionResult =>
        exceptionHandlerFor2_3.runSafely {
          new v2_3.PipeExecutionResult(other.result.toEager, other.columns, other.state, other.executionPlanBuilder,
            other.executionMode, QueryExecutionType.QueryType.READ_WRITE) {
            override def executionPlanDescription(): v2_3.planDescription.InternalPlanDescription = super.executionPlanDescription()
              .addArgument(v2_3.planDescription.InternalPlanDescription.Arguments.Planner(planner.name))
              .addArgument(v2_3.planDescription.InternalPlanDescription.Arguments.Runtime(runtime.name))
          }
        }
      case _ =>
        inner
    }
    InternalExecutionResultCompatibilityWrapperFor2_3(result)
  }

  def apply(in: ExecutionResult): InternalExecutionResult = in match {
    case ExecutionResultWrapperFor3_0(inner, planner, runtime) =>
      exceptionHandlerFor3_0.runSafely(current(inner, planner, runtime))
    case ExecutionResultWrapperFor2_3(inner, planner, runtime) =>
      exceptionHandlerFor2_3.runSafely(compatibility(inner, planner, runtime))

    case _ => throw new InternalException("Can't get the internal execution result of an older compiler")
  }

  private case class InternalExecutionResultCompatibilityWrapperFor2_3(inner: v2_3.executionplan.InternalExecutionResult) extends InternalExecutionResult {
    override def javaIterator: ResourceIterator[util.Map[String, Any]] = inner.javaIterator

    override def executionType: InternalQueryType = inner.executionType.queryType() match {
      case QueryExecutionType.QueryType.READ_ONLY => READ_ONLY
      case QueryExecutionType.QueryType.READ_WRITE => READ_WRITE
      case QueryExecutionType.QueryType.WRITE => WRITE
      case QueryExecutionType.QueryType.SCHEMA_WRITE => SCHEMA_WRITE
    }

    override def columnAs[T](column: String): Iterator[T] = inner.columnAs(column)

    override def columns: List[String] = inner.columns

    override def javaColumns: util.List[String] = inner.javaColumns

    override def queryStatistics(): InternalQueryStatistics = {
      val stats = inner.queryStatistics()
      new InternalQueryStatistics(stats.nodesCreated, stats.relationshipsCreated, stats.propertiesSet,
        stats.nodesDeleted, stats.relationshipsDeleted, stats.labelsAdded, stats.labelsRemoved,
        stats.indexesAdded, stats.indexesRemoved, stats.uniqueConstraintsAdded, stats.uniqueConstraintsRemoved,
        stats.existenceConstraintsAdded, stats.existenceConstraintsRemoved)
    }

    override def executionMode: ExecutionMode = NormalMode

    override def dumpToString(writer: PrintWriter): Unit = inner.dumpToString(writer)

    override def dumpToString(): String = inner.dumpToString()

    @throws(classOf[Exception])
    override def accept[EX <: Exception](visitor: InternalResultVisitor[EX]): Unit = ???

    override def javaColumnAs[T](column: String): ResourceIterator[T] = inner.javaColumnAs(column)

    override def executionPlanDescription(): InternalPlanDescription = ???

    override def close(): Unit = inner.close()

    override def notifications: Iterable[InternalNotification] = ???

    override def planDescriptionRequested: Boolean = inner.planDescriptionRequested

    override def next(): Map[String, Any] = inner.next()

    override def hasNext: Boolean = inner.hasNext
  }

}
