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
package org.neo4j.cypher.internal

import org.neo4j.cypher.exceptionHandler
import org.neo4j.cypher.internal.compatibility.ClosingExecutionResult
import org.neo4j.cypher.internal.compatibility.v2_3.{ExecutionResultWrapper => ExecutionResultWrapperFor2_3, exceptionHandler => exceptionHandlerFor2_3}
import org.neo4j.cypher.internal.compatibility.v3_1.{ExecutionResultWrapper => ExecutionResultWrapperFor3_1, exceptionHandler => exceptionHandlerFor3_1}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan._
import org.neo4j.cypher.internal.compiler.{v2_3, v3_1}
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.runtime._
import org.neo4j.graphdb.{QueryExecutionType, Result}

object RewindableExecutionResult {

  private def eagerize(inner: InternalExecutionResult): InternalExecutionResult =
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

  private def eagerize(inner: v2_3.executionplan.InternalExecutionResult): v2_3.executionplan.InternalExecutionResult = {
    inner match {
      case other: v2_3.PipeExecutionResult =>
        exceptionHandlerFor2_3.runSafely {
          new v2_3.PipeExecutionResult(other.result.toEager, other.columns, other.state, other.executionPlanBuilder,
            other.executionMode, QueryExecutionType.QueryType.READ_WRITE)
        }
      case other: v2_3.ExplainExecutionResult =>
        v2_3.ExplainExecutionResult(other.columns, other.executionPlanDescription, other.executionType.queryType(), other.notifications)
      case _ =>
        inner
    }
  }

  private def eagerize(inner: v3_1.executionplan.InternalExecutionResult): v3_1.executionplan.InternalExecutionResult = {
    inner match {
      case other: v3_1.PipeExecutionResult =>
        exceptionHandlerFor3_1.runSafely {
          new v3_1.PipeExecutionResult(other.result.toEager, other.columns, other.state, other.executionPlanBuilder,
            other.executionMode, v3_1.executionplan.READ_WRITE)
        }
      case other: v3_1.ExplainExecutionResult =>
        v3_1.ExplainExecutionResult(other.columns, other.executionPlanDescription, other.executionType, other.notifications)
      case _ =>
        inner
    }
  }

  private class CachingExecutionResultWrapperFor3_1(inner: v3_1.executionplan.InternalExecutionResult,
                                                    planner: v3_1.PlannerName,
                                                    runtime: v3_1.RuntimeName,
                                                    preParsingNotification: Set[org.neo4j.graphdb.Notification],
                                                    offset: Option[frontend.v3_1.InputPosition])
    extends ExecutionResultWrapperFor3_1(inner, planner, runtime, preParsingNotification, offset) {
    val cache: List[Map[String, Any]] = inner.toList
    override val toList: List[Map[String, Any]] = cache
  }

  private class CachingExecutionResultWrapperFor2_3(inner: v2_3.executionplan.InternalExecutionResult,
                                                    planner: v2_3.PlannerName,
                                                    runtime: v2_3.RuntimeName,
                                                    preParsingNotification: Set[org.neo4j.graphdb.Notification],
                                                    offset: Option[frontend.v2_3.InputPosition])
    extends ExecutionResultWrapperFor2_3(inner, planner, runtime, preParsingNotification, offset) {
    val cache: List[Map[String, Any]] = inner.toList
    override val toList: List[Map[String, Any]] = cache
  }

  def apply(in: Result): InternalExecutionResult = {
    val internal = in.asInstanceOf[ExecutionResult].internalExecutionResult
      .asInstanceOf[ClosingExecutionResult].inner
    apply(internal)
  }

  def apply(internal: InternalExecutionResult) : InternalExecutionResult = {
    internal match {
      case ExecutionResultWrapperFor3_1(inner, planner, runtime, preParsingNotification, offset) =>
        val wrapper = new CachingExecutionResultWrapperFor3_1(eagerize(inner), planner, runtime, preParsingNotification, offset)
        exceptionHandlerFor3_1.runSafely(wrapper)
      case ExecutionResultWrapperFor2_3(inner, planner, runtime, preParsingNotification, offset) =>
        val wrapper = new CachingExecutionResultWrapperFor2_3(eagerize(inner), planner, runtime, preParsingNotification, offset)
        exceptionHandlerFor2_3.runSafely(wrapper)
      case _ => exceptionHandler.runSafely(eagerize(internal))
    }
  }
}
