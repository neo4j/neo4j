/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.compatibility.{ExecutionResultWrapperFor3_0, exceptionHandlerFor3_0}
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{CompiledExecutionResult, InternalExecutionResult}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.{Planner, Runtime}
import org.neo4j.cypher.internal.compiler.v3_0.{PipeExecutionResult, PlannerName, RuntimeName}
import org.neo4j.cypher.{ExecutionResult, InternalException}
import org.neo4j.graphdb.QueryExecutionType.QueryType

object RewindableExecutionResult {
  self =>

  def apply(inner: InternalExecutionResult, planner: PlannerName, runtime: RuntimeName): InternalExecutionResult =
    inner match {
      case other: PipeExecutionResult =>
        exceptionHandlerFor3_0.runSafely {
          new PipeExecutionResult(other.result.toEager, other.columns, other.state, other.executionPlanBuilder,
            other.executionMode, QueryType.READ_WRITE) {
            override def executionPlanDescription(): InternalPlanDescription = super.executionPlanDescription()
                                                                               .addArgument(Planner(planner.name)).addArgument(Runtime(runtime.name))
          }
        }
      case other: CompiledExecutionResult =>
        exceptionHandlerFor3_0.runSafely {
          other.toEagerIterableResult(planner, runtime)
        }

      case _ =>
        inner
    }

  def apply(in: ExecutionResult): InternalExecutionResult = in match {
    case e@ExecutionResultWrapperFor3_0(inner, _, _) => exceptionHandlerFor3_0
      .runSafely(apply(inner, e.planner, e.runtime))

    case _ => throw new InternalException("Can't get the internal execution result of an older compiler")
  }
}
