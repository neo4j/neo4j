/**
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

import java.util

import org.neo4j.cypher.internal.compatibility.{ExecutionResultWrapperFor2_3, exceptionHandlerFor2_3}
import org.neo4j.cypher.internal.compiler.v2_3.PipeExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{CompiledExecutionResult, InternalExecutionResult}
import org.neo4j.cypher.{ExecutionResult, InternalException}
import org.neo4j.graphdb.QueryExecutionType.QueryType
import org.neo4j.graphdb.Result.ResultVisitor

object RewindableExecutionResult {
  self =>
  def apply(inner: InternalExecutionResult): InternalExecutionResult = inner match {
    case other: PipeExecutionResult  =>
      exceptionHandlerFor2_3.runSafely {
        new PipeExecutionResult(other.result.toEager, other.columns, other.state, other.executionPlanBuilder, other.executionMode, QueryType.READ_WRITE)
      }
    case other: CompiledExecutionResult  =>
      exceptionHandlerFor2_3.runSafely {
        new CompiledExecutionResult {
          override def javaColumns: util.List[String] = other.javaColumns
          override val toList = other.toList
          override def accept(visitor: ResultVisitor): Unit = other.accept(visitor)
        }
      }
    case _ =>
      inner
  }

  def apply(in: ExecutionResult): InternalExecutionResult = in match {
    case ExecutionResultWrapperFor2_3(inner, _, _) => exceptionHandlerFor2_3.runSafely(apply(inner))

    case _                                      => throw new InternalException("Can't get the internal execution result of an older compiler")
  }
}
