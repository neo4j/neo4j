/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.cypher.{InternalException, ExecutionResult}
import org.neo4j.cypher.internal.compatability.ExecutionResultWrapperFor2_2
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_2.{EagerPipeExecutionResult, PipeExecutionResult}

object RewindableExecutionResult {
  def apply(inner: InternalExecutionResult): InternalExecutionResult = inner match {
    case _: EagerPipeExecutionResult => inner
    case other: PipeExecutionResult  =>
      new EagerPipeExecutionResult(other.result, other.columns, other.state, other.executionPlanBuilder, other.planType)
    case _                           => inner
  }

  def apply(in: ExecutionResult): InternalExecutionResult = in match {
    case ExecutionResultWrapperFor2_2(inner) => apply(inner)
    case _                                   => throw new InternalException("Can't get the internal execution result of an older compiler")
  }
}
