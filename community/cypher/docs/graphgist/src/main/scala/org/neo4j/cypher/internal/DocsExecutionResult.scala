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

import org.neo4j.cypher.internal.compatibility.{ExecutionResultWrapperFor2_2}
import org.neo4j.cypher.internal.compiler.v2_2.PipeExecutionResult
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.InternalExecutionResult
import org.neo4j.cypher.{ExtendedExecutionResult, InternalException}
import org.neo4j.graphdb.QueryExecutionType.QueryType
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.impl.query.QuerySession

object DocsExecutionResult {
  def apply(inner: InternalExecutionResult): InternalExecutionResult = inner match {
    case other: PipeExecutionResult  =>
      new PipeExecutionResult(other.result.toEager, other.columns, other.state, other.executionPlanBuilder, ProfileMode, QueryType.READ_WRITE)
    case _ =>
      inner
  }

  def apply(in: ExtendedExecutionResult)(implicit monitor: QueryExecutionMonitor, session: QuerySession): ExecutionResultWrapperFor2_2 = in match {
    case ExecutionResultWrapperFor2_2(inner, v) => ExecutionResultWrapperFor2_2(apply(inner), v)
    case _ => throw new InternalException("Can't get the internal execution result of an older compiler")
  }
}