/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cypher.internal.compatibility.ClosingExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.exceptionHandler
import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription._
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, symbols}
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.v3_3.logical.plans.{LogicalPlanId, QualifiedName}
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb.{Notification, QueryExecutionType, ResourceIterator, Result}

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

  def apply(in: Result): InternalExecutionResult = {
    val internal = in.asInstanceOf[ExecutionResult].internalExecutionResult
      .asInstanceOf[ClosingExecutionResult].inner
    internal match {
      case _ => exceptionHandler.runSafely(current(internal))
    }
  }

}
