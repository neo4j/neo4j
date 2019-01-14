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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.procs

import java.io.PrintWriter

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.StandardInternalExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ProcedureRuntimeName
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.graphdb.Notification

/**
  * Empty result, as produced by a pure side-effect.
  */
case class PureSideEffectInternalExecutionResult(ctx: QueryContext,
                                                 executionPlanDescription: InternalPlanDescription,
                                                 queryType: InternalQueryType,
                                                 executionMode: ExecutionMode)
  extends StandardInternalExecutionResult(ctx, ProcedureRuntimeName, None)
    with StandardInternalExecutionResult.IterateByAccepting {

  override def fieldNames(): Array[String] = Array.empty

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {
    ctx.transactionalContext.close(success = true)
  }

  override def queryStatistics(): QueryStatistics = ctx.getOptStatistics.getOrElse(QueryStatistics())

  override def toList: List[Nothing] = List.empty

  override def dumpToString(writer: PrintWriter): Unit = {
    writer.println("+-------------------+")
    writer.println("| No data returned. |")
    writer.println("+-------------------+")
    writer.print(queryStatistics().toString)
  }

  override def withNotifications(added: Notification*): InternalExecutionResult = this
}


