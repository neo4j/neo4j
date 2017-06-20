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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.procs

import java.io.PrintWriter
import java.util

import org.neo4j.cypher.internal.{InternalExecutionResult, QueryStatistics}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionMode
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.{InternalQueryType, StandardInternalExecutionResult}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.Result.ResultVisitor

/**
  * Empty result, as produced by a pure side-effect.
  */
case class PureSideEffectInternalExecutionResult(ctx: QueryContext,
                                                 executionPlanDescription: InternalPlanDescription,
                                                 executionType: InternalQueryType,
                                                 executionMode: ExecutionMode,
                                                 notification: Iterable[Notification] = Iterable.empty)
  extends StandardInternalExecutionResult(ctx, None, notification)
    with StandardInternalExecutionResult.IterateByAccepting {

  override def javaColumns: util.List[String] = java.util.Collections.emptyList()

  override def accept[EX <: Exception](visitor: ResultVisitor[EX]): Unit = {
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

  override def withNotifications(added: Notification*): InternalExecutionResult =
    copy(notification = notification ++ added)
}


