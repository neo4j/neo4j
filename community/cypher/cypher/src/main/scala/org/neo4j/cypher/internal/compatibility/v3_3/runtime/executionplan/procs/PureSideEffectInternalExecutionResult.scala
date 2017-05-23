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

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionMode, InternalQueryStatistics}
import org.neo4j.cypher.internal.compiler.v3_3.executionplan.{InternalQueryType, StandardInternalExecutionResult}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_3.spi.InternalResultVisitor
import org.neo4j.cypher.internal.spi.v3_3.QueryContext

/**
  * Empty result, as produced by a pure side-effect.
  */
case class PureSideEffectInternalExecutionResult(ctx: QueryContext,
                                                 executionPlanDescription: InternalPlanDescription,
                                                 executionType: InternalQueryType,
                                                 executionMode: ExecutionMode)
  extends StandardInternalExecutionResult(ctx)
    with StandardInternalExecutionResult.IterateByAccepting {

    override def javaColumns: util.List[String] = java.util.Collections.emptyList()

    override def accept[EX <: Exception](visitor: InternalResultVisitor[EX]) = {
      ctx.transactionalContext.close(success = true)
    }

    override def queryStatistics() = ctx.getOptStatistics.getOrElse(InternalQueryStatistics())

    override def toList = List.empty

    override def dumpToString(writer: PrintWriter) = {
      writer.println("+-------------------+")
      writer.println("| No data returned. |")
      writer.println("+-------------------+")
      writer.print(queryStatistics().toString)
    }
  }


