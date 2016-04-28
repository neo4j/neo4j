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
package org.neo4j.cypher.internal.compiler.v3_1

import org.neo4j.cypher.internal.compiler.v3_1.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compiler.v3_1.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_1.InputPosition

trait CompilationOrchestrator {

  /*
  Takes a query in the form of a string, and returns a runnable ExecutionPlan. The context is used for
  communication with statistics and schema introspection
  */
  def planQuery(queryText: String, context: PlanContext, notificationLogger: InternalNotificationLogger,
                plannerName: String, offset: Option[InputPosition] = None): (ExecutionPlan, Map[String, Any])

  /*
  Takes a query half-way from text to runnable ExecutionPlan.
  This is the first half - parsing, semantic checking and AST-rewriting.
  */
  def prepareSyntacticQuery(queryText: String, rawQueryText: String, notificationLogger: InternalNotificationLogger,
                            plannerName: String, offset: Option[InputPosition] = None,
                            tracer: CompilationPhaseTracer): PreparedQuerySyntax

  /*
  Takes a query half-way from text to runnable ExecutionPlan.
  This is the second half - query planning and execution plan building/compiling.
  */
  def planPreparedQuery(syntacticQuery: PreparedQuerySyntax, context: PlanContext, offset: Option[InputPosition] = None,
                        tracer: CompilationPhaseTracer): (ExecutionPlan, Map[String, Any])
}
