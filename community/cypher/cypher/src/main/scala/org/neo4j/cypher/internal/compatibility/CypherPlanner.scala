/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility

import org.neo4j.cypher.CypherException
import org.neo4j.cypher.internal.{PreParsedQuery, ReusabilityState}
import org.neo4j.cypher.internal.compiler.v3_5.phases.{LogicalPlanState, PlannerContext}
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer

/**
  * Cypher planner, which parses and plans pre-parsed queries into a logical plan.
  */
trait CypherPlanner {

  /**
    * Compile pre-parsed query into a logical plan.
    *
    * @param preParsedQuery pre-parsed query to convert
    * @param tracer tracer to which events of the parsing and planning are reported
    * @param preParsingNotifications notifications from pre-parsing
    * @param transactionalContext transactional context to use during parsing and planning
    * @throws CypherException public cypher exceptions on compilation problems
    * @return a logical plan result
    */
  @throws[org.neo4j.cypher.CypherException]
  def parseAndPlan(preParsedQuery: PreParsedQuery,
                   tracer: CompilationPhaseTracer,
                   preParsingNotifications: Set[org.neo4j.graphdb.Notification],
                   transactionalContext: TransactionalContext
                  ): (LogicalPlanResult, PlannerContext)
}

case class LogicalPlanResult(logicalPlanState: LogicalPlanState,
                             paramNames: Seq[String],
                             extractedParams: MapValue,
                             reusability: ReusabilityState)

