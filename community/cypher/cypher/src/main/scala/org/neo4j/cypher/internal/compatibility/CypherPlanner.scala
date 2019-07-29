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
package org.neo4j.cypher.internal.compatibility

import org.neo4j.cypher.internal.compiler.phases.{LogicalPlanState, PlannerContext}
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.internal.{CypherRuntime, FullyParsedQuery, PreParsedQuery, ReusabilityState}
import org.neo4j.exceptions.Neo4jException
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.virtual.MapValue

/**
  * Cypher planner, which either parses and plans a [[PreParsedQuery]] into a [[LogicalPlanResult]] or just plans [[FullyParsedQuery]].
  */
trait CypherPlanner {

  /**
    * Compile pre-parsed query into a logical plan.
    *
    * @param preParsedQuery       pre-parsed query to convert
    * @param tracer               tracer to which events of the parsing and planning are reported
    * @param transactionalContext transactional context to use during parsing and planning
    * @throws Neo4jException public cypher exceptions on compilation problems
    * @return a logical plan result
    */
  @throws[Neo4jException]
  def parseAndPlan(preParsedQuery: PreParsedQuery,
                   tracer: CompilationPhaseTracer,
                   transactionalContext: TransactionalContext,
                   params: MapValue,
                   runtime: CypherRuntime[_]
                  ): LogicalPlanResult

  /**
    * Plan fully-parsed query into a logical plan.
    *
    * @param fullyParsedQuery     a fully-parsed query to plan
    * @param tracer               tracer to which events of the parsing and planning are reported
    * @param transactionalContext transactional context to use during parsing and planning
    * @throws Neo4jException public cypher exceptions on compilation problems
    * @return a logical plan result
    */
  @throws[Neo4jException]
  def plan(fullyParsedQuery: FullyParsedQuery,
           tracer: CompilationPhaseTracer,
           transactionalContext: TransactionalContext,
           params: MapValue,
           runtime: CypherRuntime[_]
          ): LogicalPlanResult

  def name: PlannerName
}

case class LogicalPlanResult(logicalPlanState: LogicalPlanState,
                             paramNames: Seq[String],
                             extractedParams: MapValue,
                             reusability: ReusabilityState,
                             plannerContext: PlannerContext,
                             notifications: Set[InternalNotification],
                             shouldBeCached: Boolean)

