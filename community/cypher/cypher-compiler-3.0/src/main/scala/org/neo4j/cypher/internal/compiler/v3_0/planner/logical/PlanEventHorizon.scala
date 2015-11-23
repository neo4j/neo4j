/*
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps.{projection, sortSkipAndLimit, aggregation}
import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{RelationshipCountFromCountStore, NodeCountFromCountStore, LogicalPlan}

/*
Planning event horizons means planning the WITH clauses between query patterns. Some of these clauses are inlined
away when going from a string query to a QueryGraph. The remaining WITHs are the ones containing ORDER BY/LIMIT,
aggregation and UNWIND.
 */
case class PlanEventHorizon(config: QueryPlannerConfiguration = QueryPlannerConfiguration.default)
  extends LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] {

  override def apply(query: PlannerQuery, plan: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val selectedPlan = config.applySelections(plan, query.queryGraph)
    val projectedPlan = query.horizon match {
      case aggregatingProjection: AggregatingQueryProjection =>
        val aggregationPlan = plan match {
          case n: NodeCountFromCountStore => projection(selectedPlan, aggregatingProjection.projections)
          case r: RelationshipCountFromCountStore => projection(selectedPlan, aggregatingProjection.projections)
          case _ => aggregation(selectedPlan, aggregatingProjection)
        }
        sortSkipAndLimit(aggregationPlan, query)

      case queryProjection: RegularQueryProjection =>
        val sortedAndLimited = sortSkipAndLimit(selectedPlan, query)
        if (queryProjection.projections.isEmpty && query.tail.isEmpty)
          context.logicalPlanProducer.planEmptyProjection(plan)
        else
          projection(sortedAndLimited, queryProjection.projections)

      case UnwindProjection(variable, expression) =>
        context.logicalPlanProducer.planUnwind(plan, variable, expression)

      case _ =>
        throw new CantHandleQueryException
    }

    projectedPlan
  }
}
