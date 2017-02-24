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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical

import org.neo4j.cypher.internal.compiler.v3_2.planner._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps.{aggregation, projection, sortSkipAndLimit}
import org.neo4j.cypher.internal.ir.v3_2._
import org.neo4j.cypher.internal.ir.v3_2.exception.CantHandleQueryException

/*
Planning event horizons means planning the WITH clauses between query patterns. Some of these clauses are inlined
away when going from a string query to a QueryGraph. The remaining WITHs are the ones containing ORDER BY/LIMIT,
aggregation and UNWIND.
 */
case object PlanEventHorizon
  extends LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] {

  override def apply(query: PlannerQuery, plan: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val selectedPlan = context.config.applySelections(plan, query.queryGraph)

    val projectedPlan = query.horizon match {
      case aggregatingProjection: AggregatingQueryProjection =>
        val aggregationPlan = aggregation(selectedPlan, aggregatingProjection)
        sortSkipAndLimit(aggregationPlan, query)

      case queryProjection: RegularQueryProjection =>
        val sortedAndLimited = sortSkipAndLimit(selectedPlan, query)
        if (queryProjection.projections.isEmpty && query.tail.isEmpty)
          context.logicalPlanProducer.planEmptyProjection(plan)
        else
          projection(sortedAndLimited, queryProjection.projections)

      case UnwindProjection(variable, expression) =>
        context.logicalPlanProducer.planUnwind(plan, variable, expression)

      case ProcedureCallProjection(call) =>
        context.logicalPlanProducer.planCallProcedure(plan, call)

      case LoadCSVProjection(variableName, url, format, fieldTerminator) =>
        context.logicalPlanProducer.planLoadCSV(plan, variableName, url, format, fieldTerminator)

      case PassthroughAllHorizon() =>
        context.logicalPlanProducer.planPassAll(plan)

      case _ =>
        throw new CantHandleQueryException
    }

    // We need to check if reads introduced in the horizon conflicts with future writes
    Eagerness.horizonReadWriteEagerize(projectedPlan, query)
  }
}
