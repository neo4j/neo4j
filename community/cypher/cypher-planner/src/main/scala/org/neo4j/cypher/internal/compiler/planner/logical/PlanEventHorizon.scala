/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner._
import org.neo4j.cypher.internal.compiler.planner.logical.steps._
import org.neo4j.cypher.internal.ir._
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.exceptions.InternalException

/*
Planning event horizons means planning the WITH clauses between query patterns. Some of these clauses are inlined
away when going from a string query to a QueryGraph. The remaining WITHs are the ones containing ORDER BY/LIMIT,
aggregation and UNWIND.
 */
case object PlanEventHorizon extends EventHorizonPlanner {

  override def apply(query: SinglePlannerQuery, plan: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val selectedPlan = context.config.applySelections(plan, query.queryGraph, query.interestingOrder, context)

    val projectedPlan = query.horizon match {
      case aggregatingProjection: AggregatingQueryProjection =>
        val aggregationPlan = aggregation(selectedPlan, aggregatingProjection, query.interestingOrder, context)
        // aggregation is the only case where sort happens after the projection. The provided order of the aggretion plan will include
        // renames of the projection, thus we need to rename this as well for the required order before considering planning a sort.
        val sorted = SortPlanner.ensureSortedPlanWithSolved(aggregationPlan, query.interestingOrder, context)
        val limited = skipAndLimit(sorted, query, context)
        if (aggregatingProjection.selections.isEmpty) {
          limited
        } else {
          val predicates = aggregatingProjection.selections.flatPredicates
          context.logicalPlanProducer.planHorizonSelection(limited, predicates, query.interestingOrder, context)
        }

      case regularProjection: RegularQueryProjection =>
        val sorted = SortPlanner.ensureSortedPlanWithSolved(selectedPlan, query.interestingOrder, context)
        val limited = skipAndLimit(sorted, query, context)
        val projected =
          if (regularProjection.projections.isEmpty && query.tail.isEmpty) {
            context.logicalPlanProducer.planEmptyProjection(plan, context)
          } else {
            projection(limited, regularProjection.projections, regularProjection.projections, query.interestingOrder, context)
          }
        if (regularProjection.selections.isEmpty) {
          projected
        } else {
          val predicates = regularProjection.selections.flatPredicates
          context.logicalPlanProducer.planHorizonSelection(projected, predicates, query.interestingOrder, context)
        }

      case distinctProjection: DistinctQueryProjection =>
        val distinctPlan = distinct(selectedPlan, distinctProjection, query.interestingOrder, context)
        val sorted = SortPlanner.ensureSortedPlanWithSolved(distinctPlan, query.interestingOrder, context)
        val limited = skipAndLimit(sorted, query, context)
        if (distinctProjection.selections.isEmpty) {
          limited
        } else {
          val predicates = distinctProjection.selections.flatPredicates
          context.logicalPlanProducer.planHorizonSelection(limited, predicates, query.interestingOrder, context)
        }

      case UnwindProjection(variable, expression) =>
        val projected = context.logicalPlanProducer.planUnwind(selectedPlan, variable, expression, query.interestingOrder, context)
        SortPlanner.ensureSortedPlanWithSolved(projected, query.interestingOrder, context)

      case ProcedureCallProjection(call) =>
        val projected = context.logicalPlanProducer.planCallProcedure(plan, call, query.interestingOrder, context)
        SortPlanner.ensureSortedPlanWithSolved(projected, query.interestingOrder, context)

      case LoadCSVProjection(variableName, url, format, fieldTerminator) =>
        val projected = context.logicalPlanProducer.planLoadCSV(plan, variableName, url, format, fieldTerminator, query.interestingOrder, context)
        SortPlanner.ensureSortedPlanWithSolved(projected, query.interestingOrder, context)

      case PassthroughAllHorizon() =>
        val projected = context.logicalPlanProducer.planPassAll(plan, context)
        SortPlanner.ensureSortedPlanWithSolved(projected, query.interestingOrder, context)

      case CallSubqueryHorizon(callSubquery) =>
        val (subPlan, _) =  plannerQueryPartPlanner.plan(callSubquery, context)
        context.logicalPlanProducer.planSubqueryCartesianProduct(plan, subPlan, context)

      case _ =>
        throw new InternalException(s"Received QG with unknown horizon type: ${query.horizon}")
    }

    // We need to check if reads introduced in the horizon conflicts with future writes
    Eagerness.horizonReadWriteEagerize(projectedPlan, query, context)
  }
}
