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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps._
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_5.util.InternalException

/*
Planning event horizons means planning the WITH clauses between query patterns. Some of these clauses are inlined
away when going from a string query to a QueryGraph. The remaining WITHs are the ones containing ORDER BY/LIMIT,
aggregation and UNWIND.
 */
case object PlanEventHorizon extends EventHorizonPlanner {

  override def apply(query: PlannerQuery, plan: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val selectedPlan = context.config.applySelections(plan, query.queryGraph, query.interestingOrder, context)

    val projectedPlan = query.horizon match {
      case aggregatingProjection: AggregatingQueryProjection =>
        val aggregationPlan = aggregation(selectedPlan, aggregatingProjection, query.interestingOrder, context)
        // aggregation is the only case where sort happens after the projection. The provided order of the aggretion plan will include
        // renames of the projection, thus we need to rename this as well for the required order before considering planning a sort.
        val interestingOrderWithRenames = query.interestingOrder.withProjectedColumns(aggregatingProjection.groupingExpressions)
        val sorted = sortSkipAndLimit(aggregationPlan, query, interestingOrderWithRenames, context)
        if (aggregatingProjection.selections.isEmpty) {
          sorted
        } else {
          val predicates = aggregatingProjection.selections.flatPredicates
          val (rewrittenSorted, rewrittenPredicates) = PatternExpressionSolver()(sorted, predicates, query.interestingOrder, context)
          context.logicalPlanProducer.planHorizonSelection(rewrittenSorted, rewrittenPredicates, predicates, context)
        }

      case regularProjection: RegularQueryProjection =>
        val sortedAndLimited = sortSkipAndLimit(selectedPlan, query, query.interestingOrder, context)
        val projected =
          if (regularProjection.projections.isEmpty && query.tail.isEmpty) {
            context.logicalPlanProducer.planEmptyProjection(plan, context)
          } else {
            projection(sortedAndLimited, regularProjection.projections, regularProjection.projections, query.interestingOrder, context)
          }
        if (regularProjection.selections.isEmpty) {
          projected
        } else {
          val predicates = regularProjection.selections.flatPredicates
          val (rewrittenProjected, rewrittenPredicates) = PatternExpressionSolver()(projected, predicates, query.interestingOrder, context)
          context.logicalPlanProducer.planHorizonSelection(rewrittenProjected, rewrittenPredicates, predicates, context)
        }

      case distinctProjection: DistinctQueryProjection =>
        val distinctPlan = distinct(selectedPlan, distinctProjection, query.interestingOrder, context)
        val interestingOrderWithRenames = query.interestingOrder.withProjectedColumns(distinctProjection.groupingKeys)
        val sorted = sortSkipAndLimit(distinctPlan, query, interestingOrderWithRenames, context)
        if (distinctProjection.selections.isEmpty) {
          sorted
        } else {
          val predicates = distinctProjection.selections.flatPredicates
          val (rewrittenSorted, rewrittenPredicates) = PatternExpressionSolver()(sorted, predicates, query.interestingOrder, context)
          context.logicalPlanProducer.planHorizonSelection(rewrittenSorted, rewrittenPredicates, predicates, context)
        }

      case UnwindProjection(variable, expression) =>
        val (inner, projectionsMap) = PatternExpressionSolver()(selectedPlan, Seq(expression), query.interestingOrder, context)
        context.logicalPlanProducer.planUnwind(inner, variable, projectionsMap.head, expression, context)

      case ProcedureCallProjection(call) =>
        context.logicalPlanProducer.planCallProcedure(plan, call, call, context)

      case LoadCSVProjection(variableName, url, format, fieldTerminator) =>
        context.logicalPlanProducer.planLoadCSV(plan, variableName, url, format, fieldTerminator, context)

      case PassthroughAllHorizon() =>
        context.logicalPlanProducer.planPassAll(plan, context)

      case _ =>
        throw new InternalException(s"Received QG with unknown horizon type: ${query.horizon}")
    }

    // We need to check if reads introduced in the horizon conflicts with future writes
    Eagerness.horizonReadWriteEagerize(projectedPlan, query, context)
  }
}
