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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.compiler.v3_4.planner._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps.{PatternExpressionSolver, aggregation, projection, sortSkipAndLimit}
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

/*
Planning event horizons means planning the WITH clauses between query patterns. Some of these clauses are inlined
away when going from a string query to a QueryGraph. The remaining WITHs are the ones containing ORDER BY/LIMIT,
aggregation and UNWIND.
 */
case object PlanEventHorizon
  extends ((PlannerQuery, LogicalPlan, LogicalPlanningContext, Solveds, Cardinalities) => LogicalPlan) {

  override def apply(query: PlannerQuery, plan: LogicalPlan, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): LogicalPlan = {
    val selectedPlan = context.config.applySelections(plan, query.queryGraph, context, solveds, cardinalities)

    val projectedPlan = query.horizon match {
      case aggregatingProjection: AggregatingQueryProjection =>
        val aggregationPlan = aggregation(selectedPlan, aggregatingProjection, context, solveds, cardinalities)
        sortSkipAndLimit(aggregationPlan, query, context, solveds, cardinalities)

      case queryProjection: RegularQueryProjection =>
        val sortedAndLimited = sortSkipAndLimit(selectedPlan, query, context, solveds, cardinalities)
        if (queryProjection.projections.isEmpty && query.tail.isEmpty)
          context.logicalPlanProducer.planEmptyProjection(plan, context)
        else
          projection(sortedAndLimited, queryProjection.projections, context, solveds, cardinalities)

      case queryProjection: DistinctQueryProjection =>
        val projections = queryProjection.projections
        val (inner, projectionsMap) = PatternExpressionSolver()(selectedPlan, projections, context, solveds, cardinalities)
        val distinctPlan = context.logicalPlanProducer.planDistinct(inner, projectionsMap, projections, context)
        sortSkipAndLimit(distinctPlan, query, context, solveds, cardinalities)

      case UnwindProjection(variable, expression) =>
        val (inner, projectionsMap) = PatternExpressionSolver()(selectedPlan, Seq(expression), context, solveds, cardinalities)
        context.logicalPlanProducer.planUnwind(inner, variable, projectionsMap.head, expression, context)

      case ProcedureCallProjection(call) =>
        context.logicalPlanProducer.planCallProcedure(plan, call, context)

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
