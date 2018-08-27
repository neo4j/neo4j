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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps.replacePropertyLookupsWithVariables.firstAs
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.v3_5.logical.plans.{LogicalPlan, ResolvedCall}
import org.opencypher.v9_0.expressions.Expression
import org.opencypher.v9_0.util.InternalException

/*
Planning event horizons means planning the WITH clauses between query patterns. Some of these clauses are inlined
away when going from a string query to a QueryGraph. The remaining WITHs are the ones containing ORDER BY/LIMIT,
aggregation and UNWIND.
 */
case object PlanEventHorizon extends EventHorizonPlanner {

  override def apply(query: PlannerQuery, plan: LogicalPlan, context: LogicalPlanningContext): (LogicalPlan, LogicalPlanningContext) = {
    val selectedPlan = context.config.applySelections(plan, query.queryGraph, query.requiredOrder, context)

    val (projectedPlan, contextAfterHorizon) = query.horizon match {
      case aggregatingProjection: AggregatingQueryProjection =>
        val (aggregationPlan, newContext) = aggregation(selectedPlan, aggregatingProjection, query.requiredOrder, context)
        sortSkipAndLimit(aggregationPlan, query, newContext)

      case queryProjection: RegularQueryProjection =>
        val (sortedAndLimited, contextAfterSort) = sortSkipAndLimit(selectedPlan, query, context)
        if (queryProjection.projections.isEmpty && query.tail.isEmpty) {
          (contextAfterSort.logicalPlanProducer.planEmptyProjection(plan, contextAfterSort), contextAfterSort)
        } else {
          val (newPlan, newContext) = projection(sortedAndLimited, queryProjection.projections, queryProjection.projections, query.requiredOrder, contextAfterSort)
          (newPlan, newContext)
        }

      case queryProjection: DistinctQueryProjection =>
        val (distinctPlan, newContext) = distinct(selectedPlan, queryProjection, query.requiredOrder, context)
        sortSkipAndLimit(distinctPlan, query, newContext)

      case UnwindProjection(variable, expression) =>
        val (rewrittenExpression, newSemanticTable) = firstAs[Expression](replacePropertyLookupsWithVariables(selectedPlan.availableCachedNodeProperties)(expression, context.semanticTable))
        val newContext = context.withUpdatedSemanticTable(newSemanticTable)
        val (inner, projectionsMap) = PatternExpressionSolver()(selectedPlan, Seq(rewrittenExpression), query.requiredOrder, context)
        (newContext.logicalPlanProducer.planUnwind(inner, variable, projectionsMap.head, expression, newContext), newContext)

      case ProcedureCallProjection(call) =>
        val (rewrittenCall, newSemanticTable) = firstAs[ResolvedCall](replacePropertyLookupsWithVariables(selectedPlan.availableCachedNodeProperties)(call, context.semanticTable))
        val newContext = context.withUpdatedSemanticTable(newSemanticTable)
        (newContext.logicalPlanProducer.planCallProcedure(plan, rewrittenCall, call, newContext), newContext)

      case LoadCSVProjection(variableName, url, format, fieldTerminator) =>
        (context.logicalPlanProducer.planLoadCSV(plan, variableName, url, format, fieldTerminator, context), context)

      case PassthroughAllHorizon() =>
        (context.logicalPlanProducer.planPassAll(plan, context), context)

      case _ =>
        throw new InternalException(s"Received QG with unknown horizon type: ${query.horizon}")
    }

    // We need to check if reads introduced in the horizon conflicts with future writes
    (Eagerness.horizonReadWriteEagerize(projectedPlan, query, contextAfterHorizon), contextAfterHorizon)
  }
}
