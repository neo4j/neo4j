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
package org.neo4j.cypher.internal.compiler.v4_0.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v4_0.helpers.AggregationHelper
import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.ir.v4_0.ProvidedOrder.{Asc, Desc}
import org.neo4j.cypher.internal.ir.v4_0.{AggregatingQueryProjection, InterestingOrder}
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v4_0.expressions.Expression

object aggregation {
  def apply(plan: LogicalPlan, aggregation: AggregatingQueryProjection, interestingOrder: InterestingOrder, context: LogicalPlanningContext): LogicalPlan = {

    val expressionSolver = PatternExpressionSolver()
    val (step1, groupingExpressions) = expressionSolver(plan, aggregation.groupingExpressions, interestingOrder, context)
    val (rewrittenPlan, aggregations) = expressionSolver(step1, aggregation.aggregationExpressions, interestingOrder, context)

    val projectionMapForLimit: Map[String, Expression] =
      if (groupingExpressions.isEmpty && aggregations.size == 1) {
        val key = aggregations.keys.head // just checked that there is only one key
        val value: Expression = aggregations(key)
        val providedOrder = context.planningAttributes.providedOrders.get(rewrittenPlan.id)

        def minFunc(x: String) = {
          providedOrder.columns.headOption match {
            case Some(Asc(`x`)) => true
            case _ => false
          }
        }
        def maxFunc(x: String) = {
          providedOrder.columns.headOption match {
            case Some(Desc(`x`)) => true
            case _ => false
          }
        }
        val shouldPlanLimit = AggregationHelper.checkMinOrMax(value, minFunc, maxFunc, false)

        if (shouldPlanLimit)
          //.head works since min and max always have only one argument
          Map(key -> value.arguments.head)
        else
          Map.empty
      } else {
        Map.empty
      }

      if (projectionMapForLimit.nonEmpty) {
        val projectedPlan = context.logicalPlanProducer.planRegularProjectionWithFakeSolved(
          rewrittenPlan,
          projectionMapForLimit,
          context
        )

        context.logicalPlanProducer.planLimitForAggregation(
          projectedPlan,
          reportedGrouping = aggregation.groupingExpressions,
          reportedAggregation = aggregation.aggregationExpressions,
          interestingOrder = interestingOrder,
          context = context
        )
      } else {
        context.logicalPlanProducer.planAggregation(
          rewrittenPlan,
          groupingExpressions,
          aggregations,
          aggregation.groupingExpressions,
          aggregation.aggregationExpressions,
          context)
      }
  }
}
