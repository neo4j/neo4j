/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.helpers.AggregationHelper
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.leverageOrder.OrderToLeverageWithAliases
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

object aggregation {

  /**
   * @param interestingOrderToReportForLimit the interesting order to report when planning a LIMIT for aggregation of this query part
   * @param previousInterestingOrder         the interesting order of the previous query part, if there was a previous part
   */
  def apply(
    plan: LogicalPlan,
    aggregation: AggregatingQueryProjection,
    interestingOrderToReportForLimit: InterestingOrder,
    previousInterestingOrder: Option[InterestingOrder],
    context: LogicalPlanningContext
  ): LogicalPlan = {

    val solver = SubqueryExpressionSolver.solverFor(plan, context)
    val groupingExpressionsMap = aggregation.groupingExpressions.map { case (k, v) =>
      (k, solver.solve(v, Some(k)))
    }
    val aggregations = aggregation.aggregationExpressions.map { case (k, v) => (k, solver.solve(v, Some(k))) }
    val rewrittenPlan = solver.rewrittenPlan()

    val projectionMapForLimit: Map[LogicalVariable, Expression] =
      if (AggregationHelper.isOnlyMinOrMaxAggregation(groupingExpressionsMap, aggregations)) {
        val key = aggregations.keys.head // just checked that there is only one key
        val value: Expression = aggregations(key)
        val providedOrder = context.staticComponents.planningAttributes.providedOrders.get(rewrittenPlan.id)

        def minFunc(expr: Expression) = {
          providedOrder.columns.headOption match {
            case Some(Asc(providedExpr, _)) => providedExpr == expr
            case _                          => false
          }
        }
        def maxFunc(expr: Expression) = {
          providedOrder.columns.headOption match {
            case Some(Desc(providedExpr, _)) => providedExpr == expr
            case _                           => false
          }
        }
        val shouldPlanLimit = AggregationHelper.checkMinOrMax(value, minFunc, maxFunc, false)

        if (shouldPlanLimit)
          // .head works since min and max always have only one argument
          Map(key -> value.arguments.head)
        else
          Map.empty
      } else {
        Map.empty
      }

    if (projectionMapForLimit.nonEmpty) {
      val projectedPlan = context.staticComponents.logicalPlanProducer.planRegularProjection(
        rewrittenPlan,
        projectionMapForLimit,
        None,
        context
      )

      context.staticComponents.logicalPlanProducer.planLimitForAggregation(
        projectedPlan,
        reportedGrouping = aggregation.groupingExpressions,
        reportedAggregation = aggregation.aggregationExpressions,
        interestingOrder = interestingOrderToReportForLimit,
        context = context
      )
    } else {
      val inputProvidedOrder = context.staticComponents.planningAttributes.providedOrders(plan.id)
      val OrderToLeverageWithAliases(orderToLeverageForGrouping, newGroupingExpressionsMap, newAggregationExpressions) =
        leverageOrder(inputProvidedOrder, groupingExpressionsMap, aggregations, plan.availableSymbols)

      if (orderToLeverageForGrouping.isEmpty) {
        context.staticComponents.logicalPlanProducer.planAggregation(
          rewrittenPlan,
          newGroupingExpressionsMap,
          newAggregationExpressions,
          aggregation.groupingExpressions,
          aggregation.aggregationExpressions,
          previousInterestingOrder,
          context
        )
      } else {
        context.staticComponents.logicalPlanProducer.planOrderedAggregation(
          rewrittenPlan,
          newGroupingExpressionsMap,
          newAggregationExpressions,
          orderToLeverageForGrouping,
          aggregation.groupingExpressions,
          aggregation.aggregationExpressions,
          context
        )
      }
    }
  }
}
