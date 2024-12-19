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
import org.neo4j.cypher.internal.logical.plans.RewrittenExpressions

object aggregation {

  /**
   * @param aggregation the aggregating query projection to solve
   * @param rewrittenExpressions the expressions that have been rewritten either by a prior subquery solver or by remote batch properties
   * @param interestingOrderToReportForLimit the interesting order to report when planning a LIMIT for aggregation of this query part
   * @param previousInterestingOrder The previous interesting order, if it exists, and only if the plannerQuery has an empty query graph.
   */
  def apply(
    plan: LogicalPlan,
    aggregation: AggregatingQueryProjection,
    rewrittenExpressions: RewrittenExpressions,
    interestingOrderToReportForLimit: InterestingOrder,
    previousInterestingOrder: Option[InterestingOrder],
    context: LogicalPlanningContext
  ): LogicalPlan = {

    def toSolved(variableMap: Map[LogicalVariable, Expression]): Map[LogicalVariable, Expression] = variableMap.map {
      case (variable, expr) => variable -> rewrittenExpressions.rewrittenExpressionOrSelf(expr)
    }

    val groupingExpressionsToReport = aggregation.groupingExpressions
    val aggregationsToReport = aggregation.aggregationExpressions
    val rewrittenGroupingExprs = toSolved(groupingExpressionsToReport)
    val rewrittenAggregationExprs = toSolved(aggregationsToReport)

    val projectionMapForLimit: Map[LogicalVariable, Expression] =
      if (AggregationHelper.isOnlyMinOrMaxAggregation(rewrittenGroupingExprs, rewrittenAggregationExprs)) {
        val (key, value) = rewrittenAggregationExprs.head // just checked that there is only one key
        val providedOrder = context.staticComponents.planningAttributes.providedOrders.get(plan.id)

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
        plan,
        projectionMapForLimit,
        None,
        context
      )

      context.staticComponents.logicalPlanProducer.planLimitForAggregation(
        projectedPlan,
        reportedGrouping = groupingExpressionsToReport,
        reportedAggregation = aggregationsToReport,
        interestingOrder = interestingOrderToReportForLimit,
        context = context
      )
    } else {
      val inputProvidedOrder =
        context.staticComponents.planningAttributes.providedOrders(plan.id)
      val OrderToLeverageWithAliases(
        orderToLeverageForGrouping,
        solvedGroupExpressionsMap,
        solvedAggregationsMap
      ) =
        leverageOrder(
          inputProvidedOrder,
          rewrittenGroupingExprs,
          rewrittenAggregationExprs,
          rewrittenExpressions,
          plan.availableSymbols
        )

      // Parallel runtime does currently not support OrderedAggregation
      if (orderToLeverageForGrouping.isEmpty || !context.settings.executionModel.providedOrderPreserving) {
        context.staticComponents.logicalPlanProducer.planAggregation(
          plan,
          solvedGroupExpressionsMap,
          solvedAggregationsMap,
          groupingExpressionsToReport,
          aggregationsToReport,
          previousInterestingOrder,
          context
        )
      } else {
        context.staticComponents.logicalPlanProducer.planOrderedAggregation(
          plan,
          solvedGroupExpressionsMap,
          solvedAggregationsMap,
          orderToLeverageForGrouping,
          groupingExpressionsToReport,
          aggregationsToReport,
          context
        )
      }
    }
  }
}
