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
package org.neo4j.cypher.internal.compiler.v4_0.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.ir.v4_0.{AggregatingQueryProjection, InterestingOrder}
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.InputPosition

object aggregation {
  def apply(plan: LogicalPlan, aggregation: AggregatingQueryProjection, interestingOrder: InterestingOrder, context: LogicalPlanningContext): LogicalPlan = {

    val expressionSolver = PatternExpressionSolver()
    val (step1, groupingExpressions) = expressionSolver(plan, aggregation.groupingExpressions, interestingOrder, context)
    val (rewrittenPlan, aggregations) = expressionSolver(step1, aggregation.aggregationExpressions, interestingOrder, context)

    val usingInterestingOrder: Boolean = interestingOrder.required.nonEmpty && interestingOrder.satisfiedBy(context.planningAttributes.providedOrders.get(rewrittenPlan.id))
    if (usingInterestingOrder) {
      val projectionMap =
        aggregations.keys.foldLeft(Map.empty[String, Expression]) {
          case (_projectionMap, key) =>
            val value: Expression = aggregations(key)
            if (isMinOrMax(value))
              //.head works since min and max always have only one argument
              _projectionMap ++ Map(key -> value.arguments.head)
            else
              _projectionMap
        }

      if (projectionMap.nonEmpty) {
        val projectedPlan = projection(rewrittenPlan, projectionMap, Map.empty, interestingOrder, context, solve = false)
        val limitedPlan = context.logicalPlanProducer.planLimitWithFakeSolved(projectedPlan,
          SignedDecimalIntegerLiteral("1")(InputPosition.NONE), context = context)

        context.logicalPlanProducer.updateSolvedForMinOrMax(
          limitedPlan,
          aggregation.groupingExpressions,
          aggregation.aggregationExpressions,
          interestingOrder,
          context
        )
      } else {
        context.logicalPlanProducer.planAggregation(
          rewrittenPlan,
          groupingExpressions,
          aggregations,
          aggregation.groupingExpressions,
          aggregation.aggregationExpressions,
          context,
          None)
      }
    } else {
      val keepOrder = aggregations.values.foldLeft(false) {
        case (_keepOrder, value) =>
          _keepOrder || isMinOrMax(value)
      }
      val interestingOrderForAggregation =
        if (keepOrder)
          Some(interestingOrder)
        else
          None

      context.logicalPlanProducer.planAggregation(
        rewrittenPlan,
        groupingExpressions,
        aggregations,
        aggregation.groupingExpressions,
        aggregation.aggregationExpressions,
        context,
        interestingOrderForAggregation)
    }
  }

  private def isMinOrMax(aggregation: Expression): Boolean = {
    aggregation match {
      case f: FunctionInvocation => f.name == "min" || f.name == "max"
      case _ => false
    }
  }
}
