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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.DistinctQueryProjection
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

object distinct {
  def apply(plan: LogicalPlan, distinctQueryProjection: DistinctQueryProjection, context: LogicalPlanningContext): LogicalPlan = {

    val solver = PatternExpressionSolver.solverFor(plan, context)
    val groupingExpressionsMap = distinctQueryProjection.groupingExpressions.map{ case (k,v) => (k, solver.solve(v, Some(k))) }
    val rewrittenPlan = solver.rewrittenPlan()

    // Collect aliases for all grouping expressions which project a variable that is already an available symbol
    val aliasMap: Map[Expression, Variable] = groupingExpressionsMap.collect {
      case (k, expr) if plan.availableSymbols.contains(k) => (expr, Variable(k)(expr.position))
    }

    // When we can read variables instead of expressions in distinct, we should do that.
    // The new grouping expressions map contains aliases as values, where available.
    val newGroupingExpressionsMap = groupingExpressionsMap.map {
      case (k, expr) => (k, aliasMap.getOrElse(expr, expr))
    }

    val orderToLeverage = {
      val inputProvidedOrder = context.planningAttributes.providedOrders(plan.id)
      // To find out if there are order columns of which we can leverage the order, we have to use the same aliases in the provided order.
      val aliasedInputProvidedOrder = inputProvidedOrder.mapColumns {
        case c @ ProvidedOrder.Asc(expression) => c.copy(aliasMap.getOrElse(expression, expression))
        case c @ ProvidedOrder.Desc(expression) => c.copy(aliasMap.getOrElse(expression, expression))
      }

      leverageOrder(aliasedInputProvidedOrder, newGroupingExpressionsMap.values.toSet)
    }

    if (orderToLeverage.isEmpty) {
      context.logicalPlanProducer.planDistinct(
        rewrittenPlan,
        newGroupingExpressionsMap,
        distinctQueryProjection.groupingExpressions,
        context)
    } else {
      context.logicalPlanProducer.planOrderedDistinct(
        rewrittenPlan,
        newGroupingExpressionsMap,
        orderToLeverage,
        distinctQueryProjection.groupingExpressions,
        context)
    }
  }
}
