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
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentAsc
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentDesc
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder

object leverageOrder {

  final case class OrderToLeverageWithAliases(
    orderToLeverageForGrouping: Seq[Expression],
    groupingExpressionsMap: Map[LogicalVariable, Expression],
    aggregationExpressionsMap: Map[LogicalVariable, Expression]
  )

  /**
   * Given the order of a plan, some grouping expressions, and available symbols, return the prefix
   * of the provided order that is part of the grouping expressions.
   *
   * @param inputProvidedOrder     the provided order of the current plan
   * @param groupingExpressionsMap a Map of projected name to grouping expression
   * @param availableSymbols       the available symbols of the current plan
   * @return a tuple of A) the prefix of the provided order that is part of the grouping expressions
   *         and B) the grouping expressions map rewritten to use variables that are already available symbols
   */
  def apply(
    inputProvidedOrder: ProvidedOrder,
    groupingExpressionsMap: Map[LogicalVariable, Expression],
    aggregationExpressionsMap: Map[LogicalVariable, Expression],
    availableSymbols: Set[LogicalVariable]
  ): OrderToLeverageWithAliases = {
    // Collect aliases for all grouping expressions which project a variable that is already an available symbol
    val aliasMap: Map[Expression, LogicalVariable] = groupingExpressionsMap.collect {
      case (k, expr) if availableSymbols.contains(k) => (expr, k)
    }

    // When we can read variables instead of expressions in distinct, we should do that.
    // The new grouping expressions map contains aliases as values, where available.
    val newGroupingExpressionsMap = groupingExpressionsMap.map {
      case original @ (_, _: Variable) => original
      case (k, expr)                   => (k, aliasMap.getOrElse(expr, expr))
    }

    val (orderToLeverage, newAggregationExpressionsMap) = {

      val aliasesForProvidedOrder = {
        val newGroupingVariablesAliases = newGroupingExpressionsMap.collect {
          case (k, v: Variable) if availableSymbols.contains(k) => (k, v)
        }
        aliasMap ++ newGroupingVariablesAliases
      }

      // To find out if there are order columns of which we can leverage the order, we have to use the same aliases in the provided order.
      val aliasedInputProvidedOrderColumns = inputProvidedOrder.columns.map {
        case c @ Asc(expression, _)  => c.copy(aliasesForProvidedOrder.getOrElse(expression, expression))
        case c @ Desc(expression, _) => c.copy(aliasesForProvidedOrder.getOrElse(expression, expression))
      }

      providedOrderPrefix(aliasedInputProvidedOrderColumns, newGroupingExpressionsMap.values.toSet, aggregationExpressionsMap)
    }

    OrderToLeverageWithAliases(orderToLeverage, newGroupingExpressionsMap, newAggregationExpressionsMap)
  }

  private def providedOrderPrefix(
    inputProvidedOrderColumns: Seq[ColumnOrder],
    groupingExpressions: Set[Expression],
    aggregationExpressionsMap: Map[LogicalVariable, Expression]
  ): (Seq[Expression], Map[LogicalVariable, Expression]) = {
    // We use the instances of expressions from the groupingExpressions (instead of the instance of expressions from the ProvidedOrder).
    // This is important because some rewriters will rewrite expressions based on reference equality
    // and we need to make sure that orderToLeverage expressions are equal to grouping expressions, even after those rewriters.
    // Likewise, we do the same for the aggregation order expression.
    val groupingOrderPrefixOptions: Seq[Option[Expression]] =
      inputProvidedOrderColumns.map(_.expression)
        .map { exp =>
          groupingExpressions.find(_ == exp)
        }
        .takeWhile(_.isDefined)
    val aggregationOrderCandidate: Option[ColumnOrder] =
      inputProvidedOrderColumns.lift(groupingOrderPrefixOptions.length)
    val groupingOrderPrefix = groupingOrderPrefixOptions.flatten

    val newAggregationExpressionsMap = aggregationExpressionsMap.map {
      case (v, f: FunctionInvocation) if AggregationHelper.hasInterestingOrder(f) =>
        aggregationOrderCandidate.find(_.expression == f.args(0)) match {
          case Some(_: Asc)  => v -> f.copy(order = ArgumentAsc)(f.position)
          case Some(_: Desc) => v -> f.copy(order = ArgumentDesc)(f.position)
          case None          => v -> f
        }
      case (v, e) => v -> e
    }

    (groupingOrderPrefix, newAggregationExpressionsMap)
  }
}
