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

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder

object leverageOrder {

  final case class OrderToLeverageWithAliases(
    orderToLeverage: Seq[Expression],
    aggregationOrder: Option[ColumnOrder],
    groupingExpressionsMap: Map[LogicalVariable, Expression]
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
    aggregations: Map[LogicalVariable, Expression],
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

    val (orderToLeverage, aggregationOrder) = {

      val aliasesForProvidedOrder = {
        val newGroupingVariablesAliases = newGroupingExpressionsMap.collect {
          case (k, v: Variable) if availableSymbols.contains(k) => (k, v)
        }
        aliasMap ++ newGroupingVariablesAliases
      }

      // To find out if there are order columns of which we can leverage the order, we have to use the same aliases in the provided order.
      val aliasedInputProvidedOrder = inputProvidedOrder.mapColumns {
        case c @ Asc(expression, _)  => c.copy(aliasesForProvidedOrder.getOrElse(expression, expression))
        case c @ Desc(expression, _) => c.copy(aliasesForProvidedOrder.getOrElse(expression, expression))
      }

      providedOrderPrefix(aliasedInputProvidedOrder, newGroupingExpressionsMap.values.toSet, aggregations.values)
    }

    OrderToLeverageWithAliases(orderToLeverage, aggregationOrder, newGroupingExpressionsMap)
  }

  private def providedOrderPrefix(
    inputProvidedOrder: ProvidedOrder,
    groupingExpressions: Set[Expression],
    aggregations: Iterable[Expression]
  ): (Seq[Expression], Option[ColumnOrder]) = {
    var aggregationOrderCandidate: Option[ColumnOrder] = None
    // We use the instances of expressions from the groupingExpressions (instead of the instance of expressions from the ProvidedOrder).
    // This is important because some rewriters will rewrite expressions based on reference equality
    // and we need to make sure that orderToLeverage expressions are equal to grouping expressions, even after those rewriters.
    // Likewise, we do the same for the aggregation order expression.
    val groupingOrderPrefix = inputProvidedOrder.columns
      .map { column =>
        (column, groupingExpressions.find(_ == column.expression))
      }
      .takeWhile {
        case (_, _: Some[_]) => true
        case (column, None) =>
          aggregationOrderCandidate = Some(column)
          false
      }.flatMap(_._2)

    // NOTE: currently the expression to aggregate is always at offset 0, but checking them all for future proofing
    val aggregationArgs = aggregations.collect {
      case FunctionInvocation(_, _, _, args) => args
    }.flatten
      .filter(_.isInstanceOf[LogicalVariable])

    val aggregationOrder = aggregationOrderCandidate.collect(order =>
      (order, aggregationArgs.find(_ == order.expression)) match {
        case (asc: Asc, Some(e))   => asc.copy(expression = e)
        case (desc: Desc, Some(e)) => desc.copy(expression = e)
      }
    )
    (groupingOrderPrefix, aggregationOrder)
  }
}
