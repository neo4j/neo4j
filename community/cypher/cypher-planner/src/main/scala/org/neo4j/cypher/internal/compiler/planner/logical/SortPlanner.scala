/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.logical.steps.projection
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc
import org.neo4j.cypher.internal.ir
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.FullSatisfaction
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.NoSatisfaction
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.Satisfaction
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

object SortPlanner {
  def maybeSortedPlan(plan: LogicalPlan, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Option[LogicalPlan] = {
    if (interestingOrder.requiredOrderCandidate.nonEmpty) {
      orderSatisfaction(interestingOrder, context, plan) match {
        case FullSatisfaction() =>
          None
        case NoSatisfaction() =>
          planSort(plan, Seq.empty, interestingOrder, context)
        case Satisfaction(satisfiedPrefix, _) =>
          planSort(plan, satisfiedPrefix, interestingOrder, context)
      }
    } else {
      None
    }
  }

  /**
   * Given a plan and an interesting order, plan a Sort if necessary.
   * Update the Solveds attribute if no Sort planning was necessery.
   * Throw an exception if no plan that can solve the interesting order could be found.
   */
  def ensureSortedPlanWithSolved(plan: LogicalPlan, interestingOrder: InterestingOrder, context: LogicalPlanningContext): LogicalPlan =
    maybeSortedPlan(plan, interestingOrder, context) match {
      case Some(sortedPlan) => sortedPlan
      case _ if interestingOrder.requiredOrderCandidate.nonEmpty =>
        orderSatisfaction(interestingOrder, context, plan) match {
          case FullSatisfaction() =>
            context.logicalPlanProducer.updateSolvedForSortedItems(plan, interestingOrder, context)
          case _ =>
            throw new AssertionError("Expected a sorted plan")
        }
      case _ => plan
    }

  /**
   * Given an interesting order and a plan with a provided order, return the Satisfaction of the interesting order given that provided order.
   */
  def orderSatisfaction(interestingOrder: InterestingOrder, context: LogicalPlanningContext, plan: LogicalPlan): Satisfaction =
    interestingOrder.satisfiedBy(context.planningAttributes.providedOrders.get(plan.id))

  private def planSort(plan: LogicalPlan,
                       satisfiedPrefix: Seq[ir.ordering.ColumnOrder],
                       interestingOrder: InterestingOrder,
                       context: LogicalPlanningContext): Option[LogicalPlan] = {

    def idFrom(expression: Expression, projection: Map[String, Expression]): String = {
      projection.find(_._2 == expression).map(_._1).getOrElse(expression.asCanonicalStringVal)
    }

    def projected(plan: LogicalPlan, projections: Map[String, Expression], updateSolved: Boolean = true): LogicalPlan = {
      val projectionDeps = projections.flatMap(e => e._2.dependencies)
      if (projections.nonEmpty && projectionDeps.forall(e => plan.availableSymbols.contains(e.name)))
        projection(plan, projections, if (updateSolved) projections else Map.empty, context)
      else
        plan
    }

    case class SortColumnsWithProjections(columnOrder: ColumnOrder,
                                          providedOrderColumn: ir.ordering.ColumnOrder,
                                          unaliasedProjections: Option[(String, Expression)])

    val sortItems: Seq[SortColumnsWithProjections] = interestingOrder.requiredOrderCandidate.order.map {
      // Aliased sort expressions
      case asc@Asc(Variable(key), _) =>
        SortColumnsWithProjections(Ascending(key), asc, None)
      case desc@Desc(Variable(key), _) =>
        SortColumnsWithProjections(Descending(key), desc, None)

      // Unaliased sort expressions
      case asc@Asc(expression, projections) =>
        val columnId = idFrom(expression, projections)
        SortColumnsWithProjections(Ascending(columnId), asc, Some(columnId -> expression))
      case desc@Desc(expression, projections) =>
        val columnId = idFrom(expression, projections)
        SortColumnsWithProjections(Descending(columnId), desc, Some(columnId -> expression))
    }

    // Project all variables needed for sort in two steps
    // First the ones that are part of projection list and may introduce variables that are needed for the second projection
    val projections = sortItems.foldLeft(Map.empty[String, Expression])((acc, i) => acc ++ i.providedOrderColumn.projections)
    val projected1 = projected(plan, projections)
    // And then all the ones from unaliased sort items that may refer to newly introduced variables
    val unaliasedProjections = sortItems.foldLeft(Map.empty[String, Expression])((acc, i) => acc ++ i.unaliasedProjections)
    val projected2 = projected(projected1, unaliasedProjections, updateSolved = false)

    val sortColumns: Seq[ColumnOrder] = sortItems.map(_.columnOrder)
    val providedOrderColumns = sortItems.map(_.providedOrderColumn)

    if (sortColumns.forall(column => projected2.availableSymbols.contains(column.id))) {
      if (satisfiedPrefix.isEmpty) {
        // Full sort required
        Some(context.logicalPlanProducer.planSort(projected2, sortColumns, providedOrderColumns, interestingOrder, context))
      } else {
        // Partial sort suffices
        val (prefixColumnOrders, suffixColumnOrders) = sortColumns.splitAt(satisfiedPrefix.length)
        Some(context.logicalPlanProducer.planPartialSort(projected2, prefixColumnOrders, suffixColumnOrders, providedOrderColumns, interestingOrder, context))
      }
    } else {
      None
    }
  }
}
