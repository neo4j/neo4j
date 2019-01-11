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
package org.neo4j.cypher.internal.compiler.v4_0.planner.logical

import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.steps.projection
import org.neo4j.cypher.internal.ir.v4_0.InterestingOrder
import org.neo4j.cypher.internal.v4_0.ast.{AscSortItem, DescSortItem, SortItem}
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, Variable}
import org.neo4j.cypher.internal.v4_0.logical.plans.{Ascending, ColumnOrder, Descending, LogicalPlan}

object SortPlanner {
  def maybeSortedPlan(plan: LogicalPlan, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Option[LogicalPlan] = {
    if (interestingOrder.requiredOrderCandidate.nonEmpty && !interestingOrder.satisfiedBy(context.planningAttributes.providedOrders.get(plan.id))) {
      def idFrom(expression:Expression, projection : Map[String,Expression]): String = {
        projection.find(_._2==expression).map(_._1).getOrElse(expression.asCanonicalStringVal)
      }

      def projected(plan: LogicalPlan, projections: Map[String, Expression], updateSolved: Boolean = true): LogicalPlan = {
        val projectionDeps = projections.flatMap(e => e._2.dependencies)
        if (projections.nonEmpty && projectionDeps.forall(e => plan.availableSymbols.contains(e.name)))
          projection(plan, projections, if (updateSolved) projections else Map.empty, interestingOrder, context)
        else
          plan
      }

      val sortItems: Seq[(ColumnOrder, SortItem, Map[String, Expression], Option[(String, Expression)])] = interestingOrder.requiredOrderCandidate.order.map {
        case InterestingOrder.Asc(v@Variable(key), projection) => (Ascending(key), AscSortItem(v)(v.position), projection, None)
        case InterestingOrder.Desc(v@Variable(key), projection) => (Descending(key), DescSortItem(v)(v.position), projection, None)
        case InterestingOrder.Asc(expression, projection) =>
          val columnId = idFrom(expression, projection)
          (Ascending(columnId), AscSortItem(expression)(expression.position), projection, Some(columnId -> expression))
        case InterestingOrder.Desc(expression, projection) =>
          val columnId = idFrom(expression, projection)
          (Descending(columnId), DescSortItem(expression)(expression.position), projection, Some(columnId -> expression))
      }

      val projections = sortItems.foldLeft(Map.empty[String, Expression])((acc, i) => acc ++ i._3)
      val projected1 = projected(plan, projections)
      val unaliasedProjections = sortItems.foldLeft(Map.empty[String, Expression])((acc, i) => acc ++ i._4)
      val projected2 = projected(projected1, unaliasedProjections, updateSolved = false)

      val sortColumns = sortItems.map(_._1)
      if (sortColumns.forall(column => projected2.availableSymbols.contains(column.id)))
        Some(context.logicalPlanProducer.planSort(projected2, sortColumns, sortItems.map(_._2), interestingOrder, context))
      else
        None
    } else {
      None
    }
  }

  def sortedPlanWithSolved(plan: LogicalPlan, interestingOrder: InterestingOrder, context: LogicalPlanningContext,
                           unsolvable: () => LogicalPlan = throw new AssertionError("Expected a sorted plan")): LogicalPlan = {
    maybeSortedPlan(plan, interestingOrder, context) match {
      case Some(sortedPlan) => sortedPlan
      case _ if interestingOrder.requiredOrderCandidate.nonEmpty =>
        if (interestingOrder.satisfiedBy(context.planningAttributes.providedOrders.get(plan.id)))
          context.logicalPlanProducer.updateSolvedForSortedItems(plan, interestingOrder, context)
        else
          unsolvable()
      case _ => plan
    }
  }
}
