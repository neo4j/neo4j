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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.opencypher.v9_0.util.InternalException
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical._
import org.opencypher.v9_0.ast.{AscSortItem, DescSortItem, SortItem}
import org.neo4j.cypher.internal.v3_5.logical.plans.{Ascending, ColumnOrder, Descending, LogicalPlan}
import org.neo4j.cypher.internal.ir.v3_5.{PlannerQuery, QueryProjection}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.opencypher.v9_0.expressions.{Expression, Variable}

object sortSkipAndLimit extends PlanTransformer[PlannerQuery] {

  def apply(plan: LogicalPlan, query: PlannerQuery, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): LogicalPlan = query.horizon match {
    case p: QueryProjection =>
      val shuffle = p.shuffle
      val producedPlan = (shuffle.sortItems.toList, shuffle.skip, shuffle.limit) match {
        case (Nil, s, l) =>
          addLimit(l, addSkip(s, plan, context), context)

        case (sortItems, s, l) =>
          // Sort needs its sort columns to be variables, thus we need to project these columns already now
          require(sortItems.forall(_.expression.isInstanceOf[Variable]))
          val columnsToProjectForSort = p.projections.filter { case (name, _) => sortItems.map(_.expression).exists { case Variable(varname) => varname == name } }
          val preProjected = projection(plan, columnsToProjectForSort, context, solveds, cardinalities)
          val columnOrders = sortItems.map(columnOrder)
          val sortedPlan = context.logicalPlanProducer.planSort(preProjected, columnOrders, sortItems, context)

          addLimit(l, addSkip(s, sortedPlan, context), context)
      }

      producedPlan

    case _ => plan
  }

  private def columnOrder(in: SortItem): ColumnOrder = in match {
    case AscSortItem(Variable(key)) => Ascending(key)
    case DescSortItem(Variable(key)) => Descending(key)
    case _ => throw new InternalException("Sort items expected to only use single variable expression")
  }

  private def addSkip(s: Option[Expression], plan: LogicalPlan, context: LogicalPlanningContext) =
    s.fold(plan)(x => context.logicalPlanProducer.planSkip(plan, x, context))

  private def addLimit(s: Option[Expression], plan: LogicalPlan, context: LogicalPlanningContext) =
    s.fold(plan)(x => context.logicalPlanProducer.planLimit(plan, x, context = context))
}
