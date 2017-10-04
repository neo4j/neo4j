/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical._
import org.neo4j.cypher.internal.frontend.v3_4.ast.{AscSortItem, DescSortItem, SortItem}
import org.neo4j.cypher.internal.v3_4.logical.plans.{Ascending, ColumnOrder, Descending, LogicalPlan}
import org.neo4j.cypher.internal.ir.v3_4.{IdName, PlannerQuery, QueryProjection}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, Variable}

object sortSkipAndLimit extends PlanTransformer[PlannerQuery] {

  def apply(plan: LogicalPlan, query: PlannerQuery)(implicit context: LogicalPlanningContext): LogicalPlan = query.horizon match {
    case p: QueryProjection =>
      val shuffle = p.shuffle
      val producedPlan = (shuffle.sortItems.toList, shuffle.skip, shuffle.limit) match {
        case (Nil, s, l) =>
          addLimit(l, addSkip(s, plan))

        case (sortItems, s, l) =>
          require(sortItems.forall(_.expression.isInstanceOf[Variable]))
          val columnOrders = sortItems.map(columnOrder)
          val sortedPlan = context.logicalPlanProducer.planSort(plan, columnOrders, sortItems)

          addLimit(l, addSkip(s, sortedPlan))
      }

      producedPlan

    case _ => plan
  }

  private def columnOrder(in: SortItem): ColumnOrder = in match {
    case AscSortItem(Variable(key)) => Ascending(IdName(key))
    case DescSortItem(Variable(key)) => Descending(IdName(key))
    case _ => throw new InternalException("Sort items expected to only use single variable expression")
  }

  private def addSkip(s: Option[Expression], plan: LogicalPlan)(implicit context: LogicalPlanningContext) =
    s.fold(plan)(x => context.logicalPlanProducer.planSkip(plan, x))

  private def addLimit(s: Option[Expression], plan: LogicalPlan)(implicit context: LogicalPlanningContext) =
    s.fold(plan)(x => context.logicalPlanProducer.planLimit(plan, x))
}
