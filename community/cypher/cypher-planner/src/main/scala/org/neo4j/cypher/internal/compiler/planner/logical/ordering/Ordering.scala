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
package org.neo4j.cypher.internal.compiler.planner.logical.ordering

import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.ordering
import org.neo4j.cypher.internal.logical
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

object Ordering {

  def orderedUnionColumns(
    plans: Iterable[LogicalPlan],
    context: LogicalPlanningContext
  ): Seq[ColumnOrder] = {
    plans.map(p => context.staticComponents.planningAttributes.providedOrders(p.id).columns)
      // Compute the common prefix of provided order columns, that sorts by a variable.
      .reduce { (a, b) =>
        a.lazyZip(b).takeWhile {
          case (a @ ordering.ColumnOrder.Asc(_: Variable, _), b)  => a == b
          case (a @ ordering.ColumnOrder.Desc(_: Variable, _), b) => a == b
          case _                                                  => false
        }.map(_._1).toSeq
      }
      // Convert to a logical plan ColumnOrder.
      .collect {
        case ordering.ColumnOrder.Asc(v: Variable, _)  => logical.plans.Ascending(v)
        case ordering.ColumnOrder.Desc(v: Variable, _) => logical.plans.Descending(v)
      }
  }

  def planUnionOrOrderedUnion(
    maybeSortColumns: Seq[ColumnOrder],
    p1: LogicalPlan,
    p2: LogicalPlan,
    unionMappings: List[UnionMapping],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    if (maybeSortColumns.nonEmpty && context.settings.executionModel.providedOrderPreserving) {
      // Parallel runtime does currently not support OrderedUnion
      context.staticComponents.logicalPlanProducer.planOrderedUnion(
        p1,
        p2,
        unionMappings,
        maybeSortColumns,
        context
      )
    } else {
      context.staticComponents.logicalPlanProducer.planUnion(p1, p2, unionMappings, context)
    }
  }

  def planDistinctOrOrderedDistinct(
    maybeSortColumns: Seq[ColumnOrder],
    unionPlan: LogicalPlan,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // Parallel runtime does currently not support OrderedDistinct
    if (maybeSortColumns.nonEmpty && context.settings.executionModel.providedOrderPreserving) {
      context.staticComponents.logicalPlanProducer.planOrderedDistinctForUnion(
        unionPlan,
        maybeSortColumns.map(_.id),
        context
      )
    } else {
      context.staticComponents.logicalPlanProducer.planDistinctForUnion(unionPlan, context)
    }
  }
}
