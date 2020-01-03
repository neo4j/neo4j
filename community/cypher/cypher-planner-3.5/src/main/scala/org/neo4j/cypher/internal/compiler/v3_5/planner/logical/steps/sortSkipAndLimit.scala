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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical._
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.ast.{AscSortItem, DescSortItem, SortItem}
import org.neo4j.cypher.internal.v3_5.expressions.{Expression, Variable}
import org.neo4j.cypher.internal.v3_5.util.{FreshIdNameGenerator, InternalException}

object sortSkipAndLimit extends PlanTransformerWithRequiredOrder {

  /**
    * @param query query.interestingOrder will be the one marked as solved.
    * @param interestingOrder this order can potentially be different from query.providedOrder. It can contain renames and
    *                      will be used to check if we need to sort.
    */
  def apply(plan: LogicalPlan, query: PlannerQuery, interestingOrder: InterestingOrder, context: LogicalPlanningContext): LogicalPlan = query.horizon match {
    case p: QueryProjection =>
      val shuffle = p.shuffle
       (shuffle.sortItems.toList, shuffle.skip, shuffle.limit) match {
        case (Nil, skip, limit) =>
          addLimit(limit, addSkip(skip, plan, context), context)

        case (sortItems, skip, limit) =>
          /* Collect stuff while examining the sort items.
           * For the query `WITH a, a.foo AS x ORDER BY x, a.bar` we will collect:
           *
           * projectItemsForAliased:   (x -> a.foo)
           * projectItemsForUnaliased: (a.bar -> Var01)
           * newSortItems:             x, Var01
           */
          val (projectItemsForAliased, projectItemsForUnaliased, newSortItems) =
            sortItems.foldLeft((Map.empty[String, Expression], Map.empty[String, Expression], Seq.empty[SortItem])) {
              case ((_piForAliased, _piForUnaliased, _newSortItems), sortItem) =>
                sortItem.expression match {
                  case Variable(name) =>
                    extractProjectItem(p, name) match {
                      // Aliased
                      case Some((projectItem, fromRegularProjection)) =>
                        if (fromRegularProjection) {
                          // Possibly the expression has not been projected yet.
                          // We add it to the map and pass it to projection. This class will make sure not to project anything
                          // twice if it was part of a previous RegularProjection
                          (_piForAliased + projectItem, _piForUnaliased, _newSortItems :+ sortItem)
                        } else {
                          // AggregatingQueryProjection or DistinctQueryProjection definitely took care of projecting that variable already. Cool!
                          (_piForAliased, _piForUnaliased, _newSortItems :+ sortItem)
                        }
                      case None =>
                        // If the variable we're sorting by is not part of the current projections list,
                        // it must have been part of the previous horizon. Thus, it will already be projected.
                        // If the variable is unknown, semantic analysis should have caught it and we would not get here
                        (_piForAliased, _piForUnaliased, _newSortItems :+ sortItem)
                    }
                  // Unaliased
                  case _ =>
                    // find dependencies and add them to projectItems
                    val referencedProjectItems: Set[(String, Expression)] =
                      sortItem.expression.dependencies.flatMap { logvar =>
                        extractProjectItem(p, logvar.name).collect {
                          case (projectItem, true /* fromRegularProjection*/ ) => projectItem
                        }
                      }
                    // Create a new variable that we can sort by
                    val newVariable = Variable(FreshIdNameGenerator.name(sortItem.expression.position))(sortItem.expression.position)
                    // The project item for an unaliased sort item
                    val projectItemForUnaliasedSortItem = newVariable.name -> sortItem.expression
                    // We made an aliased SortItem out of the unaliased SortItem
                    val newSortItem = sortItem.mapExpression(_ => newVariable)

                    (_piForAliased ++ referencedProjectItems, _piForUnaliased + projectItemForUnaliasedSortItem, _newSortItems :+ newSortItem)
                }
            }

          // plan the actual sort
          val columnOrders = newSortItems.map(columnOrder)

          val sortedPlan =
            // The !interestingOrder.isEmpty check is only here because we do not recognize more complex required orders
            // at the moment and do not want to abort sorting only because an empty required order is satisfied by anything.
            if (interestingOrder.required.nonEmpty && interestingOrder.satisfiedBy(context.planningAttributes.providedOrders.get(plan.id))) {
              // We can't override solved, but right now we want to set it such that it solves ORDER BY
              // on a plan that has already assigned solved.
              // Use query.interestingOrder to mark the original required order as solved.
              context.logicalPlanProducer.updateSolvedForSortedItems(plan, sortItems, query.interestingOrder, context)
            } else {
              // Project all variables needed for sort in two steps
              // First the ones that are part of projection list and may introduce variables that are needed for the second projection
              val preProjected1 = projection(plan, projectItemsForAliased, projectItemsForAliased, interestingOrder, context)
              // And then all the ones from unaliased sort items that may refer to newly introduced variables
              val preProjected2 = projection(preProjected1, projectItemsForUnaliased, Map.empty, interestingOrder, context)

              // Use query.interestingOrder to mark the original required order as solved.
              context.logicalPlanProducer.planSort(preProjected2, columnOrders, sortItems, query.interestingOrder, context)
            }

          addLimit(limit, addSkip(skip, sortedPlan, context), context)
      }

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

  /**
    * Finds the project item that is referred to by its alias. Also returns whether is came from a RegularQueryProjection (`true`)
    * or from DistinctQueryProjection or AggregatingQueryProjection (`false`).
    */
  private def extractProjectItem(projection: QueryProjection, name: String): Option[((String, Expression), Boolean)] = {
    projection match {
      case RegularQueryProjection(projections, _, _) => projections.get(name).map(exp => (name -> exp, true))
      case DistinctQueryProjection(projections, _, _) => projections.get(name).map(exp => (name -> exp, false))
      case AggregatingQueryProjection(groupingExpressions, aggregationExpressions, _, _) =>
        (groupingExpressions ++ aggregationExpressions).get(name).map(exp => (name -> exp, false))
    }
  }
}
