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

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical._
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_5.logical.plans.{Ascending, ColumnOrder, Descending, LogicalPlan}
import org.opencypher.v9_0.ast.{AscSortItem, DescSortItem, SortItem}
import org.opencypher.v9_0.expressions.{Expression, Variable}
import org.opencypher.v9_0.util.{FreshIdNameGenerator, InternalException}

object sortSkipAndLimit extends PlanTransformer[PlannerQuery] {

  def apply(plan: LogicalPlan, query: PlannerQuery, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): LogicalPlan = query.horizon match {
    case p: QueryProjection =>
      val shuffle = p.shuffle
      val producedPlan = (shuffle.sortItems.toList, shuffle.skip, shuffle.limit) match {
        case (Nil, skip, limit) =>
          addLimit(limit, addSkip(skip, plan, context), context)

        case (sortItems, skip, limit) =>
          /* Find:
           * projectItemsForAliasedSortItems: WITH a.foo AS x ORDER BY x => (x -> a.foo)
           * aliasedSortItems: WITH a.foo AS x ORDER BY x => x
           * unaliasedSortItems: WITH a, a.foo AS x ORDER BY a.bar => a.bar
           */
          val (projectItemsForAliases, aliasedSortItems, unaliasedSortItems) =
            sortItems.foldLeft((Map.empty[String, Expression], Seq.empty[SortItem], Seq.empty[SortItem])) {
              case ((_projectItems, _aliasedSortItems, _unaliasedSortItems), sortItem) =>
                sortItem.expression match {
                  case Variable(name) =>
                    extractProjectItem(p, name) match {
                      // Aliased
                      case Some((projectItem, fromRegularProjection)) =>
                        if (fromRegularProjection) {
                          // Possibly the expression has not been projected yet. Let's take care of it
                          (_projectItems + projectItem, _aliasedSortItems :+ sortItem, _unaliasedSortItems)
                        } else {
                          // AggregatingQueryProjection or DistinctQueryProjection definitely took care of projecting that variable already. Cool!
                          (_projectItems, _aliasedSortItems :+ sortItem, _unaliasedSortItems)
                        }
                      // Semantic analysis should have caught this
                      case None =>
                        // If the variable we're sorting by is not part of the current projections list,
                        // it must have been part of the previous horizon. Thus, it will already be projected.
                        // If the variable is unknown, semantic analysis should have caught it and we would not get here
                        (_projectItems, _aliasedSortItems :+ sortItem, _unaliasedSortItems)
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

                    (_projectItems ++ referencedProjectItems, _aliasedSortItems, _unaliasedSortItems :+ sortItem)
                }
            }

          // change the unaliasedSortItems to refer to newly introduced variables instead
          val (projectItemsForUnaliasedSortItems, newUnaliasedSortItems) = unaliasedSortItems.map { sortItem =>
            val newVariable = Variable(FreshIdNameGenerator.name(sortItem.expression.position))(sortItem.expression.position)
            val projectItem = newVariable.name -> sortItem.expression
            val newSortItem = sortItem.mapExpression(_ => newVariable)
            (projectItem, newSortItem)
          }.unzip

          // Project all variables needed for sort in two steps
          // First the ones that are part of projection list and may introduce variables that are needed for the second projection
          val preProjected1 = projection(plan, projectItemsForAliases, projectItemsForAliases, context, solveds, cardinalities)
          // And then all the ones from unaliased sort items that may refer to newly introduced variables
          val preProjected2 = projection(preProjected1, projectItemsForUnaliasedSortItems.toMap, Map.empty, context, solveds, cardinalities)

          // plan the actual sort
          val newSortItems = aliasedSortItems ++ newUnaliasedSortItems
          val columnOrders = newSortItems.map(columnOrder)
          val sortedPlan = context.logicalPlanProducer.planSort(preProjected2, columnOrders, sortItems, context)

          addLimit(limit, addSkip(skip, sortedPlan, context), context)
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

  /**
    * Finds the project item that is referred to by its alias. Also returns whether is came from a RegularQueryProjection (`true`)
    * or from DistinctQueryProjection or AggregatingQueryProjection (`false`).
    */
  private def extractProjectItem(projection: QueryProjection, name: String): Option[((String, Expression), Boolean)] = {
    projection match {
      case RegularQueryProjection(projections, _) => projections.get(name).map(exp => (name -> exp, true))
      case DistinctQueryProjection(projections, _) => projections.get(name).map(exp => (name -> exp, false))
      case AggregatingQueryProjection(groupingExpressions, aggregationExpressions, _) =>
        (groupingExpressions ++ aggregationExpressions).get(name).map(exp => (name -> exp, false))
    }
  }
}
