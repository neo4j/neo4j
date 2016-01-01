/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.ast
import org.neo4j.cypher.internal.compiler.v2_1.helpers.FreshIdNameGenerator
import org.neo4j.cypher.internal.compiler.v2_1.pipes.{Descending, Ascending, SortDescription}
import org.neo4j.cypher.internal.compiler.v2_1.planner.{QueryProjection, QueryGraph, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v2_1.ast.PatternExpression

object sortSkipAndLimit extends PlanTransformer[PlannerQuery] {

  import QueryPlanProducer._

  def apply(plan: QueryPlan, query: PlannerQuery)(implicit context: LogicalPlanningContext, subQueryLookupTable: Map[PatternExpression, QueryGraph]): QueryPlan = query.horizon match {
    case p: QueryProjection =>
    val shuffle = p.shuffle

      val producedPlan = (shuffle.sortItems.toList, shuffle.skip, shuffle.limit) match {
        case (Nil, s, l) =>
          addLimit(l, addSkip(s, plan))

        case (sortItems, None, Some(l)) =>
          planSortedLimit(plan, l, sortItems)

        case (sortItems, Some(s), Some(l)) =>
          planSortedSkipAndLimit(plan, s, l, sortItems)

        case (sortItems, s, None) if sortItems.exists(notIdentifier) =>
          val newPlan = ensureSortablePlan(sortItems, plan)
          val sortDescriptions = sortItems.map(sortDescription)
          val sortPlan = planSort(newPlan, sortDescriptions, sortItems)
          addSkip(s, sortPlan)

        case (sortItems, s, None) =>
          val sortDescriptions = sortItems.map(sortDescription)
          val sortPlan = planSort(plan, sortDescriptions, sortItems)
          addSkip(s, sortPlan)
      }

      producedPlan

    case _ => plan
  }

  private def ensureSortablePlan(sort: List[ast.SortItem], plan: QueryPlan): QueryPlan = {
    val expressionsToProject = sort.collect {
      case sortItem if notIdentifier(sortItem) => sortItem.expression
    }

    val projections: Map[String, ast.Expression] = expressionsToProject.map {
      e => FreshIdNameGenerator.name(e.position) -> e
    }.toMap

    val keepExistingIdentifiers = plan.availableSymbols.map {
      x => x.name -> ast.Identifier(x.name)(null)
    }

    val totalProjections = projections ++ keepExistingIdentifiers
    planRegularProjection(plan, totalProjections)
  }

  private def sortDescription(in: ast.SortItem): SortDescription = in match {
    case ast.AscSortItem(ast.Identifier(key)) => Ascending(key)
    case ast.DescSortItem(ast.Identifier(key)) => Descending(key)
    case sortItem@ast.AscSortItem(exp) => Ascending(FreshIdNameGenerator.name(exp.position))
    case sortItem@ast.DescSortItem(exp) => Descending(FreshIdNameGenerator.name(exp.position))
  }

  private def notIdentifier(s: ast.SortItem) = !s.expression.isInstanceOf[ast.Identifier]

  private def addSkip(s: Option[ast.Expression], plan: QueryPlan): QueryPlan =
    s.fold(plan)(x => planSkip(plan, x))

  private def addLimit(s: Option[ast.Expression], plan: QueryPlan): QueryPlan =
    s.fold(plan)(x => planLimit(plan, x))
}
