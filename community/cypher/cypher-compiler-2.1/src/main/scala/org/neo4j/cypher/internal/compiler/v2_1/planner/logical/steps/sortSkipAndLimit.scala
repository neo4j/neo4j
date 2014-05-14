/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

object sortSkipAndLimit {
  def apply(plan: QueryPlan)(implicit context: LogicalPlanningContext): QueryPlan = {
    val query = context.query
    val logicalPlan = plan.plan
    val projection = query.projection

    val producedPlan = (projection.sortItems.toList, projection.skip, projection.limit) match {
      case (Nil, s, l) =>
        addLimit(l, addSkip(s, logicalPlan))

      case (sort, None, Some(l)) =>
        SortedLimit(logicalPlan, l, sort)

      case (sort, Some(s), Some(l)) =>
        Skip(SortedLimit(logicalPlan, ast.Add(l, s)(null), sort), s)

      case (sort, s, None) if sort.exists(notIdentifier) =>
        val newPlan: LogicalPlan = ensureSortablePlan(sort, plan)
        val orderBy = sort.map(sortDescription)
        val sortPlan = Sort(newPlan, orderBy)

        addSkip(s, sortPlan)

      case (sort, s, None) =>
        val sortPlan = Sort(logicalPlan, sort.map(sortDescription))
        addSkip(s, sortPlan)
    }

    val newProjection = plan.solved.projection
      .withSortItems(projection.sortItems)
      .withSkip(projection.skip)
      .withLimit(projection.limit)

    val solved = plan.solved.withProjection(newProjection)

    QueryPlan(producedPlan, solved)
  }

  private def ensureSortablePlan(sort: List[ast.SortItem], plan: QueryPlan): LogicalPlan = {
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
    Projection(plan.plan, totalProjections)
  }

  private def sortDescription(in: ast.SortItem): SortDescription = in match {
    case ast.AscSortItem(ast.Identifier(key)) => Ascending(key)
    case ast.DescSortItem(ast.Identifier(key)) => Descending(key)
    case sortItem@ast.AscSortItem(exp) => Ascending(FreshIdNameGenerator.name(exp.position))
    case sortItem@ast.DescSortItem(exp) => Descending(FreshIdNameGenerator.name(exp.position))
  }

  private def notIdentifier(s: ast.SortItem) = !s.expression.isInstanceOf[ast.Identifier]

  private def addSkip(s: Option[ast.Expression], plan: LogicalPlan): LogicalPlan =
    s.fold(plan)(x => Skip(plan, x))

  private def addLimit(s: Option[ast.Expression], plan: LogicalPlan): LogicalPlan =
    s.fold(plan)(x => Limit(plan, x))
}
