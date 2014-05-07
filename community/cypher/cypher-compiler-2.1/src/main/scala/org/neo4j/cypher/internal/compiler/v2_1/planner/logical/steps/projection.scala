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

import org.neo4j.cypher.internal.compiler.v2_1.ast
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{PlanTransformer, LogicalPlanContext}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.pipes.{Descending, Ascending, SortDescription}
import org.neo4j.cypher.internal.compiler.v2_1.helpers.NameSupport.newIdName

object projection extends PlanTransformer {

  def apply(plan: QueryPlan)(implicit context: LogicalPlanContext): QueryPlan = {
    val queryGraph = context.queryGraph
    val logicalPlan = plan.plan

    val sortSkipAndLimit: LogicalPlan = (queryGraph.sortItems.toList, queryGraph.skip, queryGraph.limit) match {
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

    val solved = plan.solved
      .withSortItems(context.queryGraph.sortItems)
      .copy(skip = context.queryGraph.skip, limit = context.queryGraph.limit)

    projectIfNeeded(QueryPlan(sortSkipAndLimit, solved), queryGraph.projections)
  }

  private def ensureSortablePlan(sort: List[ast.SortItem], plan: QueryPlan): LogicalPlan = {
    val expressionsToProject = sort.collect {
      case sortItem if notIdentifier(sortItem) => sortItem.expression
    }

    val projections: Map[String, ast.Expression] = expressionsToProject.map {
      e => newIdName(e.position.offset) -> e
    }.toMap

    val keepExistingIdentifiers = plan.coveredIds.map {
      x => x.name -> ast.Identifier(x.name)(null)
    }

    val totalProjections = projections ++ keepExistingIdentifiers
    Projection(plan.plan, totalProjections)
  }

  private def notIdentifier(s: ast.SortItem) = !s.expression.isInstanceOf[ast.Identifier]

  private def sortDescription(in: ast.SortItem): SortDescription = in match {
    case ast.AscSortItem(ast.Identifier(key)) => Ascending(key)
    case ast.DescSortItem(ast.Identifier(key)) => Descending(key)
    case sortItem@ast.AscSortItem(exp) => Ascending(newIdName(exp.position.offset))
    case sortItem@ast.DescSortItem(exp) => Descending(newIdName(exp.position.offset))
  }

  private def projectIfNeeded(plan: QueryPlan, projections: Map[String, ast.Expression]): QueryPlan = {
    val ids = plan.coveredIds
    val projectAllCoveredIds = ids.map {
      case IdName(id) => id -> ast.Identifier(id)(null)
    }.toMap

    if (projections == projectAllCoveredIds)
      QueryPlan(
        plan.plan,
        plan.solved.withProjections(projections)
      )
    else
      QueryPlan(
        Projection(plan.plan, projections),
        plan.solved.withProjections(projections)
      )
  }

  private def addSkip(s: Option[ast.Expression], plan: LogicalPlan): LogicalPlan =
    s.map(x => Skip(plan, x)).getOrElse(plan)

  private def addLimit(s: Option[ast.Expression], plan: LogicalPlan): LogicalPlan =
    s.map(x => Limit(plan, x)).getOrElse(plan)
}
