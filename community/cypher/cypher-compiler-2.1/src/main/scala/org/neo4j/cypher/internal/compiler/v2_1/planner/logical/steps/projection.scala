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

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.PlanTransformer
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanContext
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.pipes.{Descending, Ascending, SortDescription}
import org.neo4j.cypher.internal.compiler.v2_1.helpers.NameSupport.newIdName
import org.neo4j.cypher.internal.compiler.v2_1.ast.{SortItem, Expression}

object projection extends PlanTransformer {
  def apply(plan: LogicalPlan)(implicit context: LogicalPlanContext): LogicalPlan = {
    val queryGraph = context.queryGraph

    val sortSkipAndLimit = (queryGraph.sortItems.toList, queryGraph.skip, queryGraph.limit) match {
      case (Nil, s, l) =>
        addLimit(l, addSkip(s, plan))

      case (sort, None, Some(l)) =>
        SortedLimit(plan, l, sort)(l)

      case (sort, Some(s), Some(l)) =>
        Skip(SortedLimit(plan, ast.Add(l, s)(null), sort)(l), s)

      case (sort, s, None) if sort.exists(notIdentifier) =>
        val projectedPlan = projectBeforeSorting(sort, plan)
        addSkip(s, projectedPlan)

      case (sort, s, None) =>
        addSkip(s, Sort(plan, sort.map(sortDescription))(sort))
    }

    projectIfNeeded(sortSkipAndLimit, context.queryGraph)
  }

  private def projectBeforeSorting(sort: List[SortItem], plan: LogicalPlan): LogicalPlan = {
    val expressionsToProject = sort.collect {
      case sortItem if notIdentifier(sortItem) => sortItem.expression
    }

    val projections: Map[String, Expression] = expressionsToProject.map {
      e => newIdName(e.position.offset) -> e
    }.toMap
    val keepExistingIdentifiers = plan.coveredIds.map {
      x => x.name -> ast.Identifier(x.name)(null)
    }

    val totalProjections = projections ++ keepExistingIdentifiers

    val orderBy = sort.collect {
      case sortItem@ast.AscSortItem(exp) if notIdentifier(sortItem) => Ascending(newIdName(exp.position.offset))
      case sortItem@ast.DescSortItem(exp) if notIdentifier(sortItem) => Descending(newIdName(exp.position.offset))
      case s => sortDescription(s)
    }

    Sort(Projection(plan, totalProjections, hideProjections = true), orderBy)(sort)
  }

  private def notIdentifier(s: ast.SortItem) = !s.expression.isInstanceOf[ast.Identifier]

  private def sortDescription(in: ast.SortItem): SortDescription = in match {
    case ast.AscSortItem(ast.Identifier(key)) => Ascending(key)
    case ast.DescSortItem(ast.Identifier(key)) => Descending(key)
  }

  private def projectIfNeeded(plan: LogicalPlan, qg: QueryGraph) = {
    val ids = plan.coveredIds
    val projectAllCoveredIds = ids.map {
      case IdName(id) => id -> ast.Identifier(id)(null)
    }.toMap

    if (qg.projections == projectAllCoveredIds)
      plan
    else
      Projection(plan, qg.projections)

  }

  private def addSkip(s: Option[ast.Expression], plan: LogicalPlan) =
    s.map(x => Skip(plan, x)).getOrElse(plan)

  private def addLimit(s: Option[ast.Expression], plan: LogicalPlan) =
    s.map(x => Limit(plan, x)).getOrElse(plan)
}
