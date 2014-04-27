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
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Add, Expression}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanContext

object postProjection extends PlanTransformer {
  def apply(plan: LogicalPlan)(implicit context: LogicalPlanContext): LogicalPlan = {
    val queryGraph = context.queryGraph

    (queryGraph.sortItems.toList, queryGraph.skip, queryGraph.limit) match {
      case (Nil, s, l)              => addLimit(l, addSkip(s, plan))
      case (sort, None, Some(l))    => SortedLimit(plan, l, sort)(l)
      case (sort, Some(s), Some(l)) => Skip(SortedLimit(plan, Add(l, s)(null), sort)(l), s)
      case (sort, s, None)          => addSkip(s, Sort(plan, sort))
    }
  }

  private def addSkip(s: Option[Expression], plan: LogicalPlan) =
    s.map(x => Skip(plan, x)).getOrElse(plan)

  private def addLimit(s: Option[Expression], plan: LogicalPlan) =
    s.map(x => Limit(plan, x)).getOrElse(plan)
}
