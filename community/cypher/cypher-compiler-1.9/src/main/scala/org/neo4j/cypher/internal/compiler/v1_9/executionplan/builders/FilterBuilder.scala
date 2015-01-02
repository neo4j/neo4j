/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.executionplan.builders

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

import org.neo4j.cypher.internal.compiler.v1_9.commands.Predicate
import org.neo4j.cypher.internal.compiler.v1_9.pipes.{FilterPipe, Pipe}
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.{CypherException, CypherTypeException, SyntaxException}

class FilterBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val p = plan.pipe

    val item = q.where.filter(pred => yesOrNo(pred, p))
    val pred: Predicate = item.map(_.token).reduce(_ ++ _)
    val newPipe = new FilterPipe(p, pred)
    val newQuery = q.where.filterNot(item.contains) ++ item.map(_.solve)

    plan.copy(
      query = q.copy(where = newQuery),
      pipe = newPipe
    )
  }


  override def missingDependencies(plan: ExecutionPlanInProgress) = {
    val querySoFar = plan.query
    val pipe = plan.pipe

    val unsolvedPredicates = querySoFar.where.filter(_.unsolved).map(_.token)

    unsolvedPredicates.
      flatMap(pred => pipe.symbols.missingSymbolTableDependencies(pred)).
      map("Unknown identifier `%s`".format(_))
  }

  private def yesOrNo(q: QueryToken[_], p: Pipe) = q match {
    case Unsolved(pred: Predicate) => pred.symbolDependenciesMet(p.symbols)
    case _                         => false
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = plan.query.where.exists(pred => yesOrNo(pred, plan.pipe))

  def priority: Int = PlanBuilder.Filter
}
