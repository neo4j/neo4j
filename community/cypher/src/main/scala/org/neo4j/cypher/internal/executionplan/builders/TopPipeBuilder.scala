/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.pipes.TopPipe
import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.commands.expressions.Add
import org.neo4j.cypher.internal.commands.Slice
import org.neo4j.helpers.ThisShouldNotHappenError

class TopPipeBuilder extends PlanBuilder with SortingPreparations {
  def apply(plan: ExecutionPlanInProgress) = {
    val newPlan = extractBeforeSort(plan)

    val q = newPlan.query
    val sortItems = q.sort.map(_.token)
    val slice = q.slice.get.token
    val limit = slice match {
      case Slice(Some(skip), Some(l)) => Add(skip, l)
      case Slice(None, Some(l))       => l
    }

    val resultPipe = new TopPipe(newPlan.pipe, sortItems.toList, limit)

    val solvedSort = q.sort.map(_.solve)
    val solvedSlice = slice match {
      case Slice(Some(x), _) => Some(Unsolved(Slice(Some(x), None)))
      case Slice(None, _)    => None
      case _                 => throw new ThisShouldNotHappenError("Andres", "This builder should not be called for this query")
    }

    val resultQ = q.copy(sort = solvedSort, slice = solvedSlice)

    plan.copy(pipe = resultPipe, query = resultQ)
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val extracted = q.extracted
    val unsolvedOrdering = q.sort.filter(_.unsolved).nonEmpty
    val limited = q.slice.exists(_.token.limit.nonEmpty)

    extracted && unsolvedOrdering && limited
  }

  def priority: Int = PlanBuilder.TopX
}

