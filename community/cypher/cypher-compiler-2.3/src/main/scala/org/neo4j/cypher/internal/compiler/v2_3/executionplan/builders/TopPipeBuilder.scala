/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.pipes.{PipeMonitor, TopPipe}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Add
import org.neo4j.cypher.internal.compiler.v2_3.commands.Slice
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext

class TopPipeBuilder extends PlanBuilder with SortingPreparations {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val newPlan = extractBeforeSort(plan)

    val q = newPlan.query
    val sortItems = q.sort.map(_.token)
    val slice = q.slice.get.token

    /*
    First, we calculate how much we must store. If the query has a SKIP, we need to keep SKIP + LIMIT number of rows,
    around, and since we do not take on the SKIP part, we need to set a new slice that contains the SKIP

    If no SKIP exists, it's simple - we mark the slice as solved, and use the LIMIT expression as is.
     */
    val (limitExpression, newSlice) = slice match {
      case Slice(Some(skip), Some(l)) => (Add(skip, l), Some(Unsolved(Slice(Some(skip), None))))
      case Slice(None, Some(l))       => (l, Some(Solved(slice)))
      case _                          => throw new ThisShouldNotHappenError("Andres", "This builder should not be called for this query")
    }

    val resultPipe = new TopPipe(newPlan.pipe, sortItems.toList, limitExpression)()

    val solvedSort = q.sort.map(_.solve)

    val resultQ = q.copy(sort = solvedSort, slice = newSlice)

    plan.copy(pipe = resultPipe, query = resultQ)
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val q = plan.query
    val extracted = q.extracted
    val unsolvedOrdering = plan.query.sort.filter(x => x.unsolved && !x.token.expression.containsAggregate).nonEmpty
    val limited = q.slice.exists(_.token.limit.nonEmpty)

    extracted && unsolvedOrdering && limited
  }

  override def missingDependencies(plan: ExecutionPlanInProgress) = if (!plan.query.extracted) {
    Seq()
  } else {
    val aggregations = plan.query.sort.
                       filter(_.token.expression.containsAggregate).
                       map(_.token.expression.toString())

    if (aggregations.nonEmpty) {
      Seq("Aggregation expressions must be listed in the RETURN/WITH clause to be used in ORDER BY")
    } else {
      Seq()
    }
  }
}

