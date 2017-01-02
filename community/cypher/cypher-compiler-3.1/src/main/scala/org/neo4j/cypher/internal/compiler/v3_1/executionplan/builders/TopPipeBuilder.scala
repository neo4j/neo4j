/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.executionplan.builders

import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.{CachedExpression, Add, Literal, Variable}
import org.neo4j.cypher.internal.compiler.v3_1.commands.{Slice, SortItem}
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v3_1.pipes
import org.neo4j.cypher.internal.compiler.v3_1.pipes.{PipeMonitor, Top1Pipe, TopNPipe}
import org.neo4j.cypher.internal.compiler.v3_1.spi.PlanContext

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
      case _                          => throw new AssertionError("This builder should not be called for this query")
    }

    val sortDescriptions = sortItems.map(translateSortDescription).toList
    val resultPipe = limitExpression match {
      case Literal(1) =>  new Top1Pipe(newPlan.pipe, sortDescriptions)()
      case e =>  new TopNPipe(newPlan.pipe, sortDescriptions, e)()
    }

    val solvedSort = q.sort.map(_.solve)

    val resultQ = q.copy(sort = solvedSort, slice = newSlice)

    plan.copy(pipe = resultPipe, query = resultQ)
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val q = plan.query
    val extracted = q.extracted
    val unsolvedOrdering = plan.query.sort.exists(x => x.unsolved && !x.token.expression.containsAggregate)
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

  private def translateSortDescription(s: SortItem): pipes.SortDescription = s match {
    case SortItem(Variable(name), true) => pipes.Ascending(name)
    case SortItem(Variable(name), false) => pipes.Descending(name)
    case SortItem(CachedExpression(name, _), true) => pipes.Ascending(name)
    case SortItem(CachedExpression(name, _), false) => pipes.Descending(name)
  }
}

