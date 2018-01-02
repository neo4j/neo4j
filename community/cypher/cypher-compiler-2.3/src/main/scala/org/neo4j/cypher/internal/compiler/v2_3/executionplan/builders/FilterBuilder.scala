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

import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{True, Predicate}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{PipeMonitor, FilterPipe, Pipe}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext

class FilterBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val q = plan.query
    val p = plan.pipe

    val onlyDeterministic = !allPatternsSolved(plan)
    val item = q.where.filter(pred => yesOrNo(pred, p, onlyDeterministic))
    val pred: Predicate = item.map(_.token).reduce(_ andWith _)

    val newPipe = if (pred == True()) {
      p
    } else {
      new FilterPipe(p, pred)()
    }

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

  private def allPatternsSolved(plan: ExecutionPlanInProgress) =
    plan.query.patterns.forall(_.solved)

  private def yesOrNo(q: QueryToken[_], p: Pipe, onlyDeterministic: Boolean) = q match {
    case Unsolved(pred: Predicate) =>
      pred.symbolDependenciesMet(p.symbols) && (!onlyDeterministic || pred.isDeterministic)
    case _                         => false
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val onlyDeterministic = !allPatternsSolved(plan)
    plan.query.where.exists(pred => yesOrNo(pred, plan.pipe, onlyDeterministic))
  }
}
