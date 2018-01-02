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

import org.neo4j.cypher.internal.compiler.v2_3.commands.{Pattern, ShortestPath}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{PipeMonitor, ShortestPathPipe, Pipe}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext

class ShortestPathBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val q = plan.query
    val p = plan.pipe

    val item = q.patterns.filter(isShortestPathCommand(p, _)).head
    val shortestPath = item.token.asInstanceOf[ShortestPath]
    val pathPredicates = q.where.collect {
      case Unsolved(predicate) => predicate
    }

    val shortestPathPipe = new ShortestPathPipe(p, shortestPath, pathPredicates)()

    plan.copy(pipe = shortestPathPipe, query = q.copy(patterns = q.patterns.filterNot(_ == item) :+ item.solve))
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) =
    plan.query.patterns.exists(isShortestPathCommand(plan.pipe, _))

  private def isShortestPathCommand(p: Pipe, token: QueryToken[_]): Boolean = token match {
    case Unsolved(sp: ShortestPath) => sp.symbolDependenciesMet(p.symbols)
    case _ => false
  }

  override def missingDependencies(plan: ExecutionPlanInProgress) = {
    val querySoFar = plan.query
    val symbols = plan.pipe.symbols

    val unsolvedShortestPaths: Seq[ShortestPath] = querySoFar.patterns.
      filter(sp => !sp.solved && sp.token.isInstanceOf[ShortestPath]).map(_.token.asInstanceOf[ShortestPath])

    unsolvedShortestPaths.
      flatMap(sp => symbols.missingSymbolTableDependencies(sp)).
      distinct.
      map("Unknown identifier `%s`".format(_))
  }
}
