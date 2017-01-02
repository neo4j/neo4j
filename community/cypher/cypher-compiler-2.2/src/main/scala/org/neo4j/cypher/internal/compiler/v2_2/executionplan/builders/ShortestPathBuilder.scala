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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_2.commands.ShortestPath
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.{PipeMonitor, ShortestPathPipe, Pipe}
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext

class ShortestPathBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val q = plan.query
    val p = plan.pipe

    val item = q.patterns.filter(yesOrNo(p, _)).head
    val shortestPath = item.token.asInstanceOf[ShortestPath]

    val pipe = new ShortestPathPipe(p, shortestPath)()

    plan.copy(pipe = pipe, query = q.copy(patterns = q.patterns.filterNot(_ == item) :+ item.solve))
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) =
    plan.query.patterns.exists(yesOrNo(plan.pipe, _))

  private def yesOrNo(p: Pipe, token: QueryToken[_]): Boolean = token match {
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
