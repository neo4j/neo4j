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

import org.neo4j.cypher.internal.compiler.v2_3.commands.NamedPath
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{PipeMonitor, NamedPathPipe, Pipe}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext

class NamedPathBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val p = plan.pipe

    val q = plan.query
    val item = q.namedPaths.filter(np => yesOrNo(np, p)).head
    val namedPaths = item.token

    val pipe = new NamedPathPipe(p, namedPaths.pathName, namedPaths.pathPattern)

    val newQ = q.copy(namedPaths = q.namedPaths.filterNot(_ == item) :+ item.solve)
    plan.copy(query = newQ, pipe = pipe)
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) =
    plan.query.namedPaths.exists(yesOrNo(_, plan.pipe))

  private def yesOrNo(q: QueryToken[_], p: Pipe) = q match {
    case Unsolved(np: NamedPath) =>
      val pathPoints = np.pathPattern.flatMap(_.possibleStartPoints)
      pathPoints.forall(x => p.symbols.checkType(x._1, x._2))

    case _                       => false
  }
}
