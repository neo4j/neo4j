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

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ExecutionPlanInProgress, Phase, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.pipes._
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext

case class OptionalMatchBuilder(solveMatch: Phase) extends PlanBuilder {
  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor): Boolean = plan.query.optional

  def apply(in: ExecutionPlanInProgress, context: PlanContext)(implicit pipeMonitor: PipeMonitor): ExecutionPlanInProgress = {
    val listeningPipe = new ArgumentPipe(in.pipe.symbols)()
    val nonOptionalQuery = in.query.copy(optional = false)
    val postMatchPlan = solveMatch(in.copy(pipe = listeningPipe, query = nonOptionalQuery), context)
    val matchPipe = postMatchPlan.pipe

    val optionalMatchPipe = OptionalMatchPipe(in.pipe, matchPipe, matchPipe.symbols)
    postMatchPlan.copy(pipe = optionalMatchPipe)
  }
}
