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

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{EmptyResultPipe, PipeMonitor}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext

/**
 * This builder should make sure that the execution result is consumed, even if the user didn't ask for any results
 */
class EmptyResultBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val resultPipe = new EmptyResultPipe(plan.pipe)

    plan.copy(pipe = resultPipe)
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val q = plan.query

    val emptyResultPipeWasNotAlreadyPlanned = plan.pipe match {
      case _: EmptyResultPipe => false
      case _ => true
    }
    val noSortingLeftToDo = q.sort.isEmpty
    val noSkipOrLimitLeftToDo = q.slice.isEmpty
    val nothingToReturn = q.returns.isEmpty
    val isLastPipe = q.tail.isEmpty

    emptyResultPipeWasNotAlreadyPlanned &&
      noSortingLeftToDo &&
      noSkipOrLimitLeftToDo &&
      isLastPipe &&
      nothingToReturn
  }
}
