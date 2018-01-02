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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.neo4j.cypher.internal.compiler.v2_3.pipes.PipeMonitor
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.InternalException

trait Phase {
  self =>

  def myBuilders: Seq[PlanBuilder]

  def apply(inPlan: ExecutionPlanInProgress, context: PlanContext)(implicit pipeMonitor:PipeMonitor): ExecutionPlanInProgress = {
    var plan = inPlan
    while (myBuilders.exists(_.canWorkWith(plan, context))) {
      val matchingBuilders = myBuilders.filter(_.canWorkWith(plan, context))

      val builder = matchingBuilders.head

      val newPlan = builder(plan, context)

      if (plan == newPlan)
        throw new InternalException("Something went wrong trying to build your query. The offending builder was: "
          + builder.getClass.getSimpleName)

      plan = newPlan
    }

    plan
  }

  def andThen(other: Phase) = new Phase {
    def myBuilders: Seq[PlanBuilder] = self.myBuilders ++ other.myBuilders

    override def apply(inPlan: ExecutionPlanInProgress, context: PlanContext)(implicit pipeMonitor: PipeMonitor): ExecutionPlanInProgress = {
      val firstStep = self(inPlan, context)
      other(firstStep, context)
    }
  }
}

