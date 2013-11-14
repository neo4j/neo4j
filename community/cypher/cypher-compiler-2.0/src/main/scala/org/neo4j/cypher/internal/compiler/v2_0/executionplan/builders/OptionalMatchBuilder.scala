/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{ExecutionPlanInProgress, PlanBuilder, Phase}
import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_0.pipes.{QueryState, Pipe}
import org.neo4j.cypher.internal.compiler.v2_0.pipes.optional.InsertingPipe
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext

case class OptionalMatchBuilder(matching: Phase) extends PlanBuilder {
  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext): Boolean = plan.query.optional

  def apply(in: ExecutionPlanInProgress, context: PlanContext): ExecutionPlanInProgress = {
    var planInProgress: ExecutionPlanInProgress = in.copy(query = in.query.copy(optional = false))

    def builder(in: Pipe): Pipe = {
      planInProgress = planInProgress.copy(pipe = in)
      planInProgress = matching(planInProgress, context)
      planInProgress.pipe
    }

    def createNullValues(in: ExecutionContext, addedIdentifiers: Seq[String], ignored: QueryState): ExecutionContext = {
      val nulls = addedIdentifiers.map(_ -> null).toMap
      in.newWith(nulls)
    }

    val nullingPipe = new InsertingPipe(planInProgress.pipe, builder, createNullValues)
    planInProgress.copy(pipe = nullingPipe)
  }
}
