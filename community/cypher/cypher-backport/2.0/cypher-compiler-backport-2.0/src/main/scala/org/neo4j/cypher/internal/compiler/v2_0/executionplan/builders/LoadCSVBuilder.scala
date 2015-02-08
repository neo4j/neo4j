/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_0.pipes.{HasHeaders, NoHeaders, LoadCSVPipe}
import org.neo4j.cypher.internal.compiler.v2_0.commands.LoadCSV

class LoadCSVBuilder extends PlanBuilder {


  override def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext): Boolean = {
    findLoadCSVItem(plan).isDefined
  }

  private def findLoadCSVItem(plan: ExecutionPlanInProgress): Option[LoadCSV] = {
    plan.query.start.collectFirst {
      case Unsolved(item: LoadCSV) => item
    }
  }

  override def apply(plan: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress = {
    val item = findLoadCSVItem(plan).get
    plan.copy(
      query = plan.query.copy(start = plan.query.start.replace(Unsolved(item), Solved(item))),
      pipe = new LoadCSVPipe(plan.pipe, if (item.withHeaders) HasHeaders else NoHeaders, item.fileUrl, item.identifier)
    )
  }
}
