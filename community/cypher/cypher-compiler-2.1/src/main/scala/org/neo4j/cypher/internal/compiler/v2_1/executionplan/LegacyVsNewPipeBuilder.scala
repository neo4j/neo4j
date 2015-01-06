/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.executionplan

import org.neo4j.cypher.internal.compiler.v2_1.PreparedQuery
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_1.planner.CantHandleQueryException

class LegacyVsNewPipeBuilder(oldBuilder: PipeBuilder,
                             newBuilder: PipeBuilder,
                             monitor: NewQueryPlanSuccessRateMonitor) extends PipeBuilder {
  def producePlan(inputQuery: PreparedQuery, planContext: PlanContext): PipeInfo = {
    val queryText = inputQuery.queryText
    try {
      monitor.newQuerySeen(queryText, inputQuery.statement)
      newBuilder.producePlan(inputQuery, planContext)
    } catch {
      case e: CantHandleQueryException =>
        monitor.unableToHandleQuery(queryText, inputQuery.statement, e)
        oldBuilder.producePlan(inputQuery, planContext)
    }
  }
}
