/*
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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan

import org.neo4j.cypher.internal.compiler.v2_2.PreparedQuery
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.CantHandleQueryException
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext

class LegacyVsNewPipeBuilder(oldBuilder: PipeBuilder,
                             newBuilder: PipeBuilder,
                             monitor: NewLogicalPlanSuccessRateMonitor) extends PipeBuilder {
  def producePlan(inputQuery: PreparedQuery, planContext: PlanContext): PipeInfo = {
    val queryText = inputQuery.queryText
    val statement = inputQuery.statement
    try {
      monitor.newQuerySeen(queryText, statement)

      // Temporary measure, to save time compiling update queries
      if (containsUpdateClause(statement)) {
        throw new CantHandleQueryException("Ronja does not handle update queries yet.")
      }


      newBuilder.producePlan(inputQuery, planContext)
    } catch {
      case e: CantHandleQueryException =>
        monitor.unableToHandleQuery(queryText, statement, e)
        oldBuilder.producePlan(inputQuery, planContext)
    }
  }

  private def containsUpdateClause(s: Statement) = s.exists {
    case _: UpdateClause => true
  }
}
