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

import org.neo4j.cypher.internal.compiler.v2_3.planner.CantHandleQueryException
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_3.{CompilationPhaseTracer, PreparedQuery}
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.notification.PlannerUnsupportedNotification

trait FallbackBuilder extends ExecutablePlanBuilder {

  def producePlan(inputQuery: PreparedQuery, planContext: PlanContext,
                  tracer: CompilationPhaseTracer) = {
    val queryText = inputQuery.queryText
    val statement = inputQuery.statement
    try {
      monitor.newQuerySeen(queryText, statement)

      // Temporary measure, to save time compiling update queries
      if (containsUpdateClause(statement)) {
        throw new CantHandleQueryException("Ronja does not handle update queries yet.")
      }

      newBuilder.producePlan(inputQuery, planContext, tracer)
    } catch {
      case e: CantHandleQueryException =>
        monitor.unableToHandleQuery(queryText, statement, e)
        warn(inputQuery)
        oldBuilder.producePlan(inputQuery, planContext, tracer)
    }
  }

  private def containsUpdateClause(s: Statement) = s.exists {
    case _: UpdateClause => true
  }

  def oldBuilder: ExecutablePlanBuilder

  def newBuilder: ExecutablePlanBuilder

  def monitor: NewLogicalPlanSuccessRateMonitor

  def warn(preparedQuery: PreparedQuery): Unit

}

case class SilentFallbackPlanBuilder(oldBuilder: ExecutablePlanBuilder,
                                     newBuilder: ExecutablePlanBuilder,
                                     monitor: NewLogicalPlanSuccessRateMonitor) extends FallbackBuilder {

  override def warn(preparedQuery: PreparedQuery): Unit = {}
}

case class WarningFallbackPlanBuilder(oldBuilder: ExecutablePlanBuilder,
                                      newBuilder: ExecutablePlanBuilder,
                                      monitor: NewLogicalPlanSuccessRateMonitor) extends FallbackBuilder {

  override def warn(preparedQuery: PreparedQuery): Unit = preparedQuery.notificationLogger
    .log(PlannerUnsupportedNotification)
}

