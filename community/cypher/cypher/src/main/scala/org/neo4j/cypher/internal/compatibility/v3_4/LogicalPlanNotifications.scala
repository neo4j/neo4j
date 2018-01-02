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
package org.neo4j.cypher.internal.compatibility.v3_4

import org.neo4j.cypher.internal.compiler.v3_4.CypherCompilerConfiguration
import org.neo4j.cypher.internal.frontend.v3_4.notification.InternalNotification
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

object LogicalPlanNotifications {
  def checkForNotifications(logicalPlan: LogicalPlan,
                            planContext: PlanContext,
                            config: CypherCompilerConfiguration): Seq[InternalNotification] = {
    val notificationCheckers = Seq(checkForEagerLoadCsv,
      CheckForLoadCsvAndMatchOnLargeLabel(planContext, config.nonIndexedLabelWarningThreshold))

    notificationCheckers.flatMap(_ (logicalPlan))
  }
}
