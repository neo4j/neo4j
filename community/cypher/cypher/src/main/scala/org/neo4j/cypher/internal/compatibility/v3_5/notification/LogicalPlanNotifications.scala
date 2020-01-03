/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compatibility.v3_5.notification

import org.neo4j.cypher.internal.compiler.v3_5.CypherPlannerConfiguration
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_5.util.InternalNotification

trait NotificationChecker {
  def apply(plan: LogicalPlan): TraversableOnce[InternalNotification]
}

object LogicalPlanNotifications {
  def checkForNotifications(logicalPlan: LogicalPlan,
                            planContext: PlanContext,
                            config: CypherPlannerConfiguration): Seq[InternalNotification] = {
    val notificationCheckers = Seq(
      checkForEagerLoadCsv,
      checkForLoadCsvAndMatchOnLargeLabel(planContext, config.nonIndexedLabelWarningThreshold),
      checkForIndexLimitation(planContext))

    notificationCheckers.flatMap(_ (logicalPlan))
  }
}
