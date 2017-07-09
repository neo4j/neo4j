/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan

import org.neo4j.cypher.internal.InternalExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionMode, RuntimeName}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.IndexUsage
import org.neo4j.cypher.internal.compiler.v3_3.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.frontend.v3_3.PlannerName
import org.neo4j.cypher.internal.frontend.v3_3.notification.InternalNotification
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.values.AnyValue

abstract class ExecutionPlan {
  def run(queryContext: QueryContext, planType: ExecutionMode, params: Map[String, AnyValue]): InternalExecutionResult
  def isPeriodicCommit: Boolean
  def plannerUsed: PlannerName
  def isStale(lastTxId: () => Long, statistics: GraphStatistics): Boolean
  def runtimeUsed: RuntimeName
  def notifications(planContext: PlanContext): Seq[InternalNotification]
  def plannedIndexUsage: Seq[IndexUsage] = Seq.empty
}