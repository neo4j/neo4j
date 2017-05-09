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
package org.neo4j.cypher.internal.compiler.v3_3.executionplan

import org.neo4j.cypher.internal.compiler.v3_3.spi.{GraphStatistics, PlanContext, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_3.{ExecutionMode, RuntimeName}
import org.neo4j.cypher.internal.frontend.v3_3.PlannerName
import org.neo4j.cypher.internal.frontend.v3_3.notification.InternalNotification

abstract class ExecutionPlan {
  def run(queryContext: QueryContext, planType: ExecutionMode, params: Map[String, Any]): InternalExecutionResult
  def isPeriodicCommit: Boolean
  def plannerUsed: PlannerName
  def isStale(lastTxId: () => Long, statistics: GraphStatistics): Boolean
  def runtimeUsed: RuntimeName
  def notifications(planContext: PlanContext): Seq[InternalNotification]
  def plannedIndexUsage: Seq[IndexUsage] = Seq.empty
}

sealed trait IndexUsage {
  def identifier:String
}

final case class SchemaIndexSeekUsage(identifier: String, label: String, propertyKeys: Seq[String]) extends IndexUsage
final case class SchemaIndexScanUsage(identifier: String, label: String, propertyKey: String) extends IndexUsage
final case class LegacyNodeIndexUsage(identifier: String, index: String) extends IndexUsage
final case class LegacyRelationshipIndexUsage(identifier: String, index: String) extends IndexUsage
