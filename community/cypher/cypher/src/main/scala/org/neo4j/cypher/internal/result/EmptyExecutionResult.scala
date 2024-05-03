/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.result

import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.ExplainMode
import org.neo4j.cypher.internal.runtime.InternalQueryType
import org.neo4j.notifications.NotificationImplementation

abstract class EmptyExecutionResult(
  val fieldNames: Array[String],
  val planDescription: InternalPlanDescription,
  val queryType: InternalQueryType,
  val notifications: Set[NotificationImplementation]
) extends InternalExecutionResult {

  override def initiate(): Unit = {}

  override def isClosed: Boolean = true

  override def getError: Option[Throwable] = None

  override def close(reason: CloseReason): Unit = {}

  override def executionMode: ExecutionMode = ExplainMode

  override def executionPlanDescription(): InternalPlanDescription = planDescription
}
