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
package org.neo4j.cypher.internal.result

import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.{ExecutionMode, ExplainMode, InternalQueryType, QueryStatistics}
import org.neo4j.graphdb.{Notification, Result}

abstract class EmptyExecutionResult(val fieldNames: Array[String],
                                    val planDescription: InternalPlanDescription,
                                    val queryType: InternalQueryType,
                                    val notifications: Set[Notification])
  extends InternalExecutionResult {

  override def initiate(): Unit = {}

  override def isClosed: Boolean = true

  override def close(reason: CloseReason): Unit = {}

  override def executionMode: ExecutionMode = ExplainMode

  override def executionPlanDescription(): InternalPlanDescription = planDescription

  override def isVisitable: Boolean = false

  override def accept[VisitationException <: Exception](visitor: Result.ResultVisitor[VisitationException]): QueryStatistics =
    throw new IllegalStateException("EmptyExecutionResult is not visitable")
}
