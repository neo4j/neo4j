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

import org.neo4j.cypher.internal.QueryTypeConversion
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.ExplainMode
import org.neo4j.cypher.internal.runtime.InternalQueryType
import org.neo4j.cypher.internal.runtime.NormalMode
import org.neo4j.cypher.internal.runtime.ProfileMode
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.QueryExecutionType
import org.neo4j.kernel.impl.query.QueryExecution
import org.neo4j.notifications.NotificationImplementation

import java.lang

import scala.jdk.CollectionConverters.IterableHasAsJava

trait InternalExecutionResult extends QueryExecution {

  /**
   * Perform any initial logic, such a materialization and early closing.
   */
  def initiate(): Unit

  def executionMode: ExecutionMode

  def queryType: InternalQueryType

  def notifications: Iterable[NotificationImplementation]

  def getError: Option[Throwable]

  override def getNotifications: lang.Iterable[Notification] = notifications.asInstanceOf[Iterable[Notification]].asJava

  def executionType: QueryExecutionType = {
    val qt = QueryTypeConversion.asPublic(queryType)
    executionMode match {
      case ExplainMode => QueryExecutionType.explained(qt)
      case ProfileMode => QueryExecutionType.profiled(qt)
      case NormalMode  => QueryExecutionType.query(qt)
    }
  }

  def isClosed: Boolean

  def close(reason: CloseReason): Unit

  def close(): Unit = close(Success)
}

trait AsyncCleanupOnClose {
  def registerOnFinishedCallback(callback: () => Unit): Unit
}
