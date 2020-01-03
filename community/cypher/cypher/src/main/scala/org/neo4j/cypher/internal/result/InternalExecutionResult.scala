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

import java.lang

import org.neo4j.cypher.internal.QueryTypeConversion
import org.neo4j.cypher.internal.runtime._
import org.neo4j.graphdb.{Notification, QueryExecutionType}
import org.neo4j.kernel.impl.query.QueryExecution

import scala.collection.JavaConverters._

trait InternalExecutionResult extends QueryExecution {

  /**
    * Perform any initial logic, such a materialization and early closing.
    */
  def initiate(): Unit

  def executionMode: ExecutionMode

  def queryType: InternalQueryType

  def notifications: Iterable[Notification]

  override def getNotifications: lang.Iterable[Notification] = notifications.asJava

  def executionType: QueryExecutionType = {
    val qt = QueryTypeConversion.asPublic(queryType)
    executionMode match {
      case ExplainMode => QueryExecutionType.explained(qt)
      case ProfileMode => QueryExecutionType.profiled(qt)
      case NormalMode => QueryExecutionType.query(qt)
    }
  }

  def isClosed: Boolean

  def close(reason: CloseReason): Unit

  def close(): Unit = close(Success)
}

sealed trait CloseReason
case object Success extends CloseReason
case object Failure extends CloseReason
case class Error(t: Throwable) extends CloseReason

