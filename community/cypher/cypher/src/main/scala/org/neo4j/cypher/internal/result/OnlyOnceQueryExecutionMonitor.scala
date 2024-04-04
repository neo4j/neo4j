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

import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.QueryExecutionMonitor

case class OnlyOnceQueryExecutionMonitor(monitor: QueryExecutionMonitor) extends QueryExecutionMonitor {
  private var closed = false

  override def endFailure(query: ExecutingQuery, failure: Throwable): Unit =
    if (!closed) {
      closed = true
      monitor.endFailure(query, failure)
    }

  override def endFailure(query: ExecutingQuery, reason: String, status: Status): Unit =
    if (!closed) {
      closed = true
      monitor.endFailure(query, reason, status)
    }

  override def endSuccess(query: ExecutingQuery): Unit =
    if (!closed) {
      closed = true
      monitor.endSuccess(query)
    }

  override def startProcessing(query: ExecutingQuery): Unit =
    monitor.startProcessing(query)

  override def startExecution(query: ExecutingQuery): Unit =
    monitor.startExecution(query)

  override def beforeEnd(query: ExecutingQuery, success: Boolean): Unit =
    monitor.beforeEnd(query, success)
}
