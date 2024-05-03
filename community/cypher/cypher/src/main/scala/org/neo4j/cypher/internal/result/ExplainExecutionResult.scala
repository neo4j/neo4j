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
import org.neo4j.cypher.internal.runtime.InternalQueryType
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.notifications.NotificationImplementation

class ExplainExecutionResult(
  fieldNames: Array[String],
  planDescription: InternalPlanDescription,
  queryType: InternalQueryType,
  notifications: Set[NotificationImplementation],
  subscriber: QuerySubscriber
) extends EmptyExecutionResult(fieldNames, planDescription, queryType, notifications) {

  override def request(numberOfRecords: Long): Unit = {
    subscriber.onResult(0)
    subscriber.onResultCompleted(QueryStatistics())
  }

  override def cancel(): Unit = {
    // do nothing
  }

  override def await(): Boolean = false
}
