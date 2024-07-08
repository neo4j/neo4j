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
import org.neo4j.graphdb.GqlStatusObject
import org.neo4j.kernel.impl.query.QuerySubscriber

class FailedExecutionResult(fieldNames: Array[String], queryType: InternalQueryType, subscriber: QuerySubscriber)
    extends EmptyExecutionResult(
      fieldNames,
      InternalPlanDescription.error("Query has failed, no plan available"),
      queryType,
      Set.empty
    ) {

  override def request(numberOfRecords: Long): Unit = {
    // do nothing
  }

  override def cancel(): Unit = {
    // do nothing
  }

  override def await(): Boolean = false

  override def gqlStatusObjects: Iterable[GqlStatusObject] = Seq.empty[GqlStatusObject]
}
