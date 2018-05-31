/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher

import org.neo4j.cypher.internal.runtime.{InternalExecutionResult, QueryStatistics}
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.scalatest.Assertions
import org.scalatest.mock.MockitoSugar

trait QueryStatisticsTestSupport extends MockitoSugar {
  self: Assertions =>

  implicit class QueryStatisticsAssertions(expected: QueryStatistics) {
    def apply(actual: QueryStatistics) {
      assertResult(expected)(actual)
    }

    def apply(actual: InternalExecutionResult) {
      implicit val monitor: QueryExecutionMonitor = new QueryExecutionMonitor {
        override def endSuccess(query: ExecutingQuery){}

        override def endFailure(query: ExecutingQuery, throwable: Throwable){}
      }
      apply(actual.queryStatistics())
    }
  }

  def assertStats(
                   result: InternalExecutionResult,
                   nodesCreated: Int = 0,
                   relationshipsCreated: Int = 0,
                   propertiesWritten: Int = 0,
                   nodesDeleted: Int = 0,
                   relationshipsDeleted: Int = 0,
                   labelsAdded: Int = 0,
                   labelsRemoved: Int = 0,
                   indexesAdded: Int = 0,
                   indexesRemoved: Int = 0,
                   uniqueConstraintsAdded: Int = 0,
                   uniqueConstraintsRemoved: Int = 0,
                   existenceConstraintsAdded: Int = 0,
                   existenceConstraintsRemoved: Int = 0,
                   nodekeyConstraintsAdded: Int = 0,
                   nodekeyConstraintsRemoved: Int = 0
  ): Unit = {
    assertStatsResult(
      nodesCreated,
      relationshipsCreated,
      propertiesWritten,
      nodesDeleted,
      relationshipsDeleted,
      labelsAdded,
      labelsRemoved,
      indexesAdded,
      indexesRemoved,
      uniqueConstraintsAdded,
      uniqueConstraintsRemoved,
      existenceConstraintsAdded,
      existenceConstraintsRemoved,
      nodekeyConstraintsAdded,
      nodekeyConstraintsRemoved
    )(result)
  }

  // This api is more in line with scala test assertions which prefer the expectation before the actual
  def assertStatsResult(nodesCreated: Int = 0,
                        relationshipsCreated: Int = 0,
                        propertiesWritten: Int = 0,
                        nodesDeleted: Int = 0,
                        relationshipsDeleted: Int = 0,
                        labelsAdded: Int = 0,
                        labelsRemoved: Int = 0,
                        indexesAdded: Int = 0,
                        indexesRemoved: Int = 0,
                        uniqueConstraintsAdded: Int = 0,
                        uniqueConstraintsRemoved: Int = 0,
                        existenceConstraintsAdded: Int = 0,
                        existenceConstraintsRemoved: Int = 0,
                        nodekeyConstraintsAdded: Int = 0,
                        nodekeyConstraintsRemoved: Int = 0
                       ): QueryStatisticsAssertions =
    QueryStatistics(
      nodesCreated,
      relationshipsCreated,
      propertiesWritten,
      nodesDeleted,
      relationshipsDeleted,
      labelsAdded,
      labelsRemoved,
      indexesAdded,
      indexesRemoved,
      uniqueConstraintsAdded,
      uniqueConstraintsRemoved,
      existenceConstraintsAdded,
      existenceConstraintsRemoved,
      nodekeyConstraintsAdded,
      nodekeyConstraintsRemoved
    )
}
