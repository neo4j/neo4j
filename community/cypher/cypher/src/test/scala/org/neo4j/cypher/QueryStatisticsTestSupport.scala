/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher

import org.neo4j.cypher.internal.compatability.ExecutionResultWrapperFor2_2
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.InternalExecutionResult
import org.scalatest.Assertions

trait QueryStatisticsTestSupport {
  self: Assertions =>

  implicit class QueryStatisticsAssertions(expected: QueryStatistics) {
    def apply(actual: QueryStatistics) {
      assertResult(expected)(actual)
    }

    def apply(actual: InternalExecutionResult) {
      val r = new ExecutionResultWrapperFor2_2(actual, CypherVersion.v2_2)
      apply(r.queryStatistics())
    }
  }

  def assertStats(
    result: InternalExecutionResult,
    nodesCreated: Int = 0,
    relationshipsCreated: Int = 0,
    propertiesSet: Int = 0,
    nodesDeleted: Int = 0,
    relationshipsDeleted: Int = 0,
    labelsAdded: Int = 0,
    labelsRemoved: Int = 0,
    indexesAdded: Int = 0,
    indexesRemoved: Int = 0,
    constraintsAdded: Int = 0,
    constraintsRemoved: Int = 0
  ) = {
    assertStatsResult(
      nodesCreated,
      relationshipsCreated,
      propertiesSet,
      nodesDeleted,
      relationshipsDeleted,
      labelsAdded,
      labelsRemoved,
      indexesAdded,
      indexesRemoved,
      constraintsAdded,
      constraintsRemoved
    )(result)
  }

  // This api is more in line with scala test assertions which prefer the expectation before the actual
  def assertStatsResult(
    nodesCreated: Int = 0,
    relationshipsCreated: Int = 0,
    propertiesSet: Int = 0,
    nodesDeleted: Int = 0,
    relationshipsDeleted: Int = 0,
    labelsAdded: Int = 0,
    labelsRemoved: Int = 0,
    indexesAdded: Int = 0,
    indexesRemoved: Int = 0,
    constraintsAdded: Int = 0,
    constraintsRemoved: Int = 0
  ): QueryStatisticsAssertions = QueryStatistics(
    nodesCreated,
    relationshipsCreated,
    propertiesSet,
    nodesDeleted,
    relationshipsDeleted,
    labelsAdded,
    labelsRemoved,
    indexesAdded,
    indexesRemoved,
    constraintsAdded,
    constraintsRemoved
  )
}
