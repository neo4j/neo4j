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
package org.neo4j.cypher

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.scalatest.Assertions

trait QueryStatisticsTestSupport {
  self: Assertions =>

  def assertStats(
    result: RewindableExecutionResult,
    nodesCreated: Long = 0L,
    relationshipsCreated: Long = 0L,
    propertiesWritten: Long = 0L,
    nodesDeleted: Long = 0L,
    relationshipsDeleted: Long = 0L,
    labelsAdded: Long = 0L,
    labelsRemoved: Long = 0L,
    indexesAdded: Int = 0,
    indexesRemoved: Int = 0,
    nodePropUniquenessConstraintsAdded: Int = 0,
    relPropUniquenessConstraintsAdded: Int = 0,
    nodePropExistenceConstraintsAdded: Int = 0,
    relPropExistenceConstraintsAdded: Int = 0,
    nodePropTypeConstraintsAdded: Int = 0,
    relPropTypeConstraintsAdded: Int = 0,
    nodekeyConstraintsAdded: Int = 0,
    relkeyConstraintsAdded: Int = 0,
    nodeLabelExistenceConstraintsAdded: Int = 0,
    relSourceLabelConstraintsAdded: Int = 0,
    relTargetLabelConstraintsAdded: Int = 0,
    constraintsRemoved: Int = 0,
    transactionsCommitted: Long = 0L,
    transactionsStarted: Long = 0L,
    transactionsRolledBack: Long = 0L,
    fileLinesRead: Long = 0L
  ): Unit = {
    val expected =
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
        nodePropUniquenessConstraintsAdded,
        relPropUniquenessConstraintsAdded,
        nodePropExistenceConstraintsAdded,
        relPropExistenceConstraintsAdded,
        nodePropTypeConstraintsAdded,
        relPropTypeConstraintsAdded,
        nodekeyConstraintsAdded,
        relkeyConstraintsAdded,
        nodeLabelExistenceConstraintsAdded,
        relSourceLabelConstraintsAdded,
        relTargetLabelConstraintsAdded,
        constraintsRemoved,
        transactionsStarted,
        transactionsCommitted,
        transactionsRolledBack,
        fileLinesRead
      )

    assertResult(expected)(result.queryStatistics())
  }
}
