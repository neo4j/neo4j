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
package org.neo4j.cypher.docgen

import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}
import org.junit.Test
import java.io.File

class UsingPeriodicCommitTest extends DocumentingTestBase with QueryStatisticsTestSupport {
  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  def section = "Using Periodic Commit"

  @Test def simple_periodic_commit() {
    testQuery(
      title = "USING PERIODIC COMMIT",
      text = "USING PERIODIC COMMIT is a hint to Neo4j to periodically commit changes in a query",
      queryText = s"USING PERIODIC COMMIT MATCH (n) CREATE (n)-[:NEW]->()",
      optionalResultExplanation =
        "By default, a transaction will be committed every 10000 changes to the database.",
      assertions = (p) => { assertStats(p, nodesCreated = 0, propertiesSet = 0, labelsAdded = 0) })
  }

  @Test def explicit_periodic_commit() {
    testQuery(
      title = "USING PERIODIC COMMIT with explicit size",
      text = "If you need to tweak how often a transaction should be committed",
      queryText = s"USING PERIODIC COMMIT 100 MATCH (n) CREATE (n)-[:NEW]->()",
      optionalResultExplanation =
        "You can specify an explicit batch size if necessary.",
      assertions = (p) => { assertStats(p, nodesCreated = 0, propertiesSet = 0, labelsAdded = 0) })
  }
}
