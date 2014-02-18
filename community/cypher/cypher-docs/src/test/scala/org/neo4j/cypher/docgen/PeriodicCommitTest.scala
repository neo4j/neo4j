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

import org.junit.{Ignore, Test}
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.internal.commons.CreateTempFileTestSupport

class PeriodicCommitTest
  extends DocumentingTestBase with QueryStatisticsTestSupport with CreateTempFileTestSupport {

  def section: String = "PERIODIC COMMIT"

  @Test def periodicCommit() {
    testQuery(
      title = "PERIODIC COMMIT",
      text = "PERIODIC COMMIT with a specified number of entity updates after which the transaction should be committed.",
      queryText = "USING PERIODIC COMMIT 500 FOREACH(id IN range(0, 10000) | CREATE (n:User {id: id}))",
      optionalResultExplanation = "",
      assertions = assertStatsResult(nodesCreated = 10001, labelsAdded = 10001, propertiesSet = 10001)(_)
    )
  }

  @Test def periodic_commit_default() {
    testQuery(
      title = "PERIODIC COMMIT",
      text = "PERIODIC COMMIT with no specified number of entity updates, using N as the default value.",
      queryText = "USING PERIODIC COMMIT FOREACH(id IN range(0, 10000) | CREATE (n:User {id: id}))",
      optionalResultExplanation = "",
      assertions = assertStatsResult(nodesCreated = 10001, labelsAdded = 10001, propertiesSet = 10001)(_)
    )
  }

  @Test def periodic_commit_with_load_csv() {
    val fileName = createTempFile("cypher", ".csv", { writer =>
      writer.println("name")
      writer.println("Davide")
      writer.println("Jakub")
      writer.println("Andres")
      writer.println("Stefan")
    })

    testQuery(
      title = "PERIODIC COMMIT",
      text = "Using PERIODIC COMMIT along with LOAD CSV",
      queryText = s"USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file://$fileName' AS line CREATE (n:User {name: line.name})",
      optionalResultExplanation = "",
      assertions = assertStatsResult(nodesCreated = 4, labelsAdded = 4, propertiesSet = 4)(_)
    )
  }

}
