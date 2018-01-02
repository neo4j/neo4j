/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.graphdb.Relationship
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class MergeNodeAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport
  with CypherComparisonSupport {

  // TODO: Reflect something like this in the TCK
  test("multiple merges after each other") {
    1 to 100 foreach { prop =>
      val result = executeWith(Configs.Interpreted - Configs.Cost2_3, s"merge (a:Label {prop: $prop}) return a.prop")
      assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 1)
    }
  }

  test("should not accidentally create relationship between wrong nodes after merge") {
    // Given
    val query =
      """
        |MERGE (a:A)
        |MERGE (b:B)
        |MERGE (c:C)
        |WITH a, b, c
        |CREATE (b)-[r:R]->(c)
        |RETURN r
      """.stripMargin

    // When
    val result = graph.execute(s"CYPHER runtime=slotted $query")

    // Then
    val row = result.next
    val r = row.get("r").asInstanceOf[Relationship]

    graph.inTx {
      val labelB = r.getStartNode.getLabels.iterator().next()
      val labelC = r.getEndNode.getLabels.iterator().next()
      labelB.name() shouldEqual ("B")
      labelC.name() shouldEqual ("C")
    }
  }
}
