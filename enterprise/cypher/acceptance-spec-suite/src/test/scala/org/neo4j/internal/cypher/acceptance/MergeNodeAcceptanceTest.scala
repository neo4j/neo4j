/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.graphdb.Relationship
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class MergeNodeAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport
  with CypherComparisonSupport {

  test("multiple merges after each other") {
    1 to 100 foreach { prop =>
      val result = executeWith(Configs.UpdateConf, s"merge (a:Label {prop: $prop}) return a.prop")
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

  test("Merging with self loop and relationship uniqueness") {
    graph.execute("CREATE (a) CREATE (a)-[:X]->(a)")
    val result = executeWith(Configs.UpdateConf, "MERGE (a)-[:X]->(b)-[:X]->(c) RETURN 42")
    assertStats(result, relationshipsCreated = 2, nodesCreated = 3)
  }

  test("Merging with self loop and relationship uniqueness - no stats") {
    graph.execute("CREATE (a) CREATE (a)-[:X]->(a)")
    val result = executeWith(Configs.UpdateConf, "MERGE (a)-[r1:X]->(b)-[r2:X]->(c) RETURN id(r1) = id(r2) as sameEdge")
    result.columnAs[Boolean]("sameEdge").toList should equal(List(false))
  }

  test("Merging with self loop and relationship uniqueness - no stats - reverse direction") {
    graph.execute("CREATE (a) CREATE (a)-[:X]->(a)")
    val result = executeWith(Configs.UpdateConf, "MERGE (a)-[r1:X]->(b)<-[r2:X]-(c) RETURN id(r1) = id(r2) as sameEdge")
    result.columnAs[Boolean]("sameEdge").toList should equal(List(false))
  }

  test("Merging with non-self-loop but require relationship uniqueness") {
    val a = createLabeledNode(Map("name" -> "a"), "A")
    val b = createLabeledNode(Map("name" -> "b"), "B")
    relate(a, b, "X")
    val result = executeWith(Configs.UpdateConf, "MERGE (a)-[r1:X]->(b)<-[r2:X]-(c) RETURN id(r1) = id(r2) as sameEdge, c.name as name")
    result.toList should equal(List(Map("sameEdge" -> false, "name" -> null)))
  }

  test("should give sensible error message on add relationship to null node") {
    val query =
      """OPTIONAL MATCH (a)
        |MERGE (a)-[r:X]->()
      """.stripMargin

    failWithError(Configs.AbsolutelyAll - Configs.Compiled - Configs.Cost2_3, query, Seq(
      "Expected to find a node, but found instead: null",
      "Expected to find a node at a but found nothing Some(null)",
      "Failed to create relationship `r`, node `a` is missing. " +
        "If you prefer to simply ignore rows where a relationship node is missing, " +
        "set 'cypher.lenient_create_relationship = true' in neo4j.conf"))
  }
}
