/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NodeHashJoinPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {

  test("should build plans containing joins") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setLabelCardinality("X", 100)
      .setRelationshipCardinality("()-[]->(:X)", 100)
      .setRelationshipCardinality("()-[]->()", Double.MaxValue)
      .build()

    val plan = cfg.plan("MATCH (a:X)<-[r1]-(b)-[r2]->(c:X) RETURN b").stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .filter("not r1 = r2")
      .nodeHashJoin("b")
      .|.expandAll("(c)<-[r2]-(b)")
      .|.nodeByLabelScan("c", "X")
      .expandAll("(a)<-[r1]-(b)")
      .nodeByLabelScan("a", "X")
      .build()
  }

  test("should plan hash join when join hint is used") {
    val cypherQuery = """
                        |MATCH (a:A)-[r1:X]->(b)-[r2:X]->(c:C)
                        |USING JOIN ON b
                        |WHERE a.prop = c.prop
                        |RETURN b""".stripMargin

    val cfg = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setLabelCardinality("A", 100)
      .setLabelCardinality("C", 100)
      .setLabelCardinality("X", 100)
      .setRelationshipCardinality("(:A)-[:X]->()", 100)
      .setRelationshipCardinality("()-[:X]->()", 200)
      .setRelationshipCardinality("()-[:X]->(:C)", 100)
      .setRelationshipCardinality("()-[]->()", Double.MaxValue)
      .build()

    val plan = cfg.plan(cypherQuery).stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .filter("a.prop = c.prop", "not r1 = r2")
      .nodeHashJoin("b")
      .|.expandAll("(c)<-[r2:X]-(b)")
      .|.nodeByLabelScan("c", "C")
      .expandAll("(a)-[r1:X]->(b)")
      .nodeByLabelScan("a", "A")
      .build()
  }
}
