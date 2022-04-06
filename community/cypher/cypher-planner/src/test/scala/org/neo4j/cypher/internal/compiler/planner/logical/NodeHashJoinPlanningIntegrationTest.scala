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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NodeHashJoinPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport with AstConstructionTestSupport {

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

  test("Should plan sort (on top of apply) such that LeftOuterHashJoin can be unnested from apply") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(10)
      .build()

    val array = (1 to 10).mkString("[", ",", "]")

    val query =
      s"""
        |MATCH (n1)
        |UNWIND $array AS a0
        |OPTIONAL MATCH (n1), (n2)
        |WITH a0
        |RETURN a0 ORDER BY a0
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .sort(Seq(Ascending("a0")))
      .leftOuterHashJoin("n1")
      .|.cartesianProduct()
      .|.|.allNodeScan("n2")
      .|.filterExpression(assertIsNode("n1"))
      .|.allNodeScan("n1")
      .unwind(s"$array AS a0")
      .allNodeScan("n1")
      .build()
  }

  // TODO: This is a suggestion for a (better) plan that the planner currently isn't capable of producing.
  ignore("Should plan sort (under hash join) if LeftOuterHashJoin can be unnested from apply") {
    // The actual plan we get (sort on top of hash join) is correct, but sub-optimal.
    // While leftOuterHashJoin preserves ordering for RHS we still don't consider placing the sort under hash join.
    // That's because at the point in time when we plan a sort the hash join is in RHS of an apply, which only preserves order for the LHS.
    // It's only when the apply has been unnested that the alternative placement of sort is feasible (and better).
    val cfg = plannerBuilder()
      .setAllNodesCardinality(10)
      .setAllRelationshipsCardinality(100)
      .build()

    val array = (1 to 10).mkString("[", ",", "]")

    val query =
      s"""
         |MATCH (n1)
         |UNWIND $array AS a0
         |OPTIONAL MATCH (n1)--(n2)
         |USING JOIN ON n1
         |RETURN n2 ORDER BY n2
         |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .leftOuterHashJoin("n1")
      .|.sort(Seq(Ascending("n2")))
      .|.expandAll("(n1)-[anon_0]-(n2)")
      .|.allNodeScan("n1")
      .unwind(s"$array AS a0")
      .allNodeScan("n1")
      .build()
  }
}
