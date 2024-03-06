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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ProjectEndpointsPlanningIntegrationTest extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport {

  private val planner = plannerBuilder()
    .setAllNodesCardinality(10)
    .setAllRelationshipsCardinality(10)
    .build()

  for {
    startInScope <- Seq(true, false)
    endInScope <- Seq(true, false)
    direction <- Seq(SemanticDirection.OUTGOING, SemanticDirection.INCOMING, SemanticDirection.BOTH)
    varLength <- Seq(true, false)
  } {
    val projectedStart = if (startInScope) "a1" else "a2"
    val projectedEnd = if (endInScope) "b1" else "b2"

    val (arrow1, arrow2) = direction match {
      case SemanticDirection.OUTGOING => ("-", "->")
      case SemanticDirection.INCOMING => ("<-", "-")
      case SemanticDirection.BOTH     => ("-", "-")
    }

    val relStar = if (varLength) "*" else ""
    var predicateStrings = Seq("all(anon_0 IN r WHERE single(anon_1 IN r WHERE anon_0 = anon_1))")
    if (varLength) {
      predicateStrings = predicateStrings :+ "size(r) >= 1"
    }

    val relString = s"($projectedStart)$arrow1[r$relStar]$arrow2($projectedEnd)"
    val query = s"MATCH (a1)-[r${relStar}]->(b1) WITH * LIMIT 1 MATCH $relString RETURN *"

    test(s"should build ProjectEndpoints, startInScope=$startInScope, endInScope=$endInScope, query: $query") {

      val plan = planner.plan(query).stripProduceResults

      val expectedSecondHalf = planner.subPlanBuilder()
        .apply()
        .|.projectEndpoints(relString, startInScope = startInScope, endInScope = endInScope)

      val expectedPlan =
        if (varLength) {
          expectedSecondHalf
            .|.filter(predicateStrings: _*)
            .|.argument("a1", "r", "b1")
            .limit(1)
            .expandAll(s"(a1)-[r$relStar]->(b1)")
            .allNodeScan("a1")
            .build()
        } else {
          expectedSecondHalf
            .|.argument("a1", "r", "b1")
            .limit(1)
            .allRelationshipsScan("(a1)-[r]->(b1)")
            .build()
        }

      plan should equal(expectedPlan)
    }
  }

  test("should build optional ProjectEndpoints none in scope with extra predicates") {
    val query = "MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2"
    val plan = planner.plan(query).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.optional("r", "a1")
        .|.filter("a1 = a2")
        .|.projectEndpoints("(a2)<-[r]-(b2)", startInScope = false, endInScope = false)
        .|.argument("r", "a1")
        .limit(1)
        .allRelationshipsScan("(a1)-[r]->(b1)")
        .build()
    )
  }

  test("should test a var-length relationship that came from a made up list for uniqueness ") {
    val plan = planner.plan("MATCH ()-[r]->() MATCH ()-[q]->() WITH [r,q] AS rs MATCH (a)-[rs*]->(b) RETURN a, b")

    plan should equal(
      planner.planBuilder()
        .produceResults("a", "b")
        .apply()
        .|.projectEndpoints("(a)-[rs*1..]->(b)", startInScope = false, endInScope = false)
        .|.filter("size(rs) >= 1", "all(anon_4 IN rs WHERE single(anon_5 IN rs WHERE anon_4 = anon_5))")
        .|.argument("rs")
        .projection("[r, q] AS rs")
        .cartesianProduct()
        .|.allRelationshipsScan("(anon_2)-[q]->(anon_3)")
        .allRelationshipsScan("(anon_0)-[r]->(anon_1)")
        .build()
    )
  }

  test("Don't plan 0 length project endpoints - relationships in different query graphs") {
    val query = "MATCH (a1)-[rs*0..10]->(b1) WITH rs LIMIT 1 MATCH (a2)-[rs*0..10]-(b2) RETURN a2, rs, b2"
    val plan = planner.plan(query).stripProduceResults

    // There are improvements available for the plan below. First of, one could use a ValueHashJoin
    // on rs=anon_0. Another variant is the following:
    //
    //        .apply()
    //        .|.union()
    //        .|.|.projection("a2 as b2")
    //        .|.|.filter("size(rs) = 0")
    //        .|.|.allNodeScan("a2", "rs")
    //        .|.projectEndpoints("(a2)-[rs*1..10]-(b2)")
    //        .|.filter("size(rs) > 0")
    //        .|.arguments("rs")
    //        .expandAll(s"(a1)-[rs*0..10]->(b1)")
    //        .allNodeScan("a1")
    //        .build()
    //
    // which works since calling projectEndpoints on a 0-length path is equivalent to scanning for
    // all node (n) and returning the pairs (n, n)

    plan should equal(
      planner.subPlanBuilder()
        .filter("rs = anon_0")
        .expandAll(s"(a2)-[anon_0*0..10]-(b2)")
        .filter("all(anon_1 IN rs WHERE single(anon_2 IN rs WHERE anon_1 = anon_2))", "size(rs) >= 0", "size(rs) <= 10")
        .apply()
        .|.allNodeScan("a2", "rs")
        .limit(1)
        .expandAll(s"(a1)-[rs*0..10]->(b1)")
        .allNodeScan("a1")
        .build()
    )
  }

  test("Don't plan 0 length project endpoints - relationships in the same query graph") {
    val query = "MATCH (a1:A)-[rs*0..1]->(b1) MATCH (a2:AA)-[rs*0..1]-(b2) RETURN a2, rs, b2"

    val planner = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setAllRelationshipsCardinality(10000)
      .setLabelCardinality("A", 1000)
      .setLabelCardinality("AA", 5000)
      .setRelationshipCardinality("(:A)-[]->()", 100)
      .setRelationshipCardinality("(:AA)-[]->()", 100)
      .setRelationshipCardinality("()-[]->(:AA)", 100)
      .build()

    val plan = planner.plan(query).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .valueHashJoin("anon_0 = rs")
        .|.expand("(a2)-[rs*0..1]-(b2)")
        .|.nodeByLabelScan("a2", "AA")
        .expand("(a1)-[anon_0*0..1]->(b1)")
        .nodeByLabelScan("a1", "A")
        .build()
    )
  }
}
