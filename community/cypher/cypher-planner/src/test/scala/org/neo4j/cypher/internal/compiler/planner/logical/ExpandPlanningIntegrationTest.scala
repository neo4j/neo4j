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
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

class ExpandPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  test("Should build plans containing expand for single relationship pattern") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]-()", 50)
      .build()

    val plan = cfg.plan("MATCH (a)-[r]->(b) RETURN r").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .expandAll("(a)-[r]->(b)")
      .allNodeScan("a")
      .build()
  }

  test("Should build plans containing expand for two unrelated relationship patterns") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setLabelCardinality("A", 1000)
      .setLabelCardinality("B", 2000)
      .setLabelCardinality("C", 3000)
      .setLabelCardinality("D", 4000)
      .setRelationshipCardinality("(:A)-[]-(:B)", 100)
      .setRelationshipCardinality("(:A)-[]->()", 100)
      .setRelationshipCardinality("()-[]->(:B)", 100)
      .setRelationshipCardinality("(:C)-[]->(:D)", 100)
      .setRelationshipCardinality("(:C)-[]->()", 100)
      .setRelationshipCardinality("()-[]->(:D)", 100)
      .build()

    val plan = cfg.plan("MATCH (a:A)-[r1]->(b:B), (c:C)-[r2]->(d:D) RETURN r1, r2").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .filter("not r1 = r2")
      .cartesianProduct()
      .|.filter("d:D")
      .|.expandAll("(c)-[r2]->(d)")
      .|.nodeByLabelScan("c", "C")
      .filter("b:B")
      .expandAll("(a)-[r1]->(b)")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("Should build plans containing expand for self-referencing relationship patterns") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]-()", 50)
      .build()

    val plan = cfg.plan("MATCH (a)-[r]->(a) RETURN r").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .expandInto("(a)-[r]->(a)")
      .allNodeScan("a")
      .build()
  }

  test("Should build plans containing expand for looping relationship patterns") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]->()", 20)
      .build()

    val plan = cfg.plan("MATCH (a)-[r1]->(b)<-[r2]-(a) RETURN r1, r2").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .filter("not r1 = r2")
      .expandInto("(a)-[r1]->(b)")
      .expandAll("(a)-[r2]->(b)")
      .allNodeScan("a")
      .build()
  }

  test("Should build plans expanding from the cheaper side for single relationship pattern") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:X]-()", 500)
      .build()

    val plan = cfg.plan("MATCH (start)-[rel:X]-(a) WHERE a.name = 'Andres' RETURN a").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .expandAll("(a)-[rel:X]-(start)")
      .filter("a.name = 'Andres'")
      .allNodeScan("a")
      .build()
  }

  test("Should build plans expanding from the more expensive side if that is requested by using a hint") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(2000)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("Person", 1000)
      .setRelationshipCardinality("(:A)-[]->(:Person)", 10)
      .setRelationshipCardinality("(:A)-[]->()", 10)
      .setRelationshipCardinality("()-[]->(:Person)", 500)
      .addNodeIndex("Person", Seq("name"), existsSelectivity = 1.0, uniqueSelectivity = 0.1)
      .build()

    val plan = cfg.plan(
      "MATCH (a:A)-[r]->(b) USING INDEX b:Person(name) WHERE b:Person AND b.name = 'Andres' return r"
    ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .filter("a:A")
      .expandAll("(b)<-[r]-(a)")
      .nodeIndexOperator("b:Person(name = 'Andres')", indexType = IndexType.RANGE)
      .build()
  }

  test("should plan typed expand with not-inlined type predicate") {
    val query =
      """MATCH (a)-[r]->(b)
        |WHERE r:REL
        |RETURN a, b, r""".stripMargin

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]->()", 10)
      .removeRelationshipLookupIndex() // We want to test Expand
      .build()

    val plan = cfg
      .plan(query)
      .stripProduceResults

    val expectedPlan = cfg.subPlanBuilder()
      .expand("(a)-[r:REL]->(b)")
      .allNodeScan("a")
      .build()

    plan shouldEqual expectedPlan
  }

  test("should consider dependency to target node when extracting predicates for var length expand") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(1000)
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("(:A)-[]->()", 100)
      .build()

    val plan = planner.plan("MATCH (a:A)-[r* {aProp: a.prop, bProp: b.prop}]->(b) RETURN a, b")

    plan shouldBe planner.planBuilder()
      .produceResults("a", "b")
      // this filter should go on top as we do not know the node b before finishing the expand.
      .filter("all(anon_0 IN r WHERE anon_0.bProp = b.prop)")
      .expand("(a)-[r*1..]->(b)", relationshipPredicate = Predicate("r_RELS", "r_RELS.aProp = a.prop"))
      .nodeByLabelScan("a", "A")
      .build()
  }
}
