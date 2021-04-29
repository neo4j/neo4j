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
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RelationshipIndexScanPlanningIntegrationTest extends CypherFunSuite
                                                   with LogicalPlanningIntegrationTestSupport
                                                   with AstConstructionTestSupport {

  test("should plan undirected relationship index scan with IS NOT NULL") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .enablePlanningRelationshipIndexes()
      .build()

    planner.plan("MATCH (a)-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)")
        .build()
    )
  }

  test("should plan directed OUTGOING relationship index scan with IS NOT NULL") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .enablePlanningRelationshipIndexes()
      .build()

    planner.plan("MATCH (a)-[r:REL]->(b) WHERE r.prop IS NOT NULL RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)")
        .build()
    )
  }

  test("should plan directed INCOMING relationship index scan with IS NOT NULL") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .enablePlanningRelationshipIndexes()
      .build()

    planner.plan("MATCH (a)<-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .relationshipIndexOperator("(a)<-[r:REL(prop)]-(b)")
        .build()
    )
  }

  test("should plan undirected relationship index scan with IS NOT NULL on the RHS of an Apply with correct arguments") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(1)
      .setRelationshipCardinality("()-[:REL]-()", 1)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .enablePlanningRelationshipIndexes()
      .build()

    planner.plan(
      """MATCH (n)
        |CALL {
        |  WITH n
        |  MATCH (a)-[r:REL]-(b)
        |  WHERE r.prop IS NOT NULL AND b.prop = n.prop
        |  RETURN r
        |}
        |RETURN n, r
        |""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("n", "r")
        .filter("b.prop = cacheN[n.prop]")
        .apply(fromSubquery = true)
        .|.relationshipIndexOperator("(a)-[r:REL(prop)]-(b)", argumentIds = Set("n"))
        .cacheProperties("cacheNFromStore[n.prop]")
        .allNodeScan("n")
        .build()
    )
  }

  test("should plan undirected relationship index scan over NodeByLabelScan using a hint") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setLabelCardinality("A", 10)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .setRelationshipCardinality("(:A)-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .enablePlanningRelationshipIndexes()
      .build()

    planner.plan("MATCH (a:A)-[r:REL]-(b) USING INDEX r:REL(prop) WHERE r.prop IS NOT NULL RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .filterExpression(hasLabels("a", "A")) // TODO change to .filter("a:A") after https://github.com/neo-technology/neo4j/pull/9823 is merged
        .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)")
        .build()
    )
  }

  test("should not plan relationship index scan when not enabled") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .build()

    withClue("Used relationship index even when not enabled:") {
      planner.plan("MATCH (a)-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN r").treeExists {
        case _: UndirectedRelationshipIndexScan => true
        case _: DirectedRelationshipIndexScan => true
      } should be(false)
    }
  }

  test("should not (yet) plan relationship index scan with filter for already bound start node") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .enablePlanningRelationshipIndexes()
      .enableRelationshipTypeScanStore()
      .build()

    planner.plan(
      """MATCH (a) WITH a SKIP 0
        |MATCH (a)-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN r""".stripMargin) should equal(
      planner.planBuilder()
        .produceResults("r")
        .filter("r.prop IS NOT NULL")
        .expandAll("(a)-[r:REL]-(b)")
        .skip(0)
        .allNodeScan("a")
        .build()
    )
  }

  test("should not (yet) plan relationship index scan with filter for already bound end node") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .enablePlanningRelationshipIndexes()
      .enableRelationshipTypeScanStore()
      .build()

    planner.plan(
      """MATCH (b) WITH b SKIP 0
        |MATCH (a)-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN r""".stripMargin) should equal(
      planner.planBuilder()
        .produceResults("r")
        .filter("r.prop IS NOT NULL")
        .expandAll("(b)-[r:REL]-(a)")
        .skip(0)
        .allNodeScan("b")
        .build()
    )
  }

  test("should not plan relationship index scan for already bound relationship variable") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .enablePlanningRelationshipIndexes()
      .enableRelationshipTypeScanStore()
      .build()

    withClue("Did not expect an UndirectedRelationshipIndexScan to be planned") {
      planner.plan(
        """MATCH (a)-[r:REL]-(b) WITH r SKIP 0
          |MATCH (a2)-[r:REL]-(b2) WHERE r.prop IS NOT NULL RETURN r""".stripMargin).leaves.treeExists {
        case _: UndirectedRelationshipIndexScan => true
      } should be(false)
    }
  }

  test("scan on inexact predicate if argument ids not provided") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .enablePlanningRelationshipIndexes()
      .enableRelationshipTypeScanStore()
      .build()

    planner.plan("MATCH (a)-[r:REL]-(b) WHERE r.prop = b.prop RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .filter("r.prop = b.prop")
        .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)", indexOrder = IndexOrderNone, argumentIds = Set(), getValue = DoNotGetValue)
        .build()
    )
  }

}
