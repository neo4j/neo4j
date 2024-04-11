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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

class RelationshipIndexScanPlanningIntegrationTest extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport
    with BeLikeMatcher {

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .setRelationshipCardinality("()-[:REL2]-()", 50)
      .addRelationshipIndex("REL", Seq("prop"), 0.01, 0.001, indexType = IndexType.RANGE)
      .addRelationshipIndex("REL", Seq("prop"), 0.01, 0.001, indexType = IndexType.TEXT)

  test("should plan undirected relationship index scan with IS NOT NULL") {
    val planner = plannerBuilder().build()
    planner.plan("MATCH (a)-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )
  }

  test("should plan directed OUTGOING relationship index scan with IS NOT NULL") {
    val planner = plannerBuilder().build()
    planner.plan("MATCH (a)-[r:REL]->(b) WHERE r.prop IS NOT NULL RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )
  }

  test("should plan directed INCOMING relationship index scan with IS NOT NULL") {
    val planner = plannerBuilder().build()
    planner.plan("MATCH (a)<-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .relationshipIndexOperator("(a)<-[r:REL(prop)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )
  }

  test("should plan undirected relationship index scan with CONTAINS") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, indexType = IndexType.TEXT)
      .build()

    planner.plan("MATCH (a)-[r:REL]-(b) WHERE r.prop CONTAINS 'test' RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .relationshipIndexOperator("(a)-[r:REL(prop CONTAINS 'test')]-(b)", indexType = IndexType.TEXT)
        .build()
    )
  }

  test("should plan directed OUTGOING relationship index scan with CONTAINS") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, indexType = IndexType.TEXT)
      .build()

    planner.plan("MATCH (a)-[r:REL]->(b) WHERE r.prop CONTAINS 'test' RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .relationshipIndexOperator("(a)-[r:REL(prop CONTAINS 'test')]->(b)", indexType = IndexType.TEXT)
        .build()
    )
  }

  test("should plan directed INCOMING relationship index scan with CONTAINS") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, indexType = IndexType.TEXT)
      .build()

    planner.plan("MATCH (a)<-[r:REL]-(b) WHERE r.prop CONTAINS 'test' RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .relationshipIndexOperator("(a)<-[r:REL(prop CONTAINS 'test')]-(b)", indexType = IndexType.TEXT)
        .build()
    )
  }

  test("should plan undirected relationship index scan with ENDS WITH") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, indexType = IndexType.TEXT)
      .build()

    planner.plan("MATCH (a)-[r:REL]-(b) WHERE r.prop ENDS WITH 'test' RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .relationshipIndexOperator("(a)-[r:REL(prop ENDS WITH 'test')]-(b)", indexType = IndexType.TEXT)
        .build()
    )
  }

  test("should plan directed OUTGOING relationship index scan with ENDS WITH") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, indexType = IndexType.TEXT)
      .build()

    planner.plan("MATCH (a)-[r:REL]->(b) WHERE r.prop ENDS WITH 'test' RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .relationshipIndexOperator("(a)-[r:REL(prop ENDS WITH 'test')]->(b)", indexType = IndexType.TEXT)
        .build()
    )
  }

  test("should plan directed INCOMING relationship index scan with ENDS WITH") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, indexType = IndexType.TEXT)
      .build()

    planner.plan("MATCH (a)<-[r:REL]-(b) WHERE r.prop ENDS WITH 'test' RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .relationshipIndexOperator("(a)<-[r:REL(prop ENDS WITH 'test')]-(b)", indexType = IndexType.TEXT)
        .build()
    )
  }

  test(
    "should plan undirected relationship index scan with IS NOT NULL on the RHS of an Apply with correct arguments"
  ) {
    val planner = plannerBuilder()
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, indexType = IndexType.RANGE)
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
        .apply()
        .|.relationshipIndexOperator(
          "(a)-[r:REL(prop)]-(b)",
          _ => GetValue,
          argumentIds = Set("n"),
          indexType = IndexType.RANGE
        )
        .cacheProperties("cacheNFromStore[n.prop]")
        .allNodeScan("n")
        .build()
    )
  }

  test("should plan undirected relationship index scan with CONTAINS on the RHS of an Apply with correct arguments") {
    val planner = plannerBuilder().build()

    planner.plan(
      """MATCH (n)
        |CALL {
        |  WITH n
        |  MATCH (a)-[r:REL]-(b)
        |  WHERE r.prop CONTAINS 'test' AND b.prop = n.prop
        |  RETURN r
        |}
        |RETURN n, r
        |""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("n", "r")
        .filter("b.prop = n.prop")
        .apply()
        .|.relationshipIndexOperator(
          "(a)-[r:REL(prop CONTAINS 'test')]-(b)",
          argumentIds = Set("n"),
          indexType = IndexType.TEXT
        )
        .allNodeScan("n")
        .build()
    )
  }

  test("should plan undirected relationship index scan with ENDS WITH on the RHS of an Apply with correct arguments") {
    val planner = plannerBuilder()
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.001, indexType = IndexType.TEXT)
      .build()

    planner.plan(
      """MATCH (n)
        |CALL {
        |  WITH n
        |  MATCH (a)-[r:REL]-(b)
        |  WHERE r.prop ENDS WITH 'test' AND b.prop = n.prop
        |  RETURN r
        |}
        |RETURN n, r
        |""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("n", "r")
        .filter("b.prop = n.prop")
        .apply()
        .|.relationshipIndexOperator(
          "(a)-[r:REL(prop ENDS WITH 'test')]-(b)",
          argumentIds = Set("n"),
          indexType = IndexType.TEXT
        )
        .allNodeScan("n")
        .build()
    )
  }

  test("should plan undirected relationship index scan over NodeByLabelScan using a hint") {
    val planner = plannerBuilder()
      .setLabelCardinality("A", 10)
      .setRelationshipCardinality("(:A)-[:REL]-()", 10)
      .build()

    planner.plan("MATCH (a:A)-[r:REL]-(b) USING INDEX r:REL(prop) WHERE r.prop IS NOT NULL RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .filter("a:A")
        .relationshipIndexOperator("(a)-[r:REL(prop)]-(b)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )
  }

  test("should plan undirected relationship index scan with filter for already bound start node") {
    val planner = plannerBuilder().build()

    planner.plan(
      """
        |MATCH (a) WITH a SKIP 0
        |MATCH (a)-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN r""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("r")
        // the filter got pushed up through the apply
        .filter("a = anon_0")
        .apply()
        .|.relationshipIndexOperator(
          "(anon_0)-[r:REL(prop)]-(b)",
          _ => GetValue,
          argumentIds = Set("a"),
          indexType = IndexType.RANGE
        )
        .skip(0)
        .allNodeScan("a")
        .build()
    )
  }

  test("should plan relationship index scan with filter for already bound end node") {
    val planner = plannerBuilder().build()
    planner.plan(
      """MATCH (b) WITH b SKIP 0
        |MATCH (a)-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN r""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("r")
        // the filter got pushed up through the apply
        .filter("b = anon_0")
        .apply()
        .|.relationshipIndexOperator(
          "(a)-[r:REL(prop)]-(anon_0)",
          _ => GetValue,
          argumentIds = Set("b"),
          indexType = IndexType.RANGE
        )
        .skip(0)
        .allNodeScan("b")
        .build()
    )
  }

  test("should plan relationship index scan with filter for already bound nodes") {
    val planner = plannerBuilder().build()
    planner.plan(
      """MATCH (a), (b) WITH a, b SKIP 0
        |MATCH (a)-[r:REL]-(b) WHERE r.prop IS NOT NULL RETURN r""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("r")
        // the filter got pushed up through the apply
        .filter("a = anon_0", "b = anon_1")
        .apply()
        .|.relationshipIndexOperator(
          "(anon_0)-[r:REL(prop)]-(anon_1)",
          _ => GetValue,
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE
        )
        .skip(0)
        .cartesianProduct()
        .|.allNodeScan("b")
        .allNodeScan("a")
        .build()
    )
  }

  test("should not plan relationship index scan for already bound relationship variable") {
    val planner = plannerBuilder().build()
    withClue("Did not expect an UndirectedRelationshipIndexScan to be planned") {
      planner.plan(
        """MATCH (a)-[r:REL]-(b) WITH r SKIP 0
          |MATCH (a2)-[r:REL]-(b2) WHERE r.prop IS NOT NULL RETURN r""".stripMargin
      ).leaves.folder.treeExists {
        case _: UndirectedRelationshipIndexScan => true
      } should be(false)
    }
  }

  test("should plan relationship index string scan with filter for already bound start node") {
    for (op <- Seq("CONTAINS", "ENDS WITH")) {
      val planner = plannerBuilder().build()

      val query =
        s"""MATCH (a) WITH a SKIP 0
           |MATCH (a)-[r:REL]-(b) WHERE r.prop $op 'foo' RETURN r""".stripMargin

      planner.plan(query) should equal(
        planner.planBuilder()
          .produceResults("r")
          .filter("a = anon_0")
          .apply()
          .|.relationshipIndexOperator(
            s"(anon_0)-[r:REL(prop $op 'foo')]-(b)",
            argumentIds = Set("a"),
            indexType = IndexType.TEXT
          )
          .skip(0)
          .allNodeScan("a")
          .build()
      )
    }
  }

  test("should plan relationship index string scan with filter for already bound end node") {
    for (op <- Seq("CONTAINS", "ENDS WITH")) {
      val planner = plannerBuilder().build()

      val query =
        s"""MATCH (b) WITH b SKIP 0
           |MATCH (a)-[r:REL]-(b) WHERE r.prop $op 'foo' RETURN r""".stripMargin

      planner.plan(query) should equal(
        planner.planBuilder()
          .produceResults("r")
          .filter("b = anon_0")
          .apply()
          .|.relationshipIndexOperator(
            s"(a)-[r:REL(prop $op 'foo')]-(anon_0)",
            argumentIds = Set("b"),
            indexType = IndexType.TEXT
          )
          .skip(0)
          .allNodeScan("b")
          .build()
      )
    }
  }

  test("should plan relationship index string scan with filter for already bound nodes") {
    for (op <- Seq("CONTAINS", "ENDS WITH")) {
      val planner = plannerBuilder().build()
      planner.plan(
        s"""MATCH (a), (b) WITH a, b SKIP 0
           |MATCH (a)-[r:REL]-(b) WHERE r.prop $op 'foo' RETURN r""".stripMargin
      ) should equal(
        planner.planBuilder()
          .produceResults("r")
          // the filter got pushed up through the apply
          .filter("a = anon_0", "b = anon_1")
          .apply()
          .|.relationshipIndexOperator(
            s"(anon_0)-[r:REL(prop $op 'foo')]-(anon_1)",
            argumentIds = Set("a", "b"),
            indexType = IndexType.TEXT
          )
          .skip(0)
          .cartesianProduct()
          .|.allNodeScan("b")
          .allNodeScan("a")
          .build()
      )
    }
  }

  test("should not plan relationship index string scan for already bound relationship variable") {
    for (op <- Seq("CONTAINS", "ENDS WITH")) {
      val planner = plannerBuilder().build()

      val query =
        s"""MATCH (a)-[r:REL]-(b) WITH r SKIP 0
           |MATCH (a2)-[r:REL]-(b2) WHERE r.prop $op 'foo' RETURN r""".stripMargin

      withClue("Used relationship index when not expected:") {
        planner.plan(query).leaves.folder.treeExists {
          case _: UndirectedRelationshipIndexContainsScan => true
          case _: UndirectedRelationshipIndexEndsWithScan => true
        } should be(false)
      }
    }
  }

  test("scan on inexact predicate if argument ids not provided") {
    val planner = plannerBuilder().build()
    planner.plan("MATCH (a)-[r:REL]-(b) WHERE r.prop = b.prop RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .filter("cacheR[r.prop] = b.prop")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop)]-(b)",
          indexOrder = IndexOrderNone,
          argumentIds = Set(),
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build()
    )
  }

  test("should not prefer relationship index scan over label scan if label scan is selective") {
    val planner = plannerBuilder()
      .setLabelCardinality("A", 10)
      .setRelationshipCardinality("(:A)-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .build()
    planner.plan("MATCH (a:A)-[r:REL]-() WHERE r.prop IS NOT NULL RETURN r").leaves should beLike {
      case Seq(_: NodeByLabelScan) => ()
    }
  }

  test("should prefer relationship index scan over label scan if label scan is not selective") {
    val planner = plannerBuilder()
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("(:A)-[:REL]-()", 10)
      .build()
    planner.plan("MATCH (a:A)-[r:REL]-() WHERE r.prop IS NOT NULL RETURN r").leaves should beLike {
      case Seq(_: UndirectedRelationshipIndexScan) => ()
    }
  }

  test("with two possible relationship indexes, should plan index scan and expand if expands decrease cardinality") {
    val planner = super.plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .setLabelCardinality("B", 700)
      .setLabelCardinality("C", 400)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 50)
      .setRelationshipCardinality("(:A)-[:REL]-()", 50)
      .setRelationshipCardinality("()-[:REL]-(:B)", 50)
      .setRelationshipCardinality("(:A)-[:REL]-(:B)", 50)
      .setRelationshipCardinality("()-[:REL2]-()", 50)
      .setRelationshipCardinality("(:B)-[:REL2]-()", 50)
      .setRelationshipCardinality("()-[:REL2]-(:C)", 50)
      .setRelationshipCardinality("(:B)-[:REL2]-(:C)", 50)
      .addRelationshipIndex("REL", Seq("prop"), 0.25, 0.01, indexType = IndexType.RANGE)
      .addRelationshipIndex("REL2", Seq("prop"), 0.5, 0.01, indexType = IndexType.RANGE)
      .build()
    planner.plan(
      """MATCH (a:A)-[r:REL]->(b:B)<-[r2:REL2]-(c:C)
        |WHERE r.prop IS NOT NULL
        |  AND r2.prop IS NOT NULL
        |RETURN r, r2""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("r", "r2")
        .filterExpression(hasLabels("c", "C"), isNotNull(prop("r2", "prop")))
        .expandAll("(b)<-[r2:REL2]-(c)")
        .filterExpression(andsReorderableAst(hasLabels("b", "B"), hasLabels("a", "A")))
        .relationshipIndexOperator(
          "(a)-[r:REL(prop)]->(b)",
          indexOrder = IndexOrderNone,
          argumentIds = Set(),
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build()
    )
  }

  test(
    "with two possible relationship indexes, should plan two index scans and node hash join if expands increase cardinality and indexes are very selective"
  ) {
    val planner = super.plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("B", 70)
      .setLabelCardinality("C", 40)
      .setAllRelationshipsCardinality(10000)
      .setRelationshipCardinality("()-[:REL]-()", 5000)
      .setRelationshipCardinality("(:A)-[:REL]-()", 5000)
      .setRelationshipCardinality("()-[:REL]-(:B)", 5000)
      .setRelationshipCardinality("(:A)-[:REL]-(:B)", 5000)
      .setRelationshipCardinality("()-[:REL2]-()", 5000)
      .setRelationshipCardinality("(:B)-[:REL2]-()", 5000)
      .setRelationshipCardinality("()-[:REL2]-(:C)", 5000)
      .setRelationshipCardinality("(:B)-[:REL2]-(:C)", 5000)
      .addRelationshipIndex("REL", Seq("prop"), 0.02, 0.01, indexType = IndexType.RANGE)
      .addRelationshipIndex("REL2", Seq("prop"), 0.1, 0.01, indexType = IndexType.RANGE)
      .build()
    planner.plan(
      """MATCH (a:A)-[r:REL]->(b:B)<-[r2:REL2]-(c:C)
        |WHERE r.prop IS NOT NULL
        |  AND r2.prop IS NOT NULL
        |RETURN r, r2""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("r", "r2")
        .nodeHashJoin("b")
        .|.filterExpression(hasLabels("c", "C"))
        .|.relationshipIndexOperator(
          "(c)-[r2:REL2(prop)]->(b)",
          indexOrder = IndexOrderNone,
          argumentIds = Set(),
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .filterExpression(andsReorderableAst(hasLabels("b", "B"), hasLabels("a", "A")))
        .relationshipIndexOperator(
          "(a)-[r:REL(prop)]->(b)",
          indexOrder = IndexOrderNone,
          argumentIds = Set(),
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .build()
    )
  }

  test("should plan relationship index scan for self-loops") {
    val planner = plannerBuilder().build()

    planner.plan(s"MATCH (a)-[r:REL]-(a) WHERE r.prop IS NOT NULL RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .filter("a = anon_0")
        .relationshipIndexOperator("(a)-[r:REL(prop)]-(anon_0)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    )
  }
}
