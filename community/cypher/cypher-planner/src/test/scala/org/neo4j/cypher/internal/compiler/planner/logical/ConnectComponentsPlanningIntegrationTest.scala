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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.ExecutionModel.Batched
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins.COMPONENT_THRESHOLD_FOR_CARTESIAN_PRODUCT
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.BOTH
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb
import org.neo4j.graphdb.schema.IndexType

import java.lang

import scala.util.Random

class ConnectComponentsPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .enableConnectComponentsPlanner()

  test(
    "should build cartesian product with sorted plan left for many disconnected components with cheap sorted component"
  ) {
    val nodes = (0 until COMPONENT_THRESHOLD_FOR_CARTESIAN_PRODUCT).map(i => s"(n$i:Few)").mkString(",")
    val orderedNode = s"n${COMPONENT_THRESHOLD_FOR_CARTESIAN_PRODUCT}"

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Few", 2)
      .setLabelCardinality("Many", 50)
      .addNodeIndex("Many", Seq("prop"), 0.5, 0.01, providesOrder = BOTH)
      .build()
      .plan(
        s"""MATCH $nodes, ($orderedNode:Many)
           |WHERE $orderedNode.prop IS NOT NULL
           |RETURN * ORDER BY $orderedNode.prop
           |""".stripMargin
      )

    // We do not want a Sort
    plan.stripProduceResults shouldBe a[CartesianProduct]
    // Sorted index should be placed on the left of the cartesian products
    plan.leftmostLeaf should beLike {
      case NodeIndexScan(LogicalVariable(name), _, _, _, _, _, _) if name == orderedNode => ()
    }
  }

  test(
    "should build cartesian product with Sort afterwards for many disconnected components with costly sorted component"
  ) {
    val nodes = (0 until COMPONENT_THRESHOLD_FOR_CARTESIAN_PRODUCT).map(i => s"(n$i:Few)").mkString(",")
    val orderedNode = s"n${COMPONENT_THRESHOLD_FOR_CARTESIAN_PRODUCT}"

    val plan = plannerBuilder()
      .setAllNodesCardinality(Batched.default.bigBatchSize * 2)
      .setLabelCardinality("Few", 1)
      .setLabelCardinality("Many", Batched.default.bigBatchSize * 2)
      .addNodeIndex("Many", Seq("prop"), 0.5, 0.01, providesOrder = BOTH)
      .setExecutionModel(Batched.default) // In Volcano, the cartesian product does not get more expensive by having to provide order, so this test does not make sense there.
      .build()
      .plan(
        s"""MATCH $nodes, ($orderedNode:Many)
           |WHERE $orderedNode.prop IS NOT NULL
           |RETURN * ORDER BY $orderedNode.prop
           |""".stripMargin
      )

    // We want a Sort
    plan.stripProduceResults shouldBe a[Sort]
    // Sorted index should not be placed on the left of the cartesian products
    plan.leftmostLeaf shouldNot beLike {
      case NodeIndexScan(LogicalVariable(node), _, _, _, _, _, _) if node == orderedNode => ()
    }
  }

  test("should build right deep tree of lots of disconnected components if no joins are possible ") {
    val c = 10
    val labelsAndNumbers = (1 to c).map(i => (s"Label$i", i))
    val varsAndLabels = labelsAndNumbers.map { case (label, num) => (s"n$num", label) }
    val patterns = Random.shuffle(varsAndLabels.map { case (v, l) => s"($v:$l)" }).mkString(",")

    val builder = plannerBuilder()
      .setAllNodesCardinality(20000)
      .setLabelCardinalities(labelsAndNumbers.toMap.view.mapValues(_.toDouble * 2000).toMap)

    val volcano = builder.setExecutionModel(Volcano).build()
    val batched = builder.setExecutionModel(Batched.default).build()
    val query = s"MATCH $patterns RETURN n1"
    val volcanoPlan = volcano.plan(query)
    val batchedPlan = batched.plan(query)

    val rightDeepPlan = builder.build().planBuilder()
      .produceResults("n1")
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.cartesianProduct()
      .|.|.|.cartesianProduct()
      .|.|.|.|.cartesianProduct()
      .|.|.|.|.|.cartesianProduct()
      .|.|.|.|.|.|.cartesianProduct()
      .|.|.|.|.|.|.|.cartesianProduct()
      .|.|.|.|.|.|.|.|.cartesianProduct()
      .|.|.|.|.|.|.|.|.|.nodeByLabelScan("n10", "Label10")
      .|.|.|.|.|.|.|.|.nodeByLabelScan("n9", "Label9")
      .|.|.|.|.|.|.|.nodeByLabelScan("n8", "Label8")
      .|.|.|.|.|.|.nodeByLabelScan("n7", "Label7")
      .|.|.|.|.|.nodeByLabelScan("n6", "Label6")
      .|.|.|.|.nodeByLabelScan("n5", "Label5")
      .|.|.|.nodeByLabelScan("n4", "Label4")
      .|.|.nodeByLabelScan("n3", "Label3")
      .|.nodeByLabelScan("n2", "Label2")
      .nodeByLabelScan("n1", "Label1")
      .build()

    // For Volcano the shape of the CP tree is not important.
    // They all have the same cost, as long as the label scans appear in the right order from left to right in the tree.
    volcanoPlan.folder.findAllByClass[NodeByLabelScan].map(_.idName) shouldEqual varsAndLabels.map(x => varFor(x._1))
    // For Batched, a right deep tree is always equally good or better than other tree shapes.
    batchedPlan shouldEqual rightDeepPlan
  }

  test("should plan cartesian product for disconnected components") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .build()

    val plan = cfg.plan("MATCH (n), (m) RETURN n, m")

    plan shouldEqual cfg.planBuilder()
      .produceResults("n", "m")
      .cartesianProduct()
      .|.allNodeScan("m")
      .allNodeScan("n")
      .build()
  }

  test("should plan cartesian product of three plans so the cost is minimized") {
    val builder = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 300)
      .setLabelCardinality("B", 200)
      .setLabelCardinality("C", 100)

    val volcano = builder.setExecutionModel(Volcano).build()
    val batched = builder.setExecutionModel(Batched.default).build()
    val query =
      """MATCH (a:A), (b:B), (c:C)
        |RETURN a, b, c
        |""".stripMargin
    val volcanoPlan = volcano.plan(query)
    val batchedPlan = batched.plan(query)

    // Volcano:
    // A x B = 300 + 300 * 200 = 60300
    // A x C = 300 + 300 * 100 = 30300
    // B x A = 200 + 200 * 300 = 60200
    // B x C = 200 + 200 * 100 = 20200
    // C x A = 100 + 100 * 300 = 30100
    // C x B = 100 + 100 * 200 = 20100

    // A x (B x C) = 300 + 300 * 20200 = 6060300
    // A x (C X B) = 300 + 300 * 20100 = 6030300
    // B x (A x C) = 200 + 200 * 30300 = 6060200
    // B x (C x A) = 200 + 200 * 30100 = 6020200
    // C x (A x B) = 100 + 100 * 60300 = 6030100
    // C x (B x A) = 100 + 100 * 60200 = 6020100 // winner
    // (A x B) x C = 60300 + 60000 * 100 = 6060300
    // (A x C) x B = 30300 + 30000 * 200 = 6030300
    // (B x A) x C = 60200 + 60000 * 100 = 6060200
    // (B x C) x A = 20200 + 20000 * 300 = 6020200
    // (C x A) x B = 30100 + 30000 * 200 = 6030100
    // (C x B) x A = 20100 + 20000 * 300 = 6020100 // winner
    volcanoPlan should (equal(volcano.planBuilder()
      .produceResults("a", "b", "c")
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.nodeByLabelScan("a", "A")
      .|.nodeByLabelScan("b", "B")
      .nodeByLabelScan("c", "C")
      .build()) or equal(volcano.planBuilder()
      .produceResults("a", "b", "c")
      .cartesianProduct()
      .|.nodeByLabelScan("a", "A")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B")
      .nodeByLabelScan("c", "C")
      .build()))

    // Batched:
    // A x B = 300 + 1 * 200 = 500
    // A x C = 300 + 1 * 100 = 400
    // B x A = 200 + 1 * 300 = 500
    // B x C = 200 + 1 * 100 = 300
    // C x A = 100 + 1 * 300 = 400
    // C x B = 100 + 1 * 200 = 300

    // A x (B x C) = 300 + 1 * 300 = 600 // all plans that look like _ x (_ x _) have the same cost
    // A x (C x B) = 300 + 1 * 300 = 600
    // ...
    // C x (B x A) = 100 + 1 * 500 = 600
    // (A x B) x C = 500 + 59 * 100 = 59500 // all of these are more expensive
    // ...
    // (C x B) x A = 300 + 20 * 300 = 6200
    batchedPlan.stripProduceResults should beLike {
      case CartesianProduct(_: NodeByLabelScan, CartesianProduct(_: NodeByLabelScan, _: NodeByLabelScan)) => ()
    }
  }

  test("should plan cartesian product of two plans so the cost is minimized") {
    val builder = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 30)
      .setLabelCardinality("B", 20)

    val volcano = builder.setExecutionModel(Volcano).build()
    val batched = builder.setExecutionModel(Batched.default).build()

    val query =
      """MATCH (a:A), (b:B)
        |RETURN a, b
        |""".stripMargin

    val volcanoPlan = volcano.plan(query)
    val batchedPlan = batched.plan(query)

    // Volcano:
    // A x B = 30 + 30 * 20 = 630
    // B x A = 20 + 20 * 30 = 620 // winner

    volcanoPlan should equal(volcano.planBuilder()
      .produceResults("a", "b")
      .cartesianProduct()
      .|.nodeByLabelScan("a", "A")
      .nodeByLabelScan("b", "B")
      .build())

    // Batched:
    // A x B = 30 + 1 * 20 => 50
    // B x A = 20 + 1 * 20 => 50
    batchedPlan.stripProduceResults should beLike {
      case CartesianProduct(_: NodeByLabelScan, _: NodeByLabelScan) => ()
    }
  }

  test("should plan nested index join of two components connected with predicates for two different nodes") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 30)
      .setLabelCardinality("B", 20)
      .setLabelCardinality("C", 20)
      .setLabelCardinality("D", 20)
      .setRelationshipCardinality("(:A)-[]-(:B)", 30)
      .setRelationshipCardinality("(:A)-[]-()", 30)
      .setRelationshipCardinality("()-[]-(:B)", 30)
      .setRelationshipCardinality("(:C)-[]-(:D)", 20)
      .setRelationshipCardinality("(:C)-[]-()", 20)
      .setRelationshipCardinality("()-[]-(:D)", 20)
      .setRelationshipCardinality("()-[]-()", 200)
      .addNodeIndex("C", Seq("prop"), 1.0, 0.05)
      .addNodeIndex("D", Seq("prop"), 1.0, 0.05)
      .build()

    val plan = cfg.plan(
      "MATCH (a:A)--(b:B), (c:C)--(d:D) WHERE a.prop + b.prop > c.prop AND a.prop + b.prop > d.prop RETURN a, b, c, d"
    )

    val nodeIndexSeeks = plan.folder.treeCount {
      case _: NodeIndexSeek => ()
    }

    withClue(plan) {
      nodeIndexSeeks should be > 0
    }
  }

  test("should plan nested index join with CONTAINS scan") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 30)
      .setLabelCardinality("C", 20)
      .addNodeIndex("C", Seq("prop"), 1.0, 0.05, indexType = graphdb.schema.IndexType.TEXT)
      .build()

    val plan = cfg.plan(
      "MATCH (a:A), (c:C) WHERE c.prop CONTAINS a.prop RETURN *"
    ).stripProduceResults

    plan should equal(
      cfg.subPlanBuilder()
        .apply()
        .|.nodeIndexOperator(
          "c:C(prop CONTAINS ???)",
          paramExpr = Some(prop("a", "prop")),
          argumentIds = Set("a"),
          indexType = graphdb.schema.IndexType.TEXT
        )
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test("should plan nested index join with ENDS WITH scan") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 30)
      .setLabelCardinality("C", 20)
      .addNodeIndex("C", Seq("prop"), 1.0, 0.05, indexType = graphdb.schema.IndexType.TEXT)
      .build()

    val plan = cfg.plan(
      "MATCH (a:A), (c:C) WHERE c.prop ENDS WITH a.prop RETURN *"
    ).stripProduceResults

    plan should equal(
      cfg.subPlanBuilder()
        .apply()
        .|.nodeIndexOperator(
          "c:C(prop ENDS WITH ???)",
          paramExpr = Some(prop("a", "prop")),
          argumentIds = Set("a"),
          indexType = graphdb.schema.IndexType.TEXT
        )
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test(
    "should plan nested relationship index join of two components with index on relationship appearing first in predicate"
  ) {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .setRelationshipCardinality("()-[:REL2]-()", 50)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)
      .build()

    val plan = cfg.plan(
      """MATCH (a)-[r1:REL]->(b), (c)-[r2:REL2]->(d)
        |WHERE r1.prop > r2.prop
        |RETURN a, b, c, d""".stripMargin
    )

    val relIndexSeeks = plan.folder.treeCount {
      case _: DirectedRelationshipIndexSeek => ()
    }

    withClue(plan) {
      relIndexSeeks should be > 0
    }
  }

  test("should plan nested relationship index join with CONTAINS scan (directed)") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .setRelationshipCardinality("()-[:REL2]-()", 50)
      .addRelationshipIndex("REL2", Seq("prop"), 1.0, 0.01, indexType = graphdb.schema.IndexType.TEXT)
      .build()

    val plan = cfg.plan(
      "MATCH (a)-[r1:REL]->(b), (c)-[r2:REL2]->(d) WHERE r2.prop CONTAINS r1.prop RETURN *"
    ).stripProduceResults

    plan should equal(
      cfg.subPlanBuilder()
        .apply()
        .|.relationshipIndexOperator(
          "(c)-[r2:REL2(prop CONTAINS ???)]->(d)",
          paramExpr = Some(prop("r1", "prop")),
          argumentIds = Set("a", "b", "r1"),
          indexType = graphdb.schema.IndexType.TEXT
        )
        .relationshipTypeScan("(a)-[r1:REL]->(b)")
        .build()
    )
  }

  test("should plan nested relationship index join with CONTAINS scan (undirected)") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .setRelationshipCardinality("()-[:REL2]-()", 50)
      .addRelationshipIndex("REL2", Seq("prop"), 1.0, 0.01, indexType = graphdb.schema.IndexType.TEXT)
      .build()

    val plan = cfg.plan(
      "MATCH (a)-[r1:REL]->(b), (c)-[r2:REL2]-(d) WHERE r2.prop CONTAINS r1.prop RETURN *"
    ).stripProduceResults

    plan should equal(
      cfg.subPlanBuilder()
        .apply()
        .|.relationshipIndexOperator(
          "(c)-[r2:REL2(prop CONTAINS ???)]-(d)",
          paramExpr = Some(prop("r1", "prop")),
          argumentIds = Set("a", "b", "r1"),
          indexType = graphdb.schema.IndexType.TEXT
        )
        .relationshipTypeScan("(a)-[r1:REL]->(b)")
        .build()
    )
  }

  test("should plan nested relationship index join with ENDS WITH scan (directed)") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .setRelationshipCardinality("()-[:REL2]-()", 50)
      .addRelationshipIndex("REL2", Seq("prop"), 1.0, 0.01, indexType = graphdb.schema.IndexType.TEXT)
      .build()

    val plan = cfg.plan(
      "MATCH (a)-[r1:REL]->(b), (c)-[r2:REL2]->(d) WHERE r2.prop ENDS WITH r1.prop RETURN *"
    ).stripProduceResults

    plan should equal(
      cfg.subPlanBuilder()
        .apply()
        .|.relationshipIndexOperator(
          "(c)-[r2:REL2(prop ENDS WITH ???)]->(d)",
          paramExpr = Some(prop("r1", "prop")),
          argumentIds = Set("a", "b", "r1"),
          indexType = graphdb.schema.IndexType.TEXT
        )
        .relationshipTypeScan("(a)-[r1:REL]->(b)")
        .build()
    )
  }

  test("should plan nested relationship index join with ENDS WITH scan (undirected)") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .setRelationshipCardinality("()-[:REL2]-()", 50)
      .addRelationshipIndex("REL2", Seq("prop"), 1.0, 0.01, indexType = graphdb.schema.IndexType.TEXT)
      .build()

    val plan = cfg.plan(
      "MATCH (a)-[r1:REL]->(b), (c)-[r2:REL2]-(d) WHERE r2.prop ENDS WITH r1.prop RETURN *"
    ).stripProduceResults

    plan should equal(
      cfg.subPlanBuilder()
        .apply()
        .|.relationshipIndexOperator(
          "(c)-[r2:REL2(prop ENDS WITH ???)]-(d)",
          paramExpr = Some(prop("r1", "prop")),
          argumentIds = Set("a", "b", "r1"),
          indexType = graphdb.schema.IndexType.TEXT
        )
        .relationshipTypeScan("(a)-[r1:REL]->(b)")
        .build()
    )
  }

  test(
    "should plan cartesian product of two plans so the cost is minimized, even if cardinality is way lower on one side."
  ) {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .build()

    val plan = cfg.plan(
      """MATCH (a:A), (b:A)
        |WHERE a.prop = 0
        |RETURN a
        |""".stripMargin
    )

    // A x B = 11 +  1 * 10 => 21
    // B x A = 10 + 10 * 11 => 121

    plan shouldEqual (cfg.planBuilder()
      .produceResults("a")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "A")
      .filter("a.prop = 0")
      .nodeByLabelScan("a", "A")
      .build())
  }

  test("should not plan apply with independent rhs") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Awesome", 10)
      .addNodeIndex("Awesome", Seq("prop1"), 0.5, 0.01)
      .addNodeIndex("Awesome", Seq("prop2"), 0.5, 0.01)
      .build()

    val plan = cfg.plan(
      """MATCH (n:Awesome), (m:Awesome)
        |WHERE n.prop1 < 42 AND m.prop2 < 42
        |  AND n.prop1 = m.prop2
        |RETURN n
        |""".stripMargin
    )

    plan.stripProduceResults shouldNot beLike {
      case Selection(_, Apply(_, NodeIndexSeek(_, _, _, _, args, _, _, _))) if args.isEmpty => ()
    }
  }

  test("should plan nested index join or value hash join where rhs depends on lhs") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Awesome", 10)
      .addNodeIndex("Awesome", Seq("prop1"), 0.5, 0.01)
      .addNodeIndex("Awesome", Seq("prop2"), 0.5, 0.01)
      .addNodeIndex("Awesome", Seq("prop3"), 0.5, 0.01)
      .build()

    val plan = cfg.plan(
      """MATCH (n:Awesome), (m:Awesome)
        |WHERE n.prop1 < 42 AND m.prop2 < 42
        |  AND n.prop3 = m.prop4
        |RETURN n
        |""".stripMargin
    )

    val beSolvedByApply = beLike {
      case Selection(
          _,
          Apply(
            NodeIndexSeek(_, _, _, RangeQueryExpression(_), _, _, _, _),
            NodeIndexSeek(_, _, _, SingleQueryExpression(_), _, _, _, _)
          )
        ) => ()
    }
    val beSolvedByJoin = beLike {
      case ValueHashJoin(
          NodeIndexSeek(_, _, _, RangeQueryExpression(_), _, _, _, _),
          NodeIndexSeek(_, _, _, RangeQueryExpression(_), _, _, _, _),
          _
        ) => ()
    }

    plan.stripProduceResults should (beSolvedByApply or beSolvedByJoin)
  }

  test("should plan value hash join where rhs depends on lhs and there are no indexes") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .build()

    val plan = cfg.plan(
      """MATCH (n), (m)
        |WHERE n.prop1 = m.prop2
        |RETURN n""".stripMargin
    )

    plan should (equal(cfg.planBuilder()
      .produceResults("n")
      .valueHashJoin("n.prop1 = m.prop2")
      .|.allNodeScan("m")
      .allNodeScan("n")
      .build()) or equal(cfg.planBuilder()
      .produceResults("n")
      .valueHashJoin("m.prop2 = n.prop1")
      .|.allNodeScan("n")
      .allNodeScan("m")
      .build()))
  }

  test("should plan value hash join with the cheapest plan on the left") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 30)
      .setLabelCardinality("M", 20)
      .build()

    val plan = cfg.plan(
      """MATCH (n:N), (m:M)
        |WHERE n.prop1 = m.prop1
        |RETURN n
        |""".stripMargin
    )

    plan shouldEqual cfg.planBuilder()
      .produceResults("n")
      .valueHashJoin("m.prop1 = n.prop1")
      .|.nodeByLabelScan("n", "N")
      .nodeByLabelScan("m", "M")
      .build()
  }

  test("should plan value hash join for one of two predicates") {

    // The implementation should choose the most selective predicate
    // as the hash join predicate, but this situation can't appear in
    // production dbs, since the only way to affect the selectivities would be
    // to add indexes, making us plan NIJ instead

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 100)
      .setLabelCardinality("M", 50)
      .build()

    val plan = cfg.plan(
      """MATCH (n:N), (m:M)
        |WHERE n.prop1 = m.prop1
        |  AND n.prop2 = m.prop2
        |RETURN n
        |""".stripMargin
    )

    val joinOnFirst = cfg.planBuilder()
      .produceResults("n")
      .filter("cache[n.prop2] = cache[m.prop2]")
      .valueHashJoin("m.prop1 = n.prop1")
      .|.cacheProperties("cacheFromStore[n.prop2]")
      .|.nodeByLabelScan("n", "N")
      .cacheProperties("cacheFromStore[m.prop2]")
      .nodeByLabelScan("m", "M")
      .build()

    val joinOnSecond = cfg.planBuilder()
      .produceResults("n")
      .filter("cache[n.prop1] = cache[m.prop1]")
      .valueHashJoin("m.prop2 = n.prop2")
      .|.cacheProperties("cacheFromStore[n.prop1]")
      .|.nodeByLabelScan("n", "N")
      .cacheProperties("cacheFromStore[m.prop1]")
      .nodeByLabelScan("m", "M")
      .build()

    plan should (equal(joinOnFirst) or equal(joinOnSecond))
  }

  test("should plan value hash join for three components and there are no indexes") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 100)
      .setLabelCardinality("M", 50)
      .setLabelCardinality("O", 40)
      .build()

    val plan = cfg.plan(
      """MATCH (n:N), (m:M), (o:O)
        |WHERE n.prop1 = m.prop1
        |  AND m.prop2 = o.prop2
        |RETURN n""".stripMargin
    )

    plan shouldEqual cfg.planBuilder()
      .produceResults("n")
      .valueHashJoin("n.prop1 = m.prop1")
      .|.valueHashJoin("o.prop2 = m.prop2") // connecting m and o is cheaper than connecting n and m
      .|.|.nodeByLabelScan("m", "M")
      .|.nodeByLabelScan("o", "O") // o should be on the left, its cheaper than m
      .nodeByLabelScan("n", "N") // n as the single component should be on the left when joined with 2 components
      .build()
  }

  test("should plan value hash join for the output of two functions being compared") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 20)
      .setLabelCardinality("B", 20)
      .build()

    planner.plan(
      """MATCH (a:A), (b:B)
        |  WHERE toString(a.prop) = toString(b.prop)
        |RETURN a, b""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("a", "b")
        .valueHashJoin("toString(a.prop) = toString(b.prop)")
        .|.nodeByLabelScan("b", "B")
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test("should plan value hash join for the output of a function on RHS being compared") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 20)
      .setLabelCardinality("B", 20)
      .build()

    planner.plan(
      """MATCH (a:A), (b:B)
        |  WHERE a.prop = toString(b.prop)
        |RETURN a, b""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("a", "b")
        .valueHashJoin("a.prop = toString(b.prop)")
        .|.nodeByLabelScan("b", "B")
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test("should plan value hash join for the output of a function on LHS being compared") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 20)
      .setLabelCardinality("B", 20)
      .build()

    planner.plan(
      """MATCH (a:A), (b:B)
        |  WHERE toString(a.prop) = b.prop
        |RETURN a, b""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("a", "b")
        .valueHashJoin("b.prop = toString(a.prop)")
        .|.nodeByLabelScan("a", "A")
        .nodeByLabelScan("b", "B")
        .build()
    )
  }

  test(
    "cheap optional match that requires no components to be connected should be solved before any components are connected"
  ) {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("N", 40)
      .setLabelCardinality("M", 50)
      .setLabelCardinality("O", 90)
      .setRelationshipCardinality("(:N)-[]-()", 10) // Cardinality is not increased by optional match
      .build()

    val plan = cfg.plan(
      """MATCH (n:N), (m:M), (o:O) WHERE n.prop = m.prop
        |OPTIONAL MATCH (n)-[r1]-(x)
        |RETURN n
        |""".stripMargin
    )

    plan.folder.treeFindByClass[OptionalExpand].get.lhs should contain(cfg.subPlanBuilder().nodeByLabelScan(
      "n",
      "N"
    ).build())
  }

  test("expensive optional match is solved after components are connected for Volcano") {
    val builder = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10000)
      .setLabelCardinality("N", 10)
      .setLabelCardinality("M", 50)
      .setRelationshipCardinality("(:N)-[]-()", 10000) // Cardinality is increased a lot by optional match

    val volcano = builder.setExecutionModel(Volcano).build()
    val batched = builder.setExecutionModel(Batched.default).build()

    val query =
      """MATCH (n:N), (m:M)
        |OPTIONAL MATCH (n)-[r1]-(x)
        |RETURN n
        |""".stripMargin

    val volcanoPlan = volcano.plan(query)
    val batchedPlan = batched.plan(query)

    // In Volcano we want to connect the components before doing the expensive optional match
    volcanoPlan shouldEqual volcano.planBuilder()
      .produceResults("n")
      .optionalExpandAll("(n)-[r1]-(x)")
      .cartesianProduct()
      .|.nodeByLabelScan("m", "M")
      .nodeByLabelScan("n", "N")
      .build()

    // In Batched, doing the optional match on the RHS of the Cartesian Product means it only has to be executed
    // (LHS batches * RHS rows) times, vs (LHS rows * RHS rows) times if it was executed after the Cartesian Product.
    batchedPlan shouldEqual batched.planBuilder()
      .produceResults("n")
      .cartesianProduct()
      .|.optionalExpandAll("(n)-[r1]-(x)")
      .|.nodeByLabelScan("n", "N")
      .nodeByLabelScan("m", "M")
      .build()
  }

  test("expensive optional match is solved after cheap optional match") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setAllRelationshipsCardinality(1000)
      .setLabelCardinality("N", 10)
      .setLabelCardinality("X", 1000)
      .setLabelCardinality("Y", 1000)
      .setRelationshipCardinality("(:N)-[]->()", 1000)
      .setRelationshipCardinality("()-[]->(:X)", 1000)
      .setRelationshipCardinality("()-[]->(:Y)", 1000)
      .setRelationshipCardinality("(:N)-[]->(:X)", 1000) // Cardinality is increased a lot by first optional match
      .setRelationshipCardinality("(:N)-[]->(:Y)", 10) // Cardinality is not increased by second optional match
      .build()

    val plan = cfg.plan(
      """MATCH (n:N)
        |OPTIONAL MATCH (n)-[r1]->(x:X)
        |OPTIONAL MATCH (n)-[r2]->(y:Y)
        |RETURN n
        |""".stripMargin
    )

    plan shouldEqual cfg.planBuilder()
      .produceResults("n")
      .optionalExpandAll("(n)-[r1]->(x)", Some("x:X"))
      .optionalExpandAll("(n)-[r2]->(y)", Some("y:Y"))
      .nodeByLabelScan("n", "N")
      .build()
  }

  test("expensive optional match is solved after cheap optional match, when separated by WITH") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setAllRelationshipsCardinality(1000)
      .setLabelCardinality("N", 10)
      .setLabelCardinality("X", 1000)
      .setLabelCardinality("Y", 1000)
      .setRelationshipCardinality("(:N)-[]->()", 1000)
      .setRelationshipCardinality("()-[]->(:X)", 1000)
      .setRelationshipCardinality("()-[]->(:Y)", 1000)
      .setRelationshipCardinality("(:N)-[]->(:X)", 1000) // Cardinality is increased a lot by first optional match
      .setRelationshipCardinality("(:N)-[]->(:Y)", 10) // Cardinality is not increased by second optional match
      .build()

    val plan = cfg.plan(
      """MATCH (n:N)
        |OPTIONAL MATCH (n)-[r1]->(x:X)
        |WITH n, x
        |OPTIONAL MATCH (n)-[r2]->(y:Y)
        |RETURN n
        |""".stripMargin
    )

    plan shouldEqual cfg.planBuilder()
      .produceResults("n")
      .optionalExpandAll("(n)-[r1]->(x)", Some("x:X"))
      .optionalExpandAll("(n)-[r2]->(y)", Some("y:Y"))
      .nodeByLabelScan("n", "N")
      .build()
  }

  test("cheap optional match is solved early even though it appears late in the query") {
    val builder = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setAllRelationshipsCardinality(1000)
      .setLabelCardinality("N", 10)
      .setLabelCardinality("M", 20)
      .setRelationshipCardinality("(:N)-[]->()", 10)
      .setRelationshipCardinality("()-[]->(:M)", 1000)
      .setRelationshipCardinality("(:N)-[]->(:M)", 10)

    val volcano = builder.setExecutionModel(Volcano).build()
    val batched = builder.setExecutionModel(Batched.default).build()

    val query =
      """MATCH (n:N), (m:M)
        |OPTIONAL MATCH (n)-[r1]->(m)
        |OPTIONAL MATCH (n)-[r2]->(y)
        |RETURN n
        |""".stripMargin

    val volcanoPlan = volcano.plan(query)
    val batchedPlan = batched.plan(query)

    volcanoPlan shouldEqual volcano.planBuilder()
      .produceResults("n")
      .optionalExpandInto("(n)-[r1]->(m)")
      .cartesianProduct()
      .|.nodeByLabelScan("m", "M")
      .optionalExpandAll("(n)-[r2]->(y)")
      .nodeByLabelScan("n", "N")
      .build()

    batchedPlan should (
      equal(batched.planBuilder()
        .produceResults("n")
        .optionalExpandInto("(n)-[r1]->(m)")
        .cartesianProduct()
        .|.optionalExpandAll("(n)-[r2]->(y)")
        .|.nodeByLabelScan("n", "N")
        .nodeByLabelScan("m", "M")
        .build())
        or equal(batched.planBuilder()
          .produceResults("n")
          .optionalExpandInto("(n)-[r1]->(m)")
          .cartesianProduct()
          .|.nodeByLabelScan("m", "M")
          .optionalExpandAll("(n)-[r2]->(y)")
          .nodeByLabelScan("n", "N")
          .build())
    )
  }

  test(
    "cheap optional match that requires 2 components to be connected should be solved before other components are connected"
  ) {
    val builder = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("N", 40)
      .setLabelCardinality("M", 50)
      .setLabelCardinality("O", 90)
      .setRelationshipCardinality("(:N)-[]-(:M)", 10)
      .setRelationshipCardinality("(:M)-[]-()", 10)

    val volcano = builder.setExecutionModel(Volcano).build()
    val batched = builder.setExecutionModel(Batched.default).build()

    val query =
      """MATCH (n:N), (m:M), (o:O) WHERE n.prop = m.prop
        |OPTIONAL MATCH (n)-[r1]-(m)-[r2]-(x)
        |RETURN n
        |""".stripMargin

    val volcanoPlan = volcano.plan(query)
    val batchedPlan = batched.plan(query)

    volcanoPlan shouldEqual volcano.planBuilder()
      .produceResults("n")
      .cartesianProduct()
      .|.nodeByLabelScan("o", "O")
      .apply()
      .|.optional("n", "m")
      .|.filter("not r2 = r1")
      .|.expandInto("(n)-[r1]-(m)")
      .|.expandAll("(m)-[r2]-(x)")
      .|.argument("n", "m")
      .valueHashJoin("n.prop = m.prop")
      .|.nodeByLabelScan("m", "M")
      .nodeByLabelScan("n", "N")
      .build()

    // LHS and RHS can freely be switched in Batched, since both 200 and 90 rows are only 1 batch
    batchedPlan should (equal(
      batched.planBuilder()
        .produceResults("n")
        .cartesianProduct()
        .|.apply()
        .|.|.optional("n", "m")
        .|.|.filter("not r2 = r1")
        .|.|.expandInto("(n)-[r1]-(m)")
        .|.|.expandAll("(m)-[r2]-(x)")
        .|.|.argument("n", "m")
        .|.valueHashJoin("n.prop = m.prop")
        .|.|.nodeByLabelScan("m", "M")
        .|.nodeByLabelScan("n", "N")
        .nodeByLabelScan("o", "O")
        .build()
    ) or equal(
      batched.planBuilder()
        .produceResults("n")
        .cartesianProduct()
        .|.nodeByLabelScan("o", "O")
        .apply()
        .|.optional("n", "m")
        .|.filter("not r2 = r1")
        .|.expandInto("(n)-[r1]-(m)")
        .|.expandAll("(m)-[r2]-(x)")
        .|.argument("n", "m")
        .valueHashJoin("n.prop = m.prop")
        .|.nodeByLabelScan("m", "M")
        .nodeByLabelScan("n", "N")
        .build()
    ))
  }

  test(
    "when ordering by a variable introduced by an optional match, choose a plan that keeps the order from the optional match subplan"
  ) {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("N", 40)
      .setLabelCardinality("M", 50)
      .setLabelCardinality("O", 100)
      .setRelationshipCardinality("(:N)-[]-(:M)", 100)
      .setRelationshipCardinality("(:M)-[]-()", 100)
      .build()

    val plan = cfg.plan(
      """MATCH (n:N), (m:M), (o:O)
        |OPTIONAL MATCH (n)-[r1]-(m)-[r2]-(x)
        |RETURN n ORDER BY x.prop
        |""".stripMargin
    )

    plan shouldEqual cfg.planBuilder()
      .produceResults("n")
      .cartesianProduct()
      .|.nodeByLabelScan("o", "O")
      .leftOuterHashJoin("n", "m") // Keeps RHS order
      .|.filter("not r2 = r1")
      .|.expandAll("(m)-[r1]-(n)")
      .|.sort("`x.prop` ASC")
      .|.projection("x.prop AS `x.prop`")
      .|.allRelationshipsScan("(m)-[r2]-(x)")
      .cartesianProduct()
      .|.nodeByLabelScan("m", "M")
      .nodeByLabelScan("n", "N")
      .build()
  }

  test("when ordering by a variable introduced by an optional match, do not sort twice") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("N", 40)
      .setLabelCardinality("M", 50)
      .setLabelCardinality("O", 30)
      .setRelationshipCardinality("(:N)-[]-(:M)", 100)
      .setRelationshipCardinality("(:M)-[]-()", 100)
      .build()

    val plan = cfg.plan(
      """MATCH (n:N), (m:M), (o:O)
        |OPTIONAL MATCH (n)-[r1]-(m)-[r2]-(x)
        |RETURN n ORDER BY x.prop
        |""".stripMargin
    )

    val numSorts = plan.folder.treeCount { case _: Sort => () }
    numSorts shouldEqual 1
  }

  test(
    "when ordering by a variable introduced before the optional match, choose a plan that keeps the order through solving the optional match"
  ) {
    val build = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("N", 40)
      .setLabelCardinality("M", 50)
      .setLabelCardinality("O", 30)
      .setRelationshipCardinality("(:N)-[]-(:M)", 100)
      .setRelationshipCardinality("(:M)-[]-()", 100)

    val volcano = build.setExecutionModel(Volcano).build()
    val batched = build.setExecutionModel(Batched.default).build()

    val query =
      """MATCH (n:N), (m:M), (o:O)
        |OPTIONAL MATCH (n)-[r1]-(m)-[r2]-(x)
        |RETURN n ORDER BY m.prop
        |""".stripMargin

    val volcanoPlan = volcano.plan(query)
    val batchedPlan = batched.plan(query)

    def shouldPlanSortBeforeOptionalMatch(plan: LogicalPlan): Unit = {
      sealed trait State
      final case object Init extends State
      final case object BeforeOptionalMatch extends State
      final case object SortBeforeOptionalMatch extends State
      final case object SortAfterOptionalMatch extends State

      plan.folder.treeFold[State](Init) {
        case _: Sort => {
          case BeforeOptionalMatch => TraverseChildren(SortBeforeOptionalMatch)
          case _                   => SkipChildren(SortAfterOptionalMatch)
        }
        case Apply(_, _: Optional) | _: LeftOuterHashJoin | _: RightOuterHashJoin => {
          case Init => TraverseChildren(BeforeOptionalMatch)
          case x    => TraverseChildren(x)
        }
      } shouldBe SortBeforeOptionalMatch
    }

    shouldPlanSortBeforeOptionalMatch(volcanoPlan)
    shouldPlanSortBeforeOptionalMatch(batchedPlan)
  }

  test("should connect many components and solve many optional matches") {
    val componentVars = (0 to 20).map(i => s"n$i")
    val components = (0 to 20).map(i => s"(n$i)").mkString(", ")
    val optionalMatchVars = (0 to 20).map(i => s"x$i")
    val optionalMatches = (0 to 20).map(i => s"OPTIONAL MATCH (n$i)--(x$i)").mkString("\n")

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .build()

    val plan = cfg.plan(
      s"""MATCH $components
         |$optionalMatches
         |RETURN *
         |""".stripMargin
    )

    val allNodesScanned = plan.folder.treeFold(Seq.empty[String]) {
      case a: AllNodesScan => ids => TraverseChildren(ids :+ a.idName.name)
    }
    val optionalExpanded = plan.folder.treeFold(Seq.empty[String]) {
      case o: OptionalExpand => ids => TraverseChildren(ids :+ o.to.name)
    }

    plan.availableSymbols.map(_.name) should contain allElementsOf (componentVars ++ optionalMatchVars)
    allNodesScanned should contain theSameElementsAs componentVars
    optionalExpanded should contain theSameElementsAs optionalMatchVars
  }

  test("should plan dependent and independent optional matches") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .build()

    val plan = cfg.plan(
      """MATCH (n), (m)
        |OPTIONAL MATCH (n)--(x)
        |OPTIONAL MATCH (y)
        |RETURN *
        |""".stripMargin
    )

    val allNodesScanned = plan.folder.treeFold(Seq.empty[String]) {
      case a: AllNodesScan => ids => TraverseChildren(ids :+ a.idName.name)
    }
    val optionalExpanded = plan.folder.treeFold(Seq.empty[String]) {
      case o: OptionalExpand => ids => TraverseChildren(ids :+ o.to.name)
    }

    plan.availableSymbols.map(_.name) should contain allElementsOf Seq("n", "m", "x", "y")
    allNodesScanned should contain theSameElementsAs Seq("n", "m", "y")
    optionalExpanded should contain theSameElementsAs Seq("x")
  }

  test("shortest path should not fail to get planned after nested index join") {

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 20)
      .setLabelCardinality("M", 20)
      .addNodeIndex("N", Seq("loc"), 1.0, 0.1)
      .build()

    val plan = cfg.plan(
      """MATCH (n:N), (m:M)
        |WHERE n.loc = m.loc
        |WITH n, m
        |MATCH p=shortestPath((n)-[r:R *]-(m))
        |RETURN n, m
        |""".stripMargin
    )

    plan shouldEqual (
      cfg.planBuilder()
        .produceResults("n", "m")
        .shortestPath("(n)-[r:R*1..]-(m)", pathName = Some("p"))
        .apply()
        .|.nodeIndexOperator(
          "n:N(loc = ???)",
          paramExpr = Some(prop("m", "loc")),
          argumentIds = Set("m"),
          indexType = IndexType.RANGE
        )
        .nodeByLabelScan("m", "M")
        .build()
    )
  }

  test("Should switch order of CartesianProduct vs. Apply-Optional in grid query depending on runtime") {
    val builder = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 100)
      .addNodeIndex("Person", Seq("name"), 1.0, 0.01)
      .setRelationshipCardinality("(:Person)-[]->()", 400)
      .setRelationshipCardinality("()-[]->()", 400)
      .withSetting(GraphDatabaseInternalSettings.cypher_idp_solver_duration_threshold, lang.Long.valueOf(2000))

    val query =
      """MATCH (p0:Person { name:'n(0,0)' }),
        |      (p1:Person { name:'n(0,1)' }),
        |      (p2:Person { name:'n(0,2)' })
        |OPTIONAL MATCH (p0)-->(p0_1)-->(p0_2)
        |OPTIONAL MATCH (p1)-->(p1_1)-->(p1_2)
        |OPTIONAL MATCH (p2)-->(p2_1)-->(p2_2)
        |RETURN count(*)
        |""".stripMargin
    val volcanoPlan = builder
      .setExecutionModel(Volcano)
      .build()
      .plan(query)

    val batchedPlan = builder
      .setExecutionModel(Batched.default)
      .build()
      .plan(query)

    // The execution model highly affects the cost of Cartesian Product.
    // However, we cannot assert on the full plans here.
    // Planning this query will lead to compaction in IDP and the exact shape of the plan will be different sometimes.

    volcanoPlan.stripProduceResults should beLike {
      case Aggregation(Apply(_, _: Optional), _, _, _) => ()
    }
    batchedPlan.stripProduceResults should beLike {
      case Aggregation(_: CartesianProduct, _, _, _) => ()
    }
  }

  test("Should switch order of CartesianProduct vs. Apply-Optional in grid query depending on runtime 2") {
    val builder = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 100)
      .addNodeIndex("Person", Seq("name"), 1.0, 0.01, withValues = true, providesOrder = BOTH)
      .setRelationshipCardinality("(:Person)-[]->()", 400)
      .setRelationshipCardinality("()-[]->()", 400)

    val query =
      """MATCH (p0:Person),
        |      (p1:Person)
        |OPTIONAL MATCH (p0)-->(p0_1)-->(p0_2)
        |RETURN * ORDER BY p1.name
        |""".stripMargin
    val volcanoPlan = builder
      .setExecutionModel(Volcano)
      .build()
      .plan(query)

    val batchedPlan = builder
      .setExecutionModel(Batched.default)
      .build()
      .plan(query)

    // Because of the order by, the CartesianProduct will have a provided order, and thus it will not get cheaper with batched execution.
    // Both execution models should find the same plan.
    batchedPlan should equal(volcanoPlan)
  }

  test("should plan predicate without dependencies on the pattern only on one side of cartesian product") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setLabelCardinality("A", 2000)
      .setLabelCardinality("B", 5000)
      .build()

    val q = "MATCH (a:A), (b:B) WHERE $param1 > $param2 RETURN a"

    val plan = planner.plan(q).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B")
      .filter("$param1 > $param2")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("Plans Generic ORDER BY with no Sort on RHS of CartesianProduct - no IDP required") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .build()

    val query =
      """MATCH (a), (b)
        |RETURN *
        |ORDER BY 1
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .sort("1 ASC")
      .projection("1 AS 1")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build())
  }

  test("Plans Generic ORDER BY with no Sort on RHS of CartesianProduct - IDP required") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .build()

    val query =
      """MATCH (a), (b)
        |WHERE a.prop > b.prop
        |RETURN *
        |ORDER BY 1
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .sort("1 ASC")
      .projection("1 AS 1")
      .filter("cacheN[a.prop] > cacheN[b.prop]")
      .cartesianProduct()
      .|.cacheProperties("cacheNFromStore[b.prop]")
      .|.allNodeScan("b")
      .cacheProperties("cacheNFromStore[a.prop]")
      .allNodeScan("a")
      .build())
  }

  test("Plans Generic ORDER BY with no Sort on LHS of Join") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .build()

    val query =
      """MATCH (a), (b)
        |WHERE a.prop = b.prop
        |RETURN *
        |ORDER BY 1
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .sort("1 ASC")
      .projection("1 AS 1")
      .valueHashJoin("a.prop = b.prop")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build())
  }

  test("should plan value hash join with EXISTS expression") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 20)
      .setLabelCardinality("B", 500)
      .setLabelCardinality("C", 200)
      .setRelationshipCardinality("()-[]->()", 300)
      .setRelationshipCardinality("(:B)-[]->()", 300)
      .setRelationshipCardinality("(:B)-[]->(:C)", 300)
      .build()

    val q =
      """MATCH (a:A), (b:B)
        |WHERE a.prop = EXISTS { (b)-[r]->(c:C) }
        |RETURN a, b
        |""".stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .valueHashJoin("a.prop = anon_0")
      .|.letSemiApply("anon_0")
      .|.|.filter("c:C")
      .|.|.expandAll("(b)-[r]->(c)")
      .|.|.argument("b")
      .|.nodeByLabelScan("b", "B")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("should plan value hash join with COUNT expression") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .setLabelCardinality("B", 20)
      .setLabelCardinality("C", 200)
      .setRelationshipCardinality("()-[]->()", 300)
      .setRelationshipCardinality("(:B)-[]->()", 300)
      .setRelationshipCardinality("(:B)-[]->(:C)", 300)
      .build()

    val q =
      """MATCH (a:A), (b:B)
        |WHERE a.prop = COUNT { (b)-[r]->(c:C) }
        |RETURN a, b
        |""".stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .valueHashJoin("anon_0 = a.prop")
      .|.nodeByLabelScan("a", "A")
      .apply()
      .|.aggregation(Seq.empty, Seq("count(*) AS anon_0"))
      .|.filter("c:C")
      .|.expandAll("(b)-[r]->(c)")
      .|.argument("b")
      .nodeByLabelScan("b", "B")
      .build()
  }

  test("should plan value hash join with COUNT expression on both sides") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .setLabelCardinality("B", 20)
      .setLabelCardinality("C", 200)
      .setLabelCardinality("D", 30)
      .setRelationshipCardinality("()-[]->()", 300)
      .setRelationshipCardinality("(:B)-[]->()", 300)
      .setRelationshipCardinality("(:B)-[]->(:D)", 300)
      .setRelationshipCardinality("(:A)-[]->()", 300)
      .setRelationshipCardinality("(:A)-[]->(:C)", 300)
      .build()

    val q =
      """MATCH (a:A), (b:B)
        |WHERE COUNT { (a)-[r1]->(c:C) } = COUNT { (b)-[r2]->(d:D) }
        |RETURN a, b
        |""".stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .valueHashJoin("anon_1 = anon_0")
      .|.apply()
      .|.|.aggregation(Seq(), Seq("count(*) AS anon_0"))
      .|.|.filter("c:C")
      .|.|.expandAll("(a)-[r1]->(c)")
      .|.|.argument("a")
      .|.nodeByLabelScan("a", "A")
      .apply()
      .|.aggregation(Seq(), Seq("count(*) AS anon_1"))
      .|.filter("d:D")
      .|.expandAll("(b)-[r2]->(d)")
      .|.argument("b")
      .nodeByLabelScan("b", "B")
      .build()
  }

  test("Should plan connected component overlapping exclusively on relationship variables") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10)
      .withSetting(GraphDatabaseInternalSettings.cypher_idp_solver_table_threshold, Int.box(16))
      .build()

    val plan = planner.plan(
      """
        |MATCH (a)-[r]->(b)
        |MATCH (c)-[r]->(d)
        |MATCH (e)-[r]->(f)
        |MATCH (g)-[r]->(h)
        |MATCH (i)-[r]->(j)
        |MATCH (k)-[r]->(l)
        |RETURN *
        |""".stripMargin
    )
      .stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .apply()
      .|.projectEndpoints("(g)-[r]->(h)", startInScope = false, endInScope = false)
      .|.projectEndpoints("(i)-[r]->(j)", startInScope = false, endInScope = false)
      .|.projectEndpoints("(k)-[r]->(l)", startInScope = false, endInScope = false)
      .|.argument("e", "d", "r", "b", "f", "a", "c")
      .projectEndpoints("(e)-[r]->(f)", startInScope = false, endInScope = false)
      .projectEndpoints("(a)-[r]->(b)", startInScope = false, endInScope = false)
      .allRelationshipsScan("(c)-[r]->(d)")
      .build()
  }

  test("should plan Argument on RHS of nested index join if it's not the leftmost leaf") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("B", 5000)
      .setLabelCardinality("C", 3000)
      .setRelationshipCardinality("()-[:REL]->()", 1500)
      .setRelationshipCardinality("(:B)-[:REL]->()", 1000)
      .setRelationshipCardinality("(:B)-[:REL]->(:C)", 1000)
      .setRelationshipCardinality("()-[:REL]->(:C)", 1500)
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 1 / 5000.0, isUnique = true)
      .build()

    val q =
      """MATCH (a:A )
        |MATCH (b:B {prop: a.id})
        |WHERE NOT (b)-[:REL]->(:C)
        |RETURN *
        |""".stripMargin

    val plan = planner.plan(q).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .apply()
      .|.antiSemiApply()
      .|.|.filter("anon_1:C")
      .|.|.expandAll("(b)-[anon_0:REL]->(anon_1)")
      .|.|.argument("b")
      .|.nodeIndexOperator("b:B(prop = ???)", paramExpr = Some(prop("a", "id")), argumentIds = Set("a"), unique = true)
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("Should plan nested index join using composite index with two predicates joining both sides") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(200)
      .setLabelCardinality("A", 100)
      .setLabelCardinality("B", 100)
      .addNodeIndex("A", Seq("id"), existsSelectivity = 1.0, uniqueSelectivity = 1 / 100.0, isUnique = true)
      .addNodeIndex("B", Seq("x", "y"), existsSelectivity = 1.0, uniqueSelectivity = 1 / 100.0, isUnique = true)
      .build()

    val query =
      """
        |MATCH (a:A {id: 15})
        |MATCH (b:B {x: a.prop, y: a.otherProp})
        |RETURN count(*)
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
      .apply()
      .|.nodeIndexOperator(
        "b:B(x = ???, y = ???)",
        paramExpr = Seq(prop("a", "prop"), prop("a", "otherProp")),
        argumentIds = Set("a"),
        unique = true
      )
      .nodeIndexOperator("a:A(id = 15)", unique = true)
      .build()
  }

  test(
    "Should plan nested index join using composite index with two predicates joining both sides, 2 different LHS dependencies"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(200)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("A", 100)
      .setLabelCardinality("B", 100)
      .setRelationshipCardinality("(:A)-[]->(:A)", 100)
      .setRelationshipCardinality("()-[]->(:A)", 100)
      .setRelationshipCardinality("(:A)-[]->()", 100)
      .addNodeIndex("A", Seq("id"), existsSelectivity = 1.0, uniqueSelectivity = 1 / 100.0, isUnique = true)
      .addNodeIndex("B", Seq("x", "y"), existsSelectivity = 1.0, uniqueSelectivity = 1 / 100.0, isUnique = true)
      .build()

    val query =
      """
        |MATCH (a:A {id: 15})-->(anotherA:A)
        |  WHERE anotherA.id > 123
        |MATCH (b:B {x: a.prop, y: anotherA.otherProp})
        |RETURN count(*)
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
      .apply()
      .|.nodeIndexOperator(
        "b:B(x = ???, y = ???)",
        paramExpr = Seq(prop("a", "prop"), prop("anotherA", "otherProp")),
        argumentIds = Set("a", "anotherA", "anon_0"),
        unique = true
      )
      .filter("anotherA.id > 123", "anotherA:A")
      .expandAll("(a)-[anon_0]->(anotherA)")
      .nodeIndexOperator("a:A(id = 15)", unique = true)
      .build()
  }

  test(
    "should prefer cartesian product + index seek with low cardinality over nested-index-join with high cardinality"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setLabelCardinality("A", 1000)
      .setLabelCardinality("B", 5000)
      .addNodeIndex("A", Seq("id"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 1000.0, isUnique = true)
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 0.1)
      .addNodeIndex("B", Seq("otherProp"), existsSelectivity = 1.0, uniqueSelectivity = 0.5)
      .build()

    val query =
      """
        |MATCH (a:A {id: 1}), (b:B {prop: 2})
        |WHERE b.otherProp < a.otherProp
        |RETURN a, b
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("b.otherProp < cacheN[a.otherProp]")
      .cartesianProduct()
      .|.nodeIndexOperator("b:B(prop = 2)")
      .cacheProperties("cacheNFromStore[a.otherProp]")
      .nodeIndexOperator("a:A(id = 1)", unique = true)
      .build()
  }
}
