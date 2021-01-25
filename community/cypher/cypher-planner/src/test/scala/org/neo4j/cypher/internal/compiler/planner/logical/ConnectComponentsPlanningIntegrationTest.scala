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
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins.COMPONENT_THRESHOLD_FOR_CARTESIAN_PRODUCT
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.util.Random

class ConnectComponentsPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport with AstConstructionTestSupport {

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
         .enableConnectComponentsPlanner()

  test("should build cartesian product with sorted plan left for many disconnected components") {
    val nodes = (0 until COMPONENT_THRESHOLD_FOR_CARTESIAN_PRODUCT).map(i => s"(n$i:Few)").mkString(",")
    val orderedNode = s"n${COMPONENT_THRESHOLD_FOR_CARTESIAN_PRODUCT}"

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Few", 2)
      .setLabelCardinality("Many", 50)
      .addIndex("Many", Seq("prop"), 0.5, 0.01, providesOrder = IndexOrderCapability.BOTH)
      .build()
      .plan(
        s"""MATCH $nodes, ($orderedNode:Many)
           |WHERE exists($orderedNode.prop)
           |RETURN * ORDER BY $orderedNode.prop
           |""".stripMargin)

    // We do not want a Sort
    plan.stripProduceResults shouldBe a[CartesianProduct]
    // Sorted index should be placed on the left of the cartesian products
    plan.leftmostLeaf should beLike {
      case NodeIndexScan(`orderedNode`, _, _, _, _) => ()
    }
  }

  test("should build left deep tree of lots of disconnected components if no joins are possible ") {
    val c = 10
    val labelsAndNumbers = (0 until c).map(i => (s"Label$i", i))
    val varsAndLabels = labelsAndNumbers.map { case (label, num) => (s"n$num", label) }
    val patterns = Random.shuffle(varsAndLabels.map { case (v, l) => s"($v:$l)" }).mkString(",")

    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinalities(labelsAndNumbers.toMap.mapValues(_.toDouble))
      .build()

    val plan = cfg.plan(s"MATCH $patterns RETURN n0")

    val leftDeepPlan = varsAndLabels
      .tail
      .foldRight(cfg.planBuilder().produceResults(varsAndLabels.head._1)) { case ((v, label), builder) =>
        builder
          .cartesianProduct()
          .|.nodeByLabelScan(v, label)
      }
      .nodeByLabelScan(varsAndLabels.head._1, varsAndLabels.head._2)
      .build()

    plan shouldEqual leftDeepPlan
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
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 300)
      .setLabelCardinality("B", 200)
      .setLabelCardinality("C", 100)
      .build()

    val plan = cfg.plan(
      """MATCH (a:A), (b:B), (c:C)
        |RETURN a, b, c
        |""".stripMargin)

    // C is cheapest so it should be furthest to the left, followed by B and A
    // Both these variants have the same cost (cardinalities denoted with '):
    // C x (B x A) = c + c'(b + b'a)
    //             = c + c'b + c'b'a
    // (C x B) x A = (c + c'b) + (c'b')a
    //             = c + c'b + c'b'a
    plan should (equal(cfg.planBuilder()
                          .produceResults("a", "b", "c")
                          .cartesianProduct()
                          .|.cartesianProduct()
                          .|.|.nodeByLabelScan("a", "A")
                          .|.nodeByLabelScan("b", "B")
                          .nodeByLabelScan("c", "C")
                          .build()
    ) or equal(cfg.planBuilder()
                  .produceResults("a", "b", "c")
                  .cartesianProduct()
                  .|.nodeByLabelScan("a", "A")
                  .cartesianProduct()
                  .|.nodeByLabelScan("b", "B")
                  .nodeByLabelScan("c", "C")
                  .build()
    ))
  }

  test("should plan cartesian product of two plans so the cost is minimized") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 30)
      .setLabelCardinality("B", 20)
      .build()

    val plan = cfg.plan(
      """MATCH (a:A), (b:B)
        |RETURN a, b
        |""".stripMargin)

    // A x B = 30 * 2 + 30 * (20 * 2) => 1260
    // B x A = 20 * 2 + 20 * (30 * 2) => 1240

    plan shouldEqual (cfg.planBuilder()
                         .produceResults("a", "b")
                         .cartesianProduct()
                         .|.nodeByLabelScan("a", "A")
                         .nodeByLabelScan("b", "B")
                         .build()
      )
  }

  test("should plan cartesian product of two plans so the cost is minimized, even if cardinality is way lower on one side.") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .build()

    val plan = cfg.plan(
      """MATCH (a:A), (b:A)
        |WHERE a.prop = 0
        |RETURN a
        |""".stripMargin)

    // A x B = 11 +  1 * 10 => 21
    // B x A = 10 + 10 * 11 => 121

    plan shouldEqual (cfg.planBuilder()
                         .produceResults("a")
                         .cartesianProduct()
                         .|.nodeByLabelScan("b", "A")
                         .filter("a.prop = 0")
                         .nodeByLabelScan("a", "A")
                         .build()
      )
  }

  test("should not plan apply with independent rhs") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Awesome", 10)
      .addIndex("Awesome", Seq("prop1"), 0.5, 0.01)
      .addIndex("Awesome", Seq("prop2"), 0.5, 0.01)
      .build()

    val plan = cfg.plan(
      """MATCH (n:Awesome), (m:Awesome)
        |WHERE n.prop1 < 42 AND m.prop2 < 42
        |  AND n.prop1 = m.prop2
        |RETURN n
        |""".stripMargin)

    plan.stripProduceResults shouldNot beLike {
      case Selection(_, Apply(_, NodeIndexSeek(_, _, _, _, args, _))) if args.isEmpty => ()
    }
  }

  test("should plan nested index join or value hash join where rhs depends on lhs") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Awesome", 10)
      .addIndex("Awesome", Seq("prop1"), 0.5, 0.01)
      .addIndex("Awesome", Seq("prop2"), 0.5, 0.01)
      .addIndex("Awesome", Seq("prop3"), 0.5, 0.01)
      .build()

    val plan = cfg.plan(
      """MATCH (n:Awesome), (m:Awesome)
        |WHERE n.prop1 < 42 AND m.prop2 < 42
        |  AND n.prop3 = m.prop4
        |RETURN n
        |""".stripMargin)

    val beSolvedByApply = beLike {
      case Selection(_, Apply(
      NodeIndexSeek(_, _, _, RangeQueryExpression(_), _, _),
      NodeIndexSeek(_, _, _, SingleQueryExpression(_), _, _))) => ()
    }
    val beSolvedByJoin = beLike {
      case ValueHashJoin(
      NodeIndexSeek(_, _, _, RangeQueryExpression(_), _, _),
      NodeIndexSeek(_, _, _, RangeQueryExpression(_), _, _), _) => ()
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
        |RETURN n""".stripMargin)

    plan should (equal(cfg.planBuilder()
                          .produceResults("n")
                          .valueHashJoin("n.prop1 = m.prop2")
                          .|.allNodeScan("m")
                          .allNodeScan("n")
                          .build()
    ) or equal(cfg.planBuilder()
                  .produceResults("n")
                  .valueHashJoin("m.prop2 = n.prop1")
                  .|.allNodeScan("n")
                  .allNodeScan("m")
                  .build()
    ))
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
        |""".stripMargin)

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
        |""".stripMargin)

    val joinOnFirst = cfg.planBuilder()
                         .produceResults("n")
                         .filter("cache[n.prop2] = cache[m.prop2]")
                         .valueHashJoin("m.prop1 = n.prop1")
                         .|.cacheProperties("cache[n.prop2]")
                         .|.nodeByLabelScan("n", "N")
                         .cacheProperties("cache[m.prop2]")
                         .nodeByLabelScan("m", "M")
                         .build()

    val joinOnSecond = cfg.planBuilder()
                          .produceResults("n")
                          .filter("cache[n.prop1] = cache[m.prop1]")
                          .valueHashJoin("m.prop2 = n.prop2")
                          .|.cacheProperties("cache[n.prop1]")
                          .|.nodeByLabelScan("n", "N")
                          .cacheProperties("cache[m.prop1]")
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
        |RETURN n""".stripMargin)

    plan shouldEqual cfg.planBuilder()
                        .produceResults("n")
                        .valueHashJoin("n.prop1 = m.prop1")
                        .|.valueHashJoin("o.prop2 = m.prop2") // connecting m and o is cheaper than connecting n and m
                        .|.|.nodeByLabelScan("m", "M")
                        .|.nodeByLabelScan("o", "O") // o should be on the left, its cheaper than m
                        .nodeByLabelScan("n", "N") // n as the single component should be on the left when joined with 2 components
                        .build()
  }

  test("cheap optional match that requires no components to be connected should be solved before any components are connected") {
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
        |""".stripMargin)

    plan.findByClass[OptionalExpand].lhs should contain(cfg.subPlanBuilder().nodeByLabelScan("n", "N").build())
  }

  test("expensive optional match is solved after components are connected") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10000)
      .setLabelCardinality("N", 10)
      .setLabelCardinality("M", 50)
      .setRelationshipCardinality("(:N)-[]-()", 10000) // Cardinality is increased a lot by optional match
      .build()

    val plan = cfg.plan(
      """MATCH (n:N), (m:M)
        |OPTIONAL MATCH (n)-[r1]-(x)
        |RETURN n
        |""".stripMargin)

    plan shouldEqual cfg.planBuilder()
                        .produceResults("n")
                        .optionalExpandAll("(n)-[r1]-(x)")
                        .cartesianProduct()
                        .|.nodeByLabelScan("m", "M")
                        .nodeByLabelScan("n", "N")
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
        |""".stripMargin)

    plan shouldEqual cfg.planBuilder()
                        .produceResults("n")
                        .optionalExpandAll("(n)-[r1]->(x)", Some("x:X"))
                        .optionalExpandAll("(n)-[r2]->(y)", Some("y:Y"))
                        .nodeByLabelScan("n", "N")
                        .build()
  }

  test("cheap optional match is solved early even though it appears late in the query") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setAllRelationshipsCardinality(1000)
      .setLabelCardinality("N", 10)
      .setLabelCardinality("M", 20)
      .setRelationshipCardinality("(:N)-[]->()", 10)
      .setRelationshipCardinality("()-[]->(:M)", 1000)
      .setRelationshipCardinality("(:N)-[]->(:M)", 10)
      .build()

    val plan = cfg.plan(
      """MATCH (n:N), (m:M)
        |OPTIONAL MATCH (n)-[r1]->(m)
        |OPTIONAL MATCH (n)-[r2]->(y)
        |RETURN n
        |""".stripMargin)

    plan shouldEqual cfg.planBuilder()
                        .produceResults("n")
                        .optionalExpandInto("(n)-[r1]->(m)")
                        .cartesianProduct()
                        .|.nodeByLabelScan("m", "M")
                        .optionalExpandAll("(n)-[r2]->(y)")
                        .nodeByLabelScan("n", "N")
                        .build()
  }

  test("cheap optional match that requires 2 components to be connected should be solved before other components are connected") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("N", 40)
      .setLabelCardinality("M", 50)
      .setLabelCardinality("O", 90)
      .setRelationshipCardinality("(:N)-[]-(:M)", 10)
      .setRelationshipCardinality("(:M)-[]-()", 10)
      .build()

    val plan = cfg.plan(
      """MATCH (n:N), (m:M), (o:O) WHERE n.prop = m.prop
        |OPTIONAL MATCH (n)-[r1]-(m)-[r2]-(x)
        |RETURN n
        |""".stripMargin)

    plan shouldEqual cfg.planBuilder()
                        .produceResults("n")
                        .cartesianProduct()
                        .|.nodeByLabelScan("o", "O")
                        .apply()
                        .|.optional("n", "m")
                        .|.filter("not r1 = r2")
                        .|.expandInto("(n)-[r1]-(m)")
                        .|.expandAll("(m)-[r2]-(x)")
                        .|.argument("n", "m")
                        .valueHashJoin("n.prop = m.prop")
                        .|.nodeByLabelScan("m", "M")
                        .nodeByLabelScan("n", "N")
                        .build()
  }

  test("when ordering by a variable introduced by an optional match, choose a plan that keeps the order from the optional match subplan") {
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
        |""".stripMargin)

    plan shouldEqual cfg.planBuilder()
                        .produceResults("n")
                        .cartesianProduct()
                        .|.nodeByLabelScan("o", "O")
                        .leftOuterHashJoin("n", "m") // Keeps RHS order
                        .|.filter("not r1 = r2")
                        .|.expandAll("(m)-[r1]-(n)")
                        .|.expandAll("(x)-[r2]-(m)")
                        .|.sort(Seq(Ascending("x.prop")))
                        .|.projection("x.prop AS `x.prop`")
                        .|.allNodeScan("x")
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
        |""".stripMargin)

    val numSorts = plan.treeCount { case _: Sort => true }
    numSorts shouldEqual 1
  }

  test("when ordering by a variable introduced before the optional match, choose a plan that keeps the order through solving the optional match") {
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
        |RETURN n ORDER BY m.prop
        |""".stripMargin)

    plan shouldEqual cfg.planBuilder()
                        .produceResults("n")
                        .cartesianProduct()
                        .|.nodeByLabelScan("o", "O")
                        .rightOuterHashJoin("n", "m")
                        .|.cartesianProduct()
                        .|.|.nodeByLabelScan("n", "N")
                        .|.sort(Seq(Ascending("m.prop")))
                        .|.projection("m.prop AS `m.prop`")
                        .|.nodeByLabelScan("m", "M")
                        .filter("not r1 = r2")
                        .expandAll("(m)-[r1]-(n)")
                        .expandAll("(x)-[r2]-(m)")
                        .allNodeScan("x")
                        .build()
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
         |""".stripMargin)

    val allNodesScanned = plan.treeFold(Seq.empty[String]) {
      case a: AllNodesScan => ids => TraverseChildren(ids :+ a.idName)
    }
    val optionalExpanded = plan.treeFold(Seq.empty[String]) {
      case o: OptionalExpand => ids => TraverseChildren(ids :+ o.to)
    }

    plan.availableSymbols should contain allElementsOf (componentVars ++ optionalMatchVars)
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
        |""".stripMargin)

    val allNodesScanned = plan.treeFold(Seq.empty[String]) {
      case a: AllNodesScan => ids => TraverseChildren(ids :+ a.idName)
    }
    val optionalExpanded = plan.treeFold(Seq.empty[String]) {
      case o: OptionalExpand => ids => TraverseChildren(ids :+ o.to)
    }

    plan.availableSymbols should contain allElementsOf Seq("n", "m", "x", "y")
    allNodesScanned should contain theSameElementsAs Seq("n", "m", "y")
    optionalExpanded should contain theSameElementsAs Seq("x")
  }

  test("shortest path should not fail to get planned after nested index join") {

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 20)
      .setLabelCardinality("M", 20)
      .addIndex("N", Seq("loc"), 1.0, 0.1)
      .build()

    val plan = cfg.plan(
      """MATCH (n:N), (m:M)
        |WHERE n.loc = m.loc
        |WITH n, m
        |MATCH p=shortestPath((n)-[r:R *]-(m))
        |RETURN n, m
        |""".stripMargin)

    plan shouldEqual (
      cfg.planBuilder()
        .produceResults("n", "m")
        .shortestPath("(n)-[r:R*1..]-(m)", pathName = Some("p"))
        .apply()
        .|.nodeIndexOperator("n:N(loc = ???)", paramExpr = Some(prop("m", "loc")), argumentIds = Set("m"))
        .nodeByLabelScan("m", "M")
        .build()
      )
  }
}
