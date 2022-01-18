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

import org.apache.commons.io.FileUtils
import org.neo4j.cypher.graphcounts.GraphCountsJson
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.QueryGraphSolverSetup
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.QueryGraphSolverWithGreedyConnectComponents
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.QueryGraphSolverWithIDPConnectComponents
import org.neo4j.cypher.internal.compiler.planner.LookupRelationshipsByTypeDisabled
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.unnestOptional
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.planner.spi.DelegatingGraphStatistics
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Inside

class OptionalMatchIDPPlanningIntegrationTest extends OptionalMatchPlanningIntegrationTest(QueryGraphSolverWithIDPConnectComponents)
class OptionalMatchGreedyPlanningIntegrationTest extends OptionalMatchPlanningIntegrationTest(QueryGraphSolverWithGreedyConnectComponents)

abstract class OptionalMatchPlanningIntegrationTest(queryGraphSolverSetup: QueryGraphSolverSetup)
  extends CypherFunSuite
  with LogicalPlanningTestSupport2
  with LogicalPlanningIntegrationTestSupport with Inside {

  locally {
    queryGraphSolver = queryGraphSolverSetup.queryGraphSolver()
  }

  test("should build plans containing left outer joins") {
    (new given {
      cost = {
        case (_: AllNodesScan, _, _, _) => 2000000.0
        case (_: NodeByLabelScan, _, _, _) => 20.0
        case (p: Expand, _, _, _) if p.findAllByClass[CartesianProduct].nonEmpty => Double.MaxValue
        case (_: Expand, _, _, _) => 10.0
        case (_: LeftOuterHashJoin, _, _, _) => 20.0
        case (_: Argument, _, _, _) => 1.0
        case _ => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._2 should equal(
      LeftOuterHashJoin(Set("b"),
        Expand(NodeByLabelScan("a", labelName("X"), Set.empty, IndexOrderNone), "a", SemanticDirection.OUTGOING, Seq(), "b", "r1"),
        Expand(NodeByLabelScan("c", labelName("Y"), Set.empty, IndexOrderNone), "c", SemanticDirection.INCOMING, Seq(), "b", "r2")
      )
    )
  }

  test("should build plans containing right outer joins") {
    (new given {
      cost = {
        case (_: AllNodesScan, _, _, _) => 2000000.0
        case (_: NodeByLabelScan, _, _, _) => 20.0
        case (p: Expand, _, _, _) if p.findAllByClass[CartesianProduct].nonEmpty => Double.MaxValue
        case (_: Expand, _, _, _) => 10.0
        case (_: RightOuterHashJoin, _, _, _) => 20.0
        case (_: Argument, _, _, _) => 1.0
        case _ => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._2 should equal(
      RightOuterHashJoin(Set("b"),
        Expand(NodeByLabelScan("c", labelName("Y"), Set.empty, IndexOrderNone), "c", SemanticDirection.INCOMING, Seq(), "b", "r2"),
        Expand(NodeByLabelScan("a", labelName("X"), Set.empty, IndexOrderNone), "a", SemanticDirection.OUTGOING, Seq(), "b", "r1")
      )
    )
  }

  test("should choose left outer join if lhs has small cardinality") {
    (new given {
      labelCardinality = Map("X" -> 1.0, "Y" -> 10.0)
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def patternStepCardinality(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality = {
          // X = 0, Y = 1
          if (fromLabel.exists(_.id == 0) && relTypeId.isEmpty && toLabel.isEmpty) {
            // low from a to b
            100.0
          } else if (fromLabel.isEmpty && relTypeId.isEmpty && toLabel.exists(_.id == 1)) {
            // high from b to c
            1000000000.0
          } else {
            super.patternStepCardinality(fromLabel, relTypeId, toLabel)
          }
        }
      }
      cost = {
        case (_: OptionalExpand, _, _, _) => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._2 should equal(
      LeftOuterHashJoin(Set("b"),
        Expand(NodeByLabelScan("a", labelName("X"), Set.empty, IndexOrderNone), "a", SemanticDirection.OUTGOING, Seq(), "b", "r1"),
        Expand(NodeByLabelScan("c", labelName("Y"), Set.empty, IndexOrderNone), "c", SemanticDirection.INCOMING, Seq(), "b", "r2")
      )
    )
  }

  test("should choose right outer join if rhs has small cardinality") {
    (new given {
      labelCardinality = Map("X" -> 10.0, "Y" -> 1.0)
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def patternStepCardinality(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality = {
          // X = 0, Y = 1
          if (fromLabel.exists(_.id == 0) && relTypeId.isEmpty && toLabel.isEmpty) {
            // high from a to b
            1000000000.0
          } else if ( fromLabel.isEmpty && relTypeId.isEmpty && toLabel.exists(_.id == 1)) {
            // low from b to c
            100.0
          } else {
            super.patternStepCardinality(fromLabel, relTypeId, toLabel)
          }
        }
      }
      cost = {
        case (_: OptionalExpand, _, _, _) => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._2 should equal(
      RightOuterHashJoin(Set("b"),
        Expand(NodeByLabelScan("c", labelName("Y"), Set.empty, IndexOrderNone), "c", SemanticDirection.INCOMING, Seq(), "b", "r2"),
        Expand(NodeByLabelScan("a", labelName("X"), Set.empty, IndexOrderNone), "a", SemanticDirection.OUTGOING, Seq(), "b", "r1")
      )
    )
  }

  test("should build simple optional match plans") { // This should be built using plan rewriting
    planFor("OPTIONAL MATCH (a) RETURN a")._2 should equal(
      Optional(AllNodesScan("a", Set.empty)))
  }

  test("should allow MATCH after OPTIONAL MATCH") {
    planFor("OPTIONAL MATCH (a) MATCH (b) RETURN a, b")._2 should equal(
      Apply(
        Optional(AllNodesScan("a", Set.empty)),
        AllNodesScan("b", Set("a"))
      )
    )
  }

  test("should allow MATCH after OPTIONAL MATCH on same node") {
    planFor("OPTIONAL MATCH (a) MATCH (a:A) RETURN a")._2 should equal(
      Selection(Seq(HasLabels(varFor("a"), Seq(LabelName("A")(pos)))(pos)),
        Optional(AllNodesScan("a", Set.empty)))
    )
  }

  test("should build simple optional expand") {
    planFor("MATCH (n) OPTIONAL MATCH (n)-[:NOT_EXIST]->(x) RETURN n")._2.endoRewrite(unnestOptional) match {
      case OptionalExpand(
      AllNodesScan("n", _),
      "n",
      SemanticDirection.OUTGOING,
      _,
      "x",
      _,
      _,
      _,
      ) => ()
    }
  }

  test("should build optional ProjectEndpoints") {
    planFor("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2")._2 match {
      case
        Apply(
        Limit(
        Expand(
        AllNodesScan(_, _), _, _, _, _, _, _), _),
        Optional(
        ProjectEndpoints(
        Argument(args), "r", "b2", false, "a1", true, None, true, SimplePatternLength
        ), _
        ), _
        ) =>
        args should equal(Set("r", "a1"))
    }
  }

  test("should build optional ProjectEndpoints with extra predicates") {
    planFor("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2")._2 match {
      case Apply(
      Limit(Expand(AllNodesScan(_, _), _, _, _, _, _, _), _),
      Optional(
      Selection(
      predicates,
      ProjectEndpoints(
      Argument(args),
      "r", "b2", false, "a2", false, None, true, SimplePatternLength
      )
      ), _
      ), _
      ) =>
        args should equal(Set("r", "a1"))
        val predicate = equals(varFor("a1"), varFor("a2"))
        predicates.exprs should equal(Seq(predicate))
    }
  }

  test("should build optional ProjectEndpoints with extra predicates 2") {
    planFor("MATCH (a1)-[r]->(b1) WITH r LIMIT 1 OPTIONAL MATCH (a2)-[r]->(b2) RETURN a2, r, b2")._2 match {
      case Apply(
      Limit(Expand(AllNodesScan(_, _), _, _, _, _, _, _), _),
      Optional(
      ProjectEndpoints(
      Argument(args),
      "r", "a2", false, "b2", false, None, true, SimplePatternLength
      ), _
      ), _
      ) =>
        args should equal(Set("r"))
    }
  }

  test("should solve multiple optional matches") {
    val plan = planFor("MATCH (a) OPTIONAL MATCH (a)-[r1:R1]->(x1) OPTIONAL MATCH (a)-[r2:R2]->(x2) RETURN a, x1, x2")._2
    val alternative1 = new LogicalPlanBuilder(wholePlan = false)
      .optionalExpandAll("(a)-[r2:R2]->(x2)")
      .optionalExpandAll("(a)-[r1:R1]->(x1)")
      .allNodeScan("a")
      .build()
    val alternative2 = new LogicalPlanBuilder(wholePlan = false)
      .optionalExpandAll("(a)-[r1:R1]->(x1)")
      .optionalExpandAll("(a)-[r2:R2]->(x2)")
      .allNodeScan("a")
      .build()
    plan should (equal(alternative1) or equal(alternative2))
  }

  test("should solve optional matches with arguments and predicates") {
    val plan = new given {
      cost = {
        case (_: Expand, _, _, _) => 1000.0
      }
    }.getLogicalPlanFor(
      """MATCH (n:X)
        |OPTIONAL MATCH (n)-[r]-(m:Y)
        |WHERE m.prop = 42
        |RETURN m""".stripMargin)._2.endoRewrite(unnestOptional)
    val allNodesN: LogicalPlan = NodeByLabelScan("n", labelName("X"), Set.empty, IndexOrderNone)

    plan should equal(
      OptionalExpand(allNodesN, "n", SemanticDirection.BOTH, Seq.empty, "m", "r", ExpandAll,
        Some(Ands.create(Seq(hasLabels("m", "Y"), equals(prop("m", "prop"), literalInt(42))))))
    )
  }

  test("should plan for large number of optional matches without numerical overflow in estimatedRows") {
    val largeOptionalMatchStructureGraphCounts = GraphCountsJson.parseAsGraphCountData(
      FileUtils.toFile(classOf[BenchmarkCardinalityEstimationTest].getResource("/largeOptionalMatchStructure.json"))
    )

    val planner = plannerBuilder()
      .processGraphCounts(largeOptionalMatchStructureGraphCounts)
      .build()

    val query =
      """
        |MATCH (me:Label1)-[rmeState:REL1]->(meState:Label2 {deleted: 0})
        |USING INDEX meState:Label2(id)
        |WHERE meState.id IN [63241]
        |WITH *
        |OPTIONAL MATCH(n1:Label3 {deleted: 0})<-[:REL1]-(:Label4)<-[:REL2]-(meState)
        |OPTIONAL MATCH(n2:Label5 {deleted: 0})<-[:REL1]-(:Label6)<-[:REL2]-(meState)
        |OPTIONAL MATCH(n3:Label7 {deleted: 0})<-[:REL1]-(:Label8)<-[:REL2]-(meState)
        |OPTIONAL MATCH(n4:Label9 {deleted: 0})<-[:REL1]-(:Label10) <-[:REL2]-(meState)
        |OPTIONAL MATCH p1 = (:Label2 {deleted: 0})<-[:REL1|REL3*]-(meState)
        |OPTIONAL MATCH(:Label11 {deleted: 0})<-[r1:REL1]-(:Label12) <-[:REL5]-(meState)
        |OPTIONAL MATCH(:Label13 {deleted: 0})<-[r2:REL1]-(:Label14) <-[:REL6]-(meState)
        |OPTIONAL MATCH(:Label15 {deleted: 0})<-[r3:REL1]-(:Label16) <-[:REL7]-(meState)
        |OPTIONAL MATCH(:Label17 {deleted: 0})<-[r4:REL1]-(:Label18)<-[:REL8]-(meState)
        |
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r5:REL1]-(:Label20) <-[:REL2]-(n1)
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r6:REL1]-(:Label20)<-[:REL2]-(n1)
        |
        |OPTIONAL MATCH(n5:Label21 {deleted: 0})<-[:REL1]-(:Label22)<-[:REL2]-(n2)
        |
        |OPTIONAL MATCH(n6:Label3 {deleted: 0})<-[:REL1]-(:Label4)<-[:REL2]-(n5)
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r7:REL1]-(:Label20)<-[:REL2]-(n5)
        |
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r8:REL1]-(:Label20)<-[:REL2]-(n6)
        |
        |OPTIONAL MATCH(n7:Label23 {deleted: 0})<-[:REL1]-(:Label24)<-[:REL2]-(n3)
        |
        |OPTIONAL MATCH(n8:Label3 {deleted: 0})<-[:REL1]-(:Label4)<-[:REL2]-(n7)
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r9:REL1]-(:Label20)<-[:REL2]-(n7)
        |
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r10:REL1]-(:Label20)<-[:REL2]-(n8)
        |
        |OPTIONAL MATCH(n9:Label25 {deleted: 0})<-[:REL1]-(:Label26)<-[:REL2]-(n4)
        |
        |OPTIONAL MATCH(n10:Label3 {deleted: 0})<-[:REL1]-(:Label4) <-[:REL2]-(n9)
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r11:REL1]-(:Label20) <-[:REL2]-(n9)
        |
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r12:REL1]-(:Label20)<-[:REL2]-(n10)
        |OPTIONAL MATCH (me)-[:REL4]->(:Label2:Label27)
        |RETURN *
      """.stripMargin

    val planState = planner.planState(query)
    val plan = planState.logicalPlan
    val cardinalities = planState.planningAttributes.cardinalities

    plan.treeExists {
      case plan: LogicalPlan =>
        cardinalities.get(plan.id) match {
          case Cardinality(amount) =>
            withClue("We should not get a NaN cardinality.") {
              amount.isNaN should not be true
            }
        }
        false // this is a "trick" to use treeExists to iterate over the whole tree
    }
  }

  test("should not plan outer hash joins when rhs has arguments other than join nodes") {
    val query = """
        |WITH 1 AS x
        |MATCH (a)
        |OPTIONAL MATCH (a)-[r]->(c)
        |WHERE c.id = x
        |RETURN c
        |""".stripMargin

    val cfg = new given {
      cost = {
        case (_: RightOuterHashJoin, _, _, _) => 1.0
        case (_: LeftOuterHashJoin, _, _, _) => 1.0
        case _ => Double.MaxValue
      }
    }

    val plan = cfg.getLogicalPlanFor(query)._2
    withClue(plan) {
      plan.treeExists {
        case _: RightOuterHashJoin => true
        case _: LeftOuterHashJoin => true
      } should be(false)
    }
  }

  test("Optional match in tail should have correct cardinality and therefore generate Argument leaf plan") {
    val query = """MATCH (a:A)
                  |WITH a
                  |LIMIT 994
                  |MATCH (:D)<-[:R1]-(a)
                  |OPTIONAL MATCH (a)-[:R1]->(:D)-[:R2]->(:B)-[:R3 {bool: false}]->(:C {some: 'prop'})
                  |RETURN count(a)""".stripMargin

    val cfg = new given {
      knownLabels = Set("A", "B", "C", "D", "E")
      knownRelationships = Set("R1", "R2", "R3")
      uniqueIndexOn("C", "prop")
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def nodesAllCardinality(): Cardinality = 1120169.0
        override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = labelId match {
          case Some(LabelId(0)) => 101.0      // C
          case Some(LabelId(2)) => 225598.0   // A
          case Some(LabelId(3)) => 41.0       // B
          case Some(LabelId(4)) => 141936.0   // D
          case _ => super.nodesWithLabelCardinality(labelId)
        }
        override def patternStepCardinality(fromLabel: Option[LabelId],
                                            relTypeId: Option[RelTypeId],
                                            toLabel: Option[LabelId]): Cardinality = (fromLabel, relTypeId, toLabel) match {
          case(Some(LabelId(2)), Some(RelTypeId(0)), None) => 223600.0                  // A - [R1] -> *
          case(Some(LabelId(2)), Some(RelTypeId(0)), Some((LabelId(4)))) => 223600.0    // A - [R1] -> D
          case(None, Some(RelTypeId(1)), Some(LabelId(3))) => 139911.0                  // * - [R2] -> B
          case(Some(LabelId(4)), Some(RelTypeId(1)), Some(LabelId(3))) => 139911.0      // D - [R2] -> B
          case(Some(LabelId(4)), Some(RelTypeId(1)), None) => 139911.0                  // D - [R2] -> *
          case(Some(LabelId(3)), Some(RelTypeId(2)), Some(LabelId(0))) => 1477.0        // B - [R3] -> C
          case(Some(LabelId(3)), Some(RelTypeId(2)), None) => 1477.0                    // B - [R3] -> *
          case(None, Some(RelTypeId(2)), Some(LabelId(0))) => 113740.0                  // * - [R3] -> C
          case _ => 0.0
        }
      }
      lookupRelationshipsByType = LookupRelationshipsByTypeDisabled
    }

    val (_, plan, table,_) = cfg.getLogicalPlanFor(query)
    inside(plan) {
      case Aggregation(Apply(_, rhs, _), _, _) =>
        rhs.leaves.foreach( leaf => leaf shouldBe an [Argument])
    }
  }

  test("should prefer OptionalExpand over hasLabel with join") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(4792670 / 2)
      .setLabelCardinality("Foo", 137)
      .setLabelCardinality("Model", 0)
      .setAllRelationshipsCardinality(4792670)
      .setRelationshipCardinality("()-[]-(:Foo)", 30)
      .build()

    val plan = cfg.plan(
      s"""
         |MATCH (a)
         |OPTIONAL MATCH (a)-[r]-(b:Foo)
         |WHERE NOT b:Model
         |  AND ANY (p IN b.latest WHERE p IN [1, 2])
         |RETURN a""".stripMargin)

    plan.stripProduceResults shouldBe an[OptionalExpand]
  }

  test("should pick a right outer hash join with hint if it is cheaper than left outer hash join, also in a tail query") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(52)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("B", 2)
      .setRelationshipCardinality("()-[]-()", 30)
      .setRelationshipCardinality("(:A)-[]-()", 30)
      .setRelationshipCardinality("()-[]-(:B)", 30)
      .setRelationshipCardinality("(:A)-[]-(:B)", 30)
      .build()

    val noTailQuery =
      """MATCH (a:A)
        |OPTIONAL MATCH (a)-[r]->(b:B)
        |USING JOIN ON a
        |RETURN a.name, b.name""".stripMargin
    val tailQuery =
      s"""MATCH (a:A)
         |WITH a, 1 AS foo
         |MATCH (a)
         |OPTIONAL MATCH (a)-[r]->(b:B)
         |USING JOIN ON a
         |RETURN a.name, b.name""".stripMargin

    val noTailPlan = cfg.plan(noTailQuery)
    val tailPlan = cfg.plan(tailQuery)

    withClue(noTailPlan) {
      noTailPlan.treeExists {
        case _: RightOuterHashJoin => true
      } should be(true)
    }
    withClue(tailPlan) {
      tailPlan.treeExists {
        case _: RightOuterHashJoin => true
      } should be(true)
    }
  }

  test("should produce a valid plan for optional match with solved arguments passed to node by label scan") {
    val config = new given {
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def nodesAllCardinality(): Cardinality = 100000.0
      }
    }

    val query =
      """MATCH (n0), (n1)
        |OPTIONAL MATCH (n0)--(:L0), (n1)
        |RETURN n0
        |LIMIT 0""".stripMargin

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._2

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.expandInto("(n0)-[anon_0]-(anon_1)")
      .|.nodeByLabelScan("anon_1", "L0", IndexOrderNone, "n0", "n1")
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should equal(expected)
  }

  test("should produce a valid plan for optional match with solved arguments passed to node index scan") {
    val config = new given {
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def nodesAllCardinality(): Cardinality = 100000.0
      }
      indexOn("L0", "prop")
    }

    val query =
      """MATCH (n0), (n1)
        |OPTIONAL MATCH (n0)--(:L0 {prop: 42}), (n1)
        |RETURN n0
        |LIMIT 0""".stripMargin

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._2

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.expandInto("(n0)-[anon_0]-(anon_1)")
      .|.nodeIndexOperator("anon_1:L0(prop = 42)", indexOrder = IndexOrderNone, argumentIds = Set("n0", "n1"))
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    // due to effective cardinality being zero, filtering on `prop` inside the index operator or separately leads to the same cost – both are valid plans
    val otherwiseExpected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.expandInto("(n0)-[anon_0]-(anon_1)")
      .|.filter("anon_1.prop = 42")
      .|.nodeIndexOperator("anon_1:L0(prop)", indexOrder = IndexOrderNone, argumentIds = Set("n0", "n1"))
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should (equal(expected) or equal(otherwiseExpected))
  }

  test("should produce a valid plan for optional match with solved arguments passed to a relationship index scan") {
    val config = new given {
      cost = {
        case (expand: OptionalExpand, _, _, _) if expand.mode == ExpandAll => Double.MaxValue
      }
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def nodesAllCardinality(): Cardinality = 100000.0
      }
      relationshipIndexOn("R0", "prop")
    }

    val query =
      """MATCH (n0), (n1)
        |OPTIONAL MATCH (n0)--()-[:R0 {prop: 42}]->(), (n1)
        |RETURN n0
        |LIMIT 0""".stripMargin

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._2

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.filter("not anon_0 = anon_2")
      .|.expandInto("(n0)-[anon_0]-(anon_1)")
      .|.relationshipIndexOperator("(anon_1)-[anon_2:R0(prop = 42)]->(anon_3)", indexOrder = IndexOrderNone, argumentIds = Set("n0", "n1"))
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    // due to effective cardinality being zero, filtering on `prop` inside the index operator or separately leads to the same cost – both are valid plans
    val otherwiseExpected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.filter("not anon_0 = anon_2")
      .|.expandInto("(n0)-[anon_0]-(anon_1)")
      .|.filter("anon_2.prop = 42")
      .|.relationshipIndexOperator("(anon_1)-[anon_2:R0(prop)]->(anon_3)", indexOrder = IndexOrderNone, argumentIds = Set("n0", "n1"))
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should (equal(expected) or equal(otherwiseExpected))
  }

  test("should produce a valid plan for optional match with solved arguments passed to an undirected relationship type scan") {
    val config = new given {
      cost = {
        case (expand: OptionalExpand, _, _, _) if expand.mode == ExpandAll => Double.MaxValue
      }
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def nodesAllCardinality(): Cardinality = 100000.0
      }
    }

    val query =
      """MATCH (n0), (n1)
        |OPTIONAL MATCH (n0)-[:REL]-(), (n1)
        |RETURN n0
        |LIMIT 0""".stripMargin

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._2

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.filter("n0 = anon_2")
      .|.relationshipTypeScan("(anon_2)-[anon_0:REL]-(anon_1)", IndexOrderNone, "n0", "n1")
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should equal(expected)
  }

  test("should produce a valid plan for optional match with solved arguments passed to a directed relationship type scan") {
    val config = new given {
      cost = {
        case (expand: OptionalExpand, _, _, _) if expand.mode == ExpandAll => Double.MaxValue
      }
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def nodesAllCardinality(): Cardinality = 100000.0
      }
    }

    val query =
      """MATCH (n0), (n1)
        |OPTIONAL MATCH (n0)-[:REL]->(), (n1)
        |RETURN n0
        |LIMIT 0""".stripMargin

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._2

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.filter("n0 = anon_2")
      .|.relationshipTypeScan("(anon_2)-[anon_0:REL]->(anon_1)", IndexOrderNone, "n0", "n1")
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should equal(expected)
  }

  test("should produce a valid plan for optional match with solved arguments passed to a node by id seek") {
    val config = new given {
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def nodesAllCardinality(): Cardinality = 100000.0
      }
    }

    val query =
      """MATCH (n0), (n1)
        |OPTIONAL MATCH (n0)--(x),(n1)
        |WHERE id(x) = 0
        |RETURN n0
        |LIMIT 0""".stripMargin

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._2

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.expandInto("(n0)-[anon_0]-(x)")
      .|.nodeByIdSeek("x", Set("n0", "n1"), 0)
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should equal(expected)
  }

  test("should produce a valid plan for optional match with solved arguments passed to an undirected relationship by id seek") {
    val config = new given {
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def nodesAllCardinality(): Cardinality = 100000.0
      }
    }

    val query =
      """MATCH (n0), (n1)
        |OPTIONAL MATCH (n0)-[r]-(),(n1)
        |WHERE id(r) = 0
        |RETURN n0
        |LIMIT 0""".stripMargin

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._2

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply(fromSubquery = false)
      .|.optional("n0", "n1")
      .|.filter("n0 = anon_1")
      .|.undirectedRelationshipByIdSeek("r", "anon_1", "anon_0", Set("n0", "n1"), 0)
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should equal(expected)
  }

  test("should produce a valid plan for optional match with solved arguments passed to a directed relationship by id seek") {
    val config = new given {
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def nodesAllCardinality(): Cardinality = 100000.0
      }
    }

    val query =
      """MATCH (n0), (n1)
        |OPTIONAL MATCH (n0)<-[r]-(),(n1)
        |WHERE id(r) = 42
        |RETURN n0
        |LIMIT 0""".stripMargin

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._2

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.filter("n0 = anon_1")
      .|.directedRelationshipByIdSeek("r", "anon_0", "anon_1", Set("n0", "n1"), 42)
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should equal(expected)
  }
}
