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

import org.apache.commons.io.FileUtils
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.graphcounts.GraphCountsJson
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.AttributeComparisonStrategy.ComparingProvidedAttributesOnly
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.QueryGraphSolverSetup
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.QueryGraphSolverWithGreedyConnectComponents
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.QueryGraphSolverWithIDPConnectComponents
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.unnestOptional
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.andsReorderable
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.DirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
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
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType
import org.scalatest.Inside

import java.lang.Boolean.FALSE

class OptionalMatchIDPPlanningIntegrationTest
    extends OptionalMatchPlanningIntegrationTest(QueryGraphSolverWithIDPConnectComponents)

class OptionalMatchGreedyPlanningIntegrationTest
    extends OptionalMatchPlanningIntegrationTest(QueryGraphSolverWithGreedyConnectComponents)

abstract class OptionalMatchPlanningIntegrationTest(queryGraphSolverSetup: QueryGraphSolverSetup)
    extends CypherFunSuite
    with LogicalPlanningTestSupport2
    with LogicalPlanningIntegrationTestSupport
    with LogicalPlanningAttributesTestSupport
    with Inside {

  locally {
    queryGraphSolver = queryGraphSolverSetup.queryGraphSolver()
  }

  test("should build plans containing left outer joins") {
    (new givenConfig {
      cost = {
        case (_: AllNodesScan, _, _, _)                                                 => 2000000.0
        case (_: NodeByLabelScan, _, _, _)                                              => 20.0
        case (p: Expand, _, _, _) if p.folder.findAllByClass[CartesianProduct].nonEmpty => Double.MaxValue
        case (_: Expand, _, _, _)                                                       => 10.0
        case (_: LeftOuterHashJoin, _, _, _)                                            => 20.0
        case (_: Argument, _, _, _)                                                     => 1.0
        case _                                                                          => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._1 should equal(
      LeftOuterHashJoin(
        Set(v"b"),
        Expand(
          NodeByLabelScan(v"a", labelName("X"), Set.empty, IndexOrderNone),
          v"a",
          SemanticDirection.OUTGOING,
          Seq(),
          v"b",
          v"r1"
        ),
        Expand(
          NodeByLabelScan(v"c", labelName("Y"), Set.empty, IndexOrderNone),
          v"c",
          SemanticDirection.INCOMING,
          Seq(),
          v"b",
          v"r2"
        )
      )
    )
  }

  test("should build plans containing right outer joins") {
    (new givenConfig {
      cost = {
        case (_: AllNodesScan, _, _, _)                                                 => 2000000.0
        case (_: NodeByLabelScan, _, _, _)                                              => 20.0
        case (p: Expand, _, _, _) if p.folder.findAllByClass[CartesianProduct].nonEmpty => Double.MaxValue
        case (_: Expand, _, _, _)                                                       => 10.0
        case (_: RightOuterHashJoin, _, _, _)                                           => 20.0
        case (_: Argument, _, _, _)                                                     => 1.0
        case _                                                                          => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._1 should equal(
      RightOuterHashJoin(
        Set(v"b"),
        Expand(
          NodeByLabelScan(v"c", labelName("Y"), Set.empty, IndexOrderNone),
          v"c",
          SemanticDirection.INCOMING,
          Seq(),
          v"b",
          v"r2"
        ),
        Expand(
          NodeByLabelScan(v"a", labelName("X"), Set.empty, IndexOrderNone),
          v"a",
          SemanticDirection.OUTGOING,
          Seq(),
          v"b",
          v"r1"
        )
      )
    )
  }

  test("should choose left outer join if lhs has small cardinality") {
    (new givenConfig {
      labelCardinality = Map("X" -> 1.0, "Y" -> 10.0)
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def patternStepCardinality(
          fromLabel: Option[LabelId],
          relTypeId: Option[RelTypeId],
          toLabel: Option[LabelId]
        ): Cardinality = {
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
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._1 should equal(
      LeftOuterHashJoin(
        Set(v"b"),
        Expand(
          NodeByLabelScan(v"a", labelName("X"), Set.empty, IndexOrderNone),
          v"a",
          SemanticDirection.OUTGOING,
          Seq(),
          v"b",
          v"r1"
        ),
        Expand(
          NodeByLabelScan(v"c", labelName("Y"), Set.empty, IndexOrderNone),
          v"c",
          SemanticDirection.INCOMING,
          Seq(),
          v"b",
          v"r2"
        )
      )
    )
  }

  test("should choose right outer join if rhs has small cardinality") {
    (new givenConfig {
      labelCardinality = Map("X" -> 10.0, "Y" -> 1.0)
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def patternStepCardinality(
          fromLabel: Option[LabelId],
          relTypeId: Option[RelTypeId],
          toLabel: Option[LabelId]
        ): Cardinality = {
          // X = 0, Y = 1
          if (fromLabel.exists(_.id == 0) && relTypeId.isEmpty && toLabel.isEmpty) {
            // high from a to b
            1000000000.0
          } else if (fromLabel.isEmpty && relTypeId.isEmpty && toLabel.exists(_.id == 1)) {
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
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._1 should equal(
      RightOuterHashJoin(
        Set(v"b"),
        Expand(
          NodeByLabelScan(v"c", labelName("Y"), Set.empty, IndexOrderNone),
          v"c",
          SemanticDirection.INCOMING,
          Seq(),
          v"b",
          v"r2"
        ),
        Expand(
          NodeByLabelScan(v"a", labelName("X"), Set.empty, IndexOrderNone),
          v"a",
          SemanticDirection.OUTGOING,
          Seq(),
          v"b",
          v"r1"
        )
      )
    )
  }

  test("should build simple optional match plans") { // This should be built using plan rewriting
    planFor("OPTIONAL MATCH (a) RETURN a")._1 should equal(
      Optional(AllNodesScan(v"a", Set.empty))
    )
  }

  test("should allow MATCH after OPTIONAL MATCH") {
    planFor("OPTIONAL MATCH (a) MATCH (b) RETURN a, b")._1 should equal(
      Apply(
        Optional(AllNodesScan(v"a", Set.empty)),
        AllNodesScan(v"b", Set(v"a"))
      )
    )
  }

  test("should allow MATCH after OPTIONAL MATCH on same node") {
    planFor("OPTIONAL MATCH (a) MATCH (a:A) RETURN a")._1 shouldEqual
      new LogicalPlanBuilder(wholePlan = false)
        .filterExpression(assertIsNode("a"), hasLabels("a", "A"))
        .optional()
        .allNodeScan("a")
        .build()
  }

  test("should build simple optional expand") {
    planFor("MATCH (n) OPTIONAL MATCH (n)-[:NOT_EXIST]->(x) RETURN n")._1.endoRewrite(unnestOptional) match {
      case OptionalExpand(
          AllNodesScan(LogicalVariable("n"), _),
          LogicalVariable("n"),
          SemanticDirection.OUTGOING,
          _,
          LogicalVariable("x"),
          _,
          _,
          _
        ) => ()
      case plan => throw new IllegalArgumentException(s"Unexpected plan: $plan")
    }
  }

  test("should build optional ProjectEndpoints") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setAllRelationshipsCardinality(10)
      .build()

    val query = "MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2"
    val plan = planner.plan(query).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.optional("r", "a1")
        .|.projectEndpoints("(a1)<-[r]-(b2)", startInScope = true, endInScope = false)
        .|.argument("r", "a1")
        .limit(1)
        .allRelationshipsScan("(a1)-[r]->(b1)")
        .build()
    )
  }

  test("should build optional ProjectEndpoints with extra predicates") {
    planFor(
      "MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2"
    )._1 match {
      case Apply(
          Limit(DirectedAllRelationshipsScan(_, _, _, _), _),
          Optional(
            Selection(
              predicates,
              ProjectEndpoints(
                Argument(args),
                LogicalVariable("r"),
                LogicalVariable("a2"),
                false,
                LogicalVariable("b2"),
                false,
                Seq(),
                SemanticDirection.INCOMING,
                SimplePatternLength
              )
            ),
            _
          )
        ) =>
        args.map(_.name) should equal(Set("r", "a1"))
        val predicate = equals(v"a1", v"a2")
        predicates.exprs should equal(ListSet(predicate))
      case plan => throw new IllegalArgumentException(s"Unexpected plan: $plan")
    }
  }

  test("should build optional ProjectEndpoints with extra predicates 2") {
    planFor("MATCH (a1)-[r]->(b1) WITH r LIMIT 1 OPTIONAL MATCH (a2)-[r]->(b2) RETURN a2, r, b2")._1 match {
      case Apply(
          Limit(DirectedAllRelationshipsScan(_, _, _, _), _),
          Optional(
            ProjectEndpoints(
              Argument(args),
              LogicalVariable("r"),
              LogicalVariable("a2"),
              false,
              LogicalVariable("b2"),
              false,
              Seq(),
              SemanticDirection.OUTGOING,
              SimplePatternLength
            ),
            _
          )
        ) =>
        args.map(_.name) should equal(Set("r"))
      case plan => throw new IllegalArgumentException(s"Unexpected plan: $plan")
    }
  }

  test("should solve multiple optional matches") {
    val plan =
      planFor("MATCH (a) OPTIONAL MATCH (a)-[r1:R1]->(x1) OPTIONAL MATCH (a)-[r2:R2]->(x2) RETURN a, x1, x2")._1
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
    val plan = new givenConfig {
      cost = {
        case (_: Expand, _, _, _) => 1000.0
      }
    }.getLogicalPlanFor(
      """MATCH (n:X)
        |OPTIONAL MATCH (n)-[r]-(m:Y)
        |WHERE m.prop = 42
        |RETURN m""".stripMargin
    )._1.endoRewrite(unnestOptional)
    val allNodesN: LogicalPlan = NodeByLabelScan(v"n", labelName("X"), Set.empty, IndexOrderNone)

    plan should equal(
      OptionalExpand(
        allNodesN,
        v"n",
        SemanticDirection.BOTH,
        Seq.empty,
        v"m",
        v"r",
        ExpandAll,
        Some(Ands.create(ListSet(hasLabels("m", "Y"), equals(prop("m", "prop"), literalInt(42)))))
      )
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

    plan.folder.treeExists {
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

    val cfg = new givenConfig {
      cost = {
        case (_: RightOuterHashJoin, _, _, _) => 1.0
        case (_: LeftOuterHashJoin, _, _, _)  => 1.0
        case _                                => Double.MaxValue
      }
    }

    val plan = cfg.getLogicalPlanFor(query)._1
    withClue(plan) {
      containsOuterHashJoin(plan) shouldBe false
    }
  }

  test("Optional match in tail should have correct cardinality and therefore generate Argument leaf plan") {
    val config =
      plannerBuilder()
        .setAllNodesCardinality(1120169)
        .setLabelCardinality("A", 225598)
        .setLabelCardinality("B", 41)
        .setLabelCardinality("C", 101)
        .setLabelCardinality("D", 141936)
        .setRelationshipCardinality("()-[:R1]->()", 223600)
        .setRelationshipCardinality("(:A)-[:R1]->()", 223600)
        .setRelationshipCardinality("(:A)-[:R1]->(:D)", 223600)
        .setRelationshipCardinality("()-[:R1]->(:D)", 223600)
        .setRelationshipCardinality("()-[:R2]->()", 139911)
        .setRelationshipCardinality("()-[:R2]->(:B)", 139911)
        .setRelationshipCardinality("(:D)-[:R2]->()", 139911)
        .setRelationshipCardinality("(:D)-[:R2]->(:B)", 139911)
        .setRelationshipCardinality("()-[:R3]->()", 113740)
        .setRelationshipCardinality("(:B)-[:R3]->()", 1477)
        .setRelationshipCardinality("(:B)-[:R3]->(:C)", 1477)
        .setRelationshipCardinality("()-[:R3]->(:C)", 113740)
        .build()

    val query = """MATCH (a:A)
                  |WITH a
                  |LIMIT 994
                  |MATCH (:D)<-[:R1]-(a)
                  |OPTIONAL MATCH (a)-[:R1]->(:D)-[:R2]->(:B)-[:R3 {bool: false}]->(:C {some: 'prop'})
                  |RETURN count(a)""".stripMargin

    val planState = config.planState(query)

    val expected =
      config
        .planBuilder()
        .produceResults("`count(a)`")
        .aggregation(Seq(), Seq("count(a) AS `count(a)`"))
        .apply()
        .|.optional("a", "anon_1", "anon_0").withCardinality(1)
        .|.filterExpressionOrString("anon_7:C", andsReorderable("anon_6.bool = false", "anon_7.some = 'prop'"))
        .|.expandAll("(anon_5)-[anon_6:R3]->(anon_7)")
        .|.filter("anon_5:B")
        .|.expandAll("(anon_3)-[anon_4:R2]->(anon_5)")
        .|.filter("anon_3:D")
        .|.expandAll("(a)-[anon_2:R1]->(anon_3)")
        .|.argument("a", "anon_1", "anon_0").withCardinality(1)
        .filter("anon_0:D")
        .expandAll("(a)-[anon_1:R1]->(anon_0)")
        .limit(994)
        .nodeByLabelScan("a", "A", IndexOrderNone)

    planState should haveSamePlanAndCardinalitiesAsBuilder(expected, ComparingProvidedAttributesOnly)
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
         |RETURN a""".stripMargin
    )

    plan.stripProduceResults shouldBe an[OptionalExpand]
  }

  test("should not pick an outer hash join with hint if the join could end up in RHS of an apply") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(52)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("B", 2)
      .setRelationshipCardinality("()-[]-()", 30)
      .setRelationshipCardinality("(:A)-[]-()", 30)
      .setRelationshipCardinality("()-[]-(:B)", 30)
      .setRelationshipCardinality("(:A)-[]-(:B)", 30)
      .withSetting(GraphDatabaseSettings.cypher_hints_error, FALSE)
      .build()

    val tailQuery =
      s"""MATCH (a: A)
         |WITH a, 1 AS horizon
         |OPTIONAL MATCH (a)-[r]->(b:B)
         |USING JOIN ON a
         |OPTIONAL MATCH (a)--(c)
         |RETURN a.name, b.name""".stripMargin

    val tailPlan = cfg.plan(tailQuery)

    withClue(tailPlan) {
      containsOuterHashJoin(tailPlan) shouldBe false
    }
  }

  test(
    "should not pick an outer hash join with hint if any of the join nodes is from the outer apply(even join nodes that are not hinted about)."
  ) {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(52)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("B", 2)
      .setRelationshipCardinality("()-[]-()", 30)
      .setRelationshipCardinality("(:A)-[]-()", 30)
      .setRelationshipCardinality("()-[]-(:B)", 30)
      .setRelationshipCardinality("(:A)-[]-(:B)", 30)
      .withSetting(GraphDatabaseSettings.cypher_hints_error, FALSE)
      .build()

    val tailQuery =
      s"""MATCH (a: A)
         |WITH a, 1 AS horizon
         |MATCH (b: B)
         |// all nodes coming in as arguments to the optional expand will be seen as join nodes
         |// (this fails to plan a hash join because "a" is seen as a join node and a comes from the outer scope.)
         |OPTIONAL MATCH (b)--(c{foo:a.prop})
         |USING JOIN ON b
         |RETURN a.name, b.name""".stripMargin

    val tailPlan = cfg.plan(tailQuery)

    withClue(tailPlan) {
      containsOuterHashJoin(tailPlan) shouldBe false
    }
  }

  test(
    "should pick an outer hash join with hint if it is cheaper than left outer hash join and if the outer query does not contain any join node."
  ) {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(52)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("B", 2)
      .setRelationshipCardinality("()-[]-()", 30)
      .setRelationshipCardinality("(:A)-[]-()", 30)
      .setRelationshipCardinality("()-[]-(:B)", 30)
      .setRelationshipCardinality("(:A)-[]-(:B)", 30)
      .withSetting(GraphDatabaseSettings.cypher_hints_error, FALSE)
      .build()

    val noTailQuery =
      """MATCH (a:A)
        |OPTIONAL MATCH (a)-[r]->(b:B)
        |USING JOIN ON a
        |RETURN a.name, b.name""".stripMargin
    val enclosingQGDoesNotContainJoinNode =
      s"""MATCH (x:A)
         |WITH x, 1 AS foo
         |MATCH (a:A)
         |OPTIONAL MATCH (a)-[r]->(b:B)
         |USING JOIN ON a
         |RETURN a.name, b.name""".stripMargin
    val enclosingQGContainsJoinNode =
      s"""MATCH (a:A)
         |WITH a, 1 as foo
         |MATCH (x:A)
         |OPTIONAL MATCH (a)-[r]->(b:B)
         |USING JOIN ON a
         |RETURN a.name, b.name""".stripMargin

    val noTailPlan = cfg.plan(noTailQuery)
    val enclosingQGDoesNotContainJoinNodePlan = cfg.plan(enclosingQGDoesNotContainJoinNode)
    val enclosingQGContainsJoinNodePlan = cfg.plan(enclosingQGContainsJoinNode)

    withClue(noTailPlan) {
      containsOuterHashJoin(noTailPlan) shouldBe true
    }
    withClue(enclosingQGDoesNotContainJoinNodePlan) {
      containsOuterHashJoin(enclosingQGDoesNotContainJoinNodePlan) shouldBe true
    }
    withClue(enclosingQGContainsJoinNodePlan) {
      containsOuterHashJoin(enclosingQGContainsJoinNodePlan) shouldBe false
    }
  }

  test("should produce a valid plan for optional match with solved arguments passed to node by label scan") {
    val config = new givenConfig {
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def nodesAllCardinality(): Cardinality = 100000.0
      }
    }

    val query =
      """MATCH (n0), (n1)
        |OPTIONAL MATCH (n0)--(:L0), (n1)
        |RETURN n0
        |LIMIT 0""".stripMargin

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._1

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.expandInto("(n0)-[anon_0]-(anon_1)")
      .|.filterExpression(assertIsNode("n1"))
      .|.nodeByLabelScan("anon_1", "L0", IndexOrderNone, "n0", "n1")
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should equal(expected)
  }

  test("should produce a valid plan for optional match with solved arguments passed to node index scan") {
    val config = new givenConfig {
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def nodesAllCardinality(): Cardinality = 100000.0
      }
      indexOn("L0", "prop")
    }

    val query =
      """MATCH (n0), (n1)
        |OPTIONAL MATCH (n0)--(n2:L0 {prop: 42}), (n1)
        |  USING INDEX n2:L0(prop)
        |RETURN n0
        |LIMIT 0""".stripMargin

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._1

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.expandInto("(n0)-[anon_0]-(n2)")
      .|.filterExpression(assertIsNode("n1"))
      .|.nodeIndexOperator(
        "n2:L0(prop = 42)",
        indexOrder = IndexOrderNone,
        argumentIds = Set("n0", "n1"),
        indexType = IndexType.RANGE
      )
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
      .|.expandInto("(n0)-[anon_0]-(n2)")
      .|.filterExpression(propEquality("n2", "prop", 42), assertIsNode("n1"))
      .|.nodeIndexOperator(
        "n2:L0(prop)",
        indexOrder = IndexOrderNone,
        argumentIds = Set("n0", "n1"),
        indexType = IndexType.RANGE
      )
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should (equal(expected) or equal(otherwiseExpected))
  }

  /*
  Cannot get the planner to generate the plan with relationship index scan as the leaf of the right hand side with 2.13.
  Hoping we can re-enable it once we replace Set with ListSet or something similar.
  The logic inside apply optional is covered by the other similar tests in this file anyway.
   */
  ignore("should produce a valid plan for optional match with solved arguments passed to a relationship index scan") {
    val config = new givenConfig {
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

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._1

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.filter("not anon_0 = anon_2")
      .|.expandInto("(n0)-[anon_0]-(anon_1)")
      .|.filterExpression(assertIsNode("n1"))
      .|.relationshipIndexOperator(
        "(anon_1)-[anon_2:R0(prop = 42)]->(anon_3)",
        indexOrder = IndexOrderNone,
        argumentIds = Set("n0", "n1"),
        indexType = IndexType.RANGE
      )
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
      .|.filterExpression(propEquality("anon_2", "prop", 42), assertIsNode("n1"))
      .|.relationshipIndexOperator(
        "(anon_1)-[anon_2:R0(prop)]->(anon_3)",
        indexOrder = IndexOrderNone,
        argumentIds = Set("n0", "n1"),
        indexType = IndexType.RANGE
      )
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should (equal(expected) or equal(otherwiseExpected))
  }

  test(
    "should produce a valid plan for optional match with solved arguments passed to an undirected relationship type scan"
  ) {
    val config = new givenConfig {
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

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._1

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.filterExpression(equals(v"n0", v"anon_2"), assertIsNode("n1"))
      .|.relationshipTypeScan("(anon_2)-[anon_0:REL]-(anon_1)", IndexOrderNone, "n0", "n1")
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should equal(expected)
  }

  test(
    "should produce a valid plan for optional match with solved arguments passed to a directed relationship type scan"
  ) {
    val config = new givenConfig {
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

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._1

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.filterExpression(equals(v"n0", v"anon_2"), assertIsNode("n1"))
      .|.relationshipTypeScan("(anon_2)-[anon_0:REL]->(anon_1)", IndexOrderNone, "n0", "n1")
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should equal(expected)
  }

  test("should produce a valid plan for optional match with solved arguments passed to a node by id seek") {
    val config = new givenConfig {
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

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._1

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.expandInto("(n0)-[anon_0]-(x)")
      .|.filterExpression(assertIsNode("n1"))
      .|.nodeByIdSeek("x", Set("n0", "n1"), 0)
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should equal(expected)
  }

  test(
    "should produce a valid plan for optional match with solved arguments passed to an undirected relationship by id seek"
  ) {
    val config = new givenConfig {
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

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._1

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.filterExpression(equals(v"n0", v"anon_1"), assertIsNode("n1"))
      .|.undirectedRelationshipByIdSeek("r", "anon_1", "anon_0", Set("n0", "n1"), 0)
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should equal(expected)
  }

  test(
    "should produce a valid plan for optional match with solved arguments passed to a directed relationship by id seek"
  ) {
    val config = new givenConfig {
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

    val plan = config.getLogicalPlanFor(query, stripProduceResults = false)._1

    val expected = new LogicalPlanBuilder()
      .produceResults("n0")
      .limit(0)
      .apply()
      .|.optional("n0", "n1")
      .|.filterExpression(equals(v"n0", v"anon_1"), assertIsNode("n1"))
      .|.directedRelationshipByIdSeek("r", "anon_0", "anon_1", Set("n0", "n1"), 42)
      .cartesianProduct()
      .|.allNodeScan("n1")
      .allNodeScan("n0")
      .build()

    plan should equal(expected)
  }

  test("should solve an optional match followed by a regular match on the same variable, label scan in tail") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(30)
      .setLabelCardinality("L0", 10)
      .setRelationshipCardinality("()-[]->()", 20)
      .setRelationshipCardinality("(:L0)-[]->()", 20)
      .build()

    val query =
      """
        |OPTIONAL MATCH (n0)-[r1]->(n1)
        |MATCH (a:L0)-[r2]->(n1), (n0)
        |RETURN *
        |""".stripMargin

    planner.plan(query).stripProduceResults shouldEqual
      new LogicalPlanBuilder(wholePlan = false)
        .filter("a:L0")
        .expandAll("(n1)<-[r2]-(a)")
        .filterExpression(assertIsNode("n0"))
        .optional()
        .allRelationshipsScan("(n0)-[r1]->(n1)")
        .build()
  }

  test("should solve OPTIONAL MATCH containing shortestPath, followed by DISTINCT") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 200)
      .setLabelCardinality("B", 500)
      .build()

    val query =
      """OPTIONAL MATCH p=shortestPath((a:A)-[r:REL*1..10]->(b:B))
        |RETURN DISTINCT p
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val planAB = planner.subPlanBuilder()
      .distinct("p AS p")
      .optional()
      .shortestPath("(a)-[r:REL*1..10]->(b)", pathName = Some("p"))
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B")
      .nodeByLabelScan("a", "A")
      .build()

    val planBA = planner.subPlanBuilder()
      .distinct("p AS p")
      .optional()
      .shortestPath("(a)-[r:REL*1..10]->(b)", pathName = Some("p"))
      .cartesianProduct()
      .|.nodeByLabelScan("a", "A")
      .nodeByLabelScan("b", "B")
      .build()

    plan should (equal(planAB) or equal(planBA))
  }

  test("should solve OPTIONAL MATCH containing QPP, followed by DISTINCT") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(40)
      .build()

    val query = "OPTIONAL MATCH (u)((n)-[]->(m))* RETURN DISTINCT u"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .distinct("u AS u")
      .optional()
      .bfsPruningVarExpand("(u)-[anon_7*0..]->(anon_0)")
      .allNodeScan("u")
      .build()
  }

  test("should plan optional match with or-leaf-plan correctly") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setLabelCardinality("kg_Concept", 100)
      .setLabelCardinality("kg_Dimension", 100)
      .addNodeIndex("kg_Dimension", Seq("source"), 1.0, 0.1)
      .addNodeIndex("kg_Dimension", Seq("isPrivate"), 1.0, 0.1)
      .build()

    val q =
      """
        |OPTIONAL MATCH (c:kg_Concept)
        |WITH "clientOnboardingApp" as key, collect(c)[0] as resolvedConcept
        |OPTIONAL MATCH (cRelatedDimension:kg_Dimension)
        |WHERE (cRelatedDimension.source IN ["bla"] OR cRelatedDimension.isPrivate = 4)
        |WITH key, collect([1]) as cRelated2
        |RETURN key
        |""".stripMargin

    planner.plan(q) should equal(
      planner.planBuilder()
        .produceResults("key")
        .aggregation(Seq("key AS key"), Seq("collect([1]) AS cRelated2"))
        .apply()
        .|.optional("key", "resolvedConcept")
        .|.distinct("cRelatedDimension AS cRelatedDimension", "key AS key", "resolvedConcept AS resolvedConcept")
        .|.union()
        .|.|.nodeIndexOperator(
          "cRelatedDimension:kg_Dimension(isPrivate = 4)",
          indexOrder = IndexOrderNone,
          argumentIds = Set("key", "resolvedConcept")
        )
        .|.nodeIndexOperator(
          "cRelatedDimension:kg_Dimension(source = 'bla')",
          indexOrder = IndexOrderNone,
          argumentIds = Set("key", "resolvedConcept")
        )
        .projection("anon_0[0] AS resolvedConcept")
        .aggregation(Seq("'clientOnboardingApp' AS key"), Seq("collect(c) AS anon_0"))
        .optional()
        .nodeByLabelScan("c", "kg_Concept")
        .build()
    )
  }

  test("should not project ORDER BY column without dependencies under Optional") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("DoesNotExist", 0)
      .enableMinimumGraphStatistics()
      .build()

    val query =
      """
        |OPTIONAL MATCH (n:DoesNotExist)
        |WHERE n.x = 123 AND n.y STARTS WITH 'hello'
        |RETURN 123, count(*)
        |ORDER BY 123
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .sort("123 ASC")
      .aggregation(Seq("123 AS 123"), Seq("count(*) AS `count(*)`"))
      .optional()
      .filter("n.x = 123", "n.y STARTS WITH 'hello'")
      .nodeByLabelScan("n", "DoesNotExist")
      .build()
  }

  def containsOuterHashJoin(plan: LogicalPlan): Boolean = {
    plan.folder.treeExists {
      case _: RightOuterHashJoin => true
      case _: LeftOuterHashJoin  => true
    }
  }
}
