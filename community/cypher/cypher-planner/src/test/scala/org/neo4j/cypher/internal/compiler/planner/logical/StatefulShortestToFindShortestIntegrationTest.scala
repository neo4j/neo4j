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
import org.neo4j.configuration.GraphDatabaseInternalSettings.StatefulShortestPlanningMode.ALL_IF_POSSIBLE
import org.neo4j.configuration.GraphDatabaseInternalSettings.StatefulShortestPlanningMode.INTO_ONLY
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.helpers.WindowsSafeAnyRef
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.AllowSameNode
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class StatefulShortestToFindShortestIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  // We compare "solvedExpressionString" nested inside LogicalPlans.
  // This saves us from windows line break mismatches in those strings.
  implicit val windowsSafe: WindowsSafeAnyRef[LogicalPlan] = new WindowsSafeAnyRef[LogicalPlan]

  private val plannerBase = plannerBuilder()
    .setAllNodesCardinality(100)
    .setAllRelationshipsCardinality(40)
    .setLabelCardinality("User", 4)
    .addNodeIndex("User", Seq("prop"), 1.0, 0.25)
    .setRelationshipCardinality("()-[:R]->()", 10)
    .setRelationshipCardinality("()-[]->(:User)", 10)
    .setRelationshipCardinality("(:User)-[]->(:User)", 10)
    .setRelationshipCardinality("(:User)-[]->()", 10)
    .addSemanticFeature(SemanticFeature.GpmShortestPath)
    // This makes it deterministic which plans ends up on what side of a CartesianProduct.
    .setExecutionModel(Volcano)

  private val planner = plannerBase
    // For the rewrite to trigger, we need an INTO plan
    .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, INTO_ONLY)
    .build()

  private val all_if_possible_planner = plannerBase
    .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, ALL_IF_POSSIBLE)
    .build()

  test("Shortest should be rewritten to legacy shortest for simple varLength pattern") {
    val query =
      s"""
         |MATCH p = ANY SHORTEST (a:User)-[r*]->(b:User)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    val expected = planner.subPlanBuilder()
      .projection(Map("p" -> multiOutgoingRelationshipPath("a", "r", "b")))
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.nodeByLabelScan("b", "User")
      .nodeByLabelScan("a", "User")
      .build()

    plan should equal(expected)(SymmetricalLogicalPlanEquality)
  }

  test("Shortest Group should be rewritten to legacy all shortest for simple varLength pattern") {
    val query =
      s"""
         |MATCH (a), (b)
         |WITH * SKIP 1
         |MATCH p = SHORTEST GROUP (a:User)-[r*]->(b:User)
         |RETURN *
         |""".stripMargin
    val plan = all_if_possible_planner.plan(query).stripProduceResults
    val expected = all_if_possible_planner.subPlanBuilder()
      .projection(Map("p" -> multiOutgoingRelationshipPath("a", "r", "b")))
      .apply()
      .|.shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        all = true,
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .|.filterExpression(andsReorderableAst(hasLabels("a", "User"), hasLabels("b", "User")))
      .|.argument("a", "b")
      .skip(1)
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    plan should equal(expected)(SymmetricalLogicalPlanEquality)
  }

  test("Shortest should be rewritten to legacy shortest for varLength pattern with outer relationship predicate") {
    val query =
      s"""
         |MATCH ANY SHORTEST ((a)-[r*]->(b) WHERE all(x IN r WHERE x.prop > 5))
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(Predicate("x", "x.prop > 5")),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test("Shortest should be rewritten to legacy shortest for varLength pattern with inlined relationship predicate") {
    val query =
      s"""
         |MATCH ANY SHORTEST (a)-[r* {prop: 10}]->(b)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_1"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(Predicate("anon_0", "anon_0.prop = 10")),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should be rewritten to legacy shortest for varLength pattern with predicate containing an all nodes reference predicate"
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST (p = (a)-[r*]->(b) WHERE all(x IN nodes(p) WHERE x.prop > 5))
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .projection(Map("p" -> multiOutgoingRelationshipPath("a", "r", "b")))
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(Predicate("x", "x.prop > 5")),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should be rewritten to legacy shortest for varLength with predicate containing an all relationships reference predicate"
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST (p = (a)-[r*]->(b) WHERE all(x IN relationships(p) WHERE x.prop > 5))
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .projection(Map("p" -> multiOutgoingRelationshipPath("a", "r", "b")))
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(Predicate("x", "x.prop > 5")),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test("Shortest should be rewritten to legacy shortest for simple QPP") {
    val query =
      s"""
         |MATCH ANY SHORTEST (a)-[r]->*(b)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .shortestPath(
          "(a)-[r*0..]->(b)",
          pathName = Some("anon_0"),
          nodePredicates = Seq(),
          relationshipPredicates = Seq(),
          sameNodeMode = AllowSameNode
        )
        .cartesianProduct()
        .|.allNodeScan("a")
        .allNodeScan("b")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("Shortest GROUP should be rewritten to legacy all shortest for simple QPP") {
    val query =
      s"""
         |MATCH (a), (b)
         |WITH * SKIP 1
         |MATCH SHORTEST GROUP (a)-[r]->*(b)
         |RETURN *
         |""".stripMargin
    val plan = all_if_possible_planner.plan(query).stripProduceResults
    plan should equal(all_if_possible_planner.subPlanBuilder()
      .apply()
      .|.shortestPath(
        "(a)-[r*0..]->(b)",
        pathName = Some("anon_0"),
        all = true,
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .|.argument("a", "b")
      .skip(1)
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should be rewritten to legacy shortest for QPP with outer relationship predicate and inlined relationship Type"
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST ((a)-[r:R]->*(b) WHERE all(x IN r WHERE x.prop > 5))
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .shortestPath(
        "(a)-[r:R*0..]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(Predicate("x", "x.prop > 5")),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should be rewritten to legacy shortest for QPP Kleene star if all quantified nodes share the same predicates and one of the boundary nodes also covers those predicates"
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST (a{prop: 1}) (({prop: 1})-[r]->({prop: 1})) *(b)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .shortestPath(
        "(a)-[r*0..]->(b)",
        pathName = Some("anon_1"),
        nodePredicates = Seq(Predicate("anon_0", "anon_0.prop = 1")),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("b")
      .filter("a.prop = 1")
      .allNodeScan("a")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should NOT be rewritten to legacy shortest for QPP Kleene star if none of the juxtaposed nodes does not cover the quantified nodes predicates"
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST (a) (({prop: 1})-[r]->({prop: 1})) *(b)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    val nfa = new TestNFABuilder(0, "a")
      .addTransition(0, 1, "(a) (anon_0 WHERE anon_0.prop = 1)")
      .addTransition(0, 3, "(a) (b)")
      .addTransition(1, 2, "(anon_0)-[r]->(anon_1 WHERE anon_1.prop = 1)")
      .addTransition(2, 1, "(anon_1) (anon_0 WHERE anon_0.prop = 1)")
      .addTransition(2, 3, "(anon_1) (b)")
      .setFinalState(3)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 (a) ((`anon_0`)-[`r`]->(`anon_1`)){0, } (b)",
          None,
          Set(),
          Set(("r", "r")),
          Set(),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandInto
        )
        .cartesianProduct()
        .|.allNodeScan("a")
        .allNodeScan("b")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("Shortest should be rewritten to legacy shortest for simple varLength pattern with set upper bound") {
    val query =
      s"""
         |MATCH (a:User), (b:User)
         |WITH * SKIP 0
         |MATCH ANY SHORTEST (a)-[*0..1]-(b)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.shortestPath(
          "(a)-[anon_0*0..1]-(b)",
          pathName = Some("anon_1"),
          nodePredicates = Seq(),
          relationshipPredicates = Seq(),
          sameNodeMode = AllowSameNode
        )
        .|.argument("a", "b")
        .skip(0)
        .cartesianProduct()
        .|.nodeByLabelScan("b", "User")
        .nodeByLabelScan("a", "User")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should NOT be rewritten to legacy shortest for QPP with different predicates on the quantified nodes."
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST (a) (({prop: 1})-[r]->({prop: 2})) *(b)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    val nfa = new TestNFABuilder(0, "a")
      .addTransition(0, 1, "(a) (anon_0 WHERE anon_0.prop = 1)")
      .addTransition(0, 3, "(a) (b)")
      .addTransition(1, 2, "(anon_0)-[r]->(anon_1 WHERE anon_1.prop = 2)")
      .addTransition(2, 1, "(anon_1) (anon_0 WHERE anon_0.prop = 1)")
      .addTransition(2, 3, "(anon_1) (b)")
      .setFinalState(3)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 (a) ((`anon_0`)-[`r`]->(`anon_1`)){0, } (b)",
          None,
          Set(),
          Set(("r", "r")),
          Set(),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandInto
        )
        .cartesianProduct()
        .|.allNodeScan("a")
        .allNodeScan("b")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("Shortest with multiple QPPs should not be rewritten to legacy shortest") {
    val query =
      s"""
         |MATCH (a:User), (b:User), (x)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST (a)-->+(x)-->+(b)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    val nfa = new TestNFABuilder(0, "a")
      .addTransition(0, 1, "(a) (anon_0)")
      .addTransition(1, 2, "(anon_0)-[anon_1]->(anon_2)")
      .addTransition(2, 1, "(anon_2) (anon_0)")
      .addTransition(2, 3, "(anon_2) (x WHERE x = x)")
      .addTransition(3, 4, "(x) (anon_3)")
      .addTransition(4, 5, "(anon_3)-[anon_4]->(anon_5)")
      .addTransition(5, 4, "(anon_5) (anon_3)")
      .addTransition(5, 6, "(anon_5) (b)")
      .setFinalState(6)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 (a) ((`anon_0`)-[`anon_1`]->(`anon_2`)){1, } (`x`) ((`anon_3`)-[`anon_4`]->(`anon_5`)){1, } (b)",
          None,
          Set(),
          Set(),
          Set(("x", "x")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandInto
        )
        .skip(1)
        .cartesianProduct()
        .|.cartesianProduct()
        .|.|.allNodeScan("x")
        .|.nodeByLabelScan("b", "User")
        .nodeByLabelScan("a", "User")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("Shortest should be rewritten to legacy shortest for QPP with post filter referencing relationship") {
    val query =
      s"""
         |MATCH ANY SHORTEST (a:User)-[r]->+(b:User)
         |WHERE none(rel IN r WHERE rel.prop = 1)
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    val expected = planner.subPlanBuilder()
      .filter("none(rel IN r WHERE rel.prop = 1)")
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.nodeByLabelScan("a", "User")
      .nodeByLabelScan("b", "User")
      .build()
    plan should equal(expected)(SymmetricalLogicalPlanEquality)
  }

  test(
    "Shortest should NOT be rewritten to legacy shortest for QPP with inverted pre-filter referencing relationship"
  ) {
    val query =
      s"""
         |MATCH ANY SHORTEST ((:User)-[r]->+(:User) WHERE none(rel IN r WHERE rel.prop = 5))
         |RETURN *
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    val nfa = new TestNFABuilder(0, "anon_0")
      .addTransition(0, 1, "(anon_0) (anon_2)")
      .addTransition(1, 2, "(anon_2)-[r]->(anon_3)")
      .addTransition(2, 1, "(anon_3) (anon_2)")
      .addTransition(2, 3, "(anon_3) (anon_1)")
      .setFinalState(3)
      .build()
    val expected = planner.subPlanBuilder()
      .statefulShortestPath(
        "anon_0",
        "anon_1",
        "SHORTEST 1 (`anon_0`) ((`anon_2`)-[`r`]->(`anon_3`)){1, } (`anon_1`)",
        Some("none(rel IN r WHERE rel.prop = 5)"),
        Set(),
        Set(("r", "r")),
        Set(),
        Set(),
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandInto
      )
      .cartesianProduct()
      .|.nodeByLabelScan("anon_1", "User")
      .nodeByLabelScan("anon_0", "User")
      .build()

    plan should equal(expected)(SymmetricalLogicalPlanEquality)
  }

  test("SHORTEST should be rewritten since inner nodes are not referenced") {
    val query =
      s"""
         |MATCH ANY SHORTEST (a) ((c)-[r]->(d)) +(b)
         |RETURN r
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .shortestPath(
        "(a)-[r*]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build())(SymmetricalLogicalPlanEquality)
  }

  test("SHORTEST should be rewritten even if path is referenced in return") {
    val query =
      s"""
         |MATCH p = ANY SHORTEST (a) ((c)-[r]->(d)) +(b)
         |RETURN p
         |""".stripMargin
    val plan = planner.plan(query)
    val pathExpression = PathExpression(NodePathStep(
      v"a",
      MultiRelationshipPathStep(varFor("r"), OUTGOING, Some(varFor("b")), NilPathStep()(pos))(pos)
    )(pos))(pos)
    val expected = planner.planBuilder()
      .produceResults("p")
      .projection(Map("p" -> pathExpression))
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build()

    plan should equal(expected)(SymmetricalLogicalPlanEquality)
  }

  test("SHORTEST should be rewritten even if path is referenced in return, incoming rel") {
    val query =
      s"""
         |MATCH p = ANY SHORTEST (a) ((c)<-[r]-(d)) +(b)
         |RETURN p
         |""".stripMargin
    val plan = planner.plan(query)
    val pathExpression = PathExpression(NodePathStep(
      v"a",
      MultiRelationshipPathStep(varFor("r"), INCOMING, Some(varFor("b")), NilPathStep()(pos))(pos)
    )(pos))(pos)
    val expected = planner.planBuilder()
      .produceResults("p")
      .projection(Map("p" -> pathExpression))
      .shortestPath(
        "(a)<-[r*1..]-(b)",
        pathName = Some("anon_0"),
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build()

    plan should equal(expected)(SymmetricalLogicalPlanEquality)
  }

  test("SHORTEST should be rewritten since inner nodes are only referenced by inner QPP predicates") {
    val query =
      s"""
         |MATCH ANY SHORTEST (a) ((c)-[r]->(d) WHERE c.prop = 1 AND d.prop = 1) +(b)
         |RETURN r
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan should equal(planner.subPlanBuilder()
      .shortestPath(
        "(a)-[r*1..]->(b)",
        pathName = Some("anon_1"),
        nodePredicates = Seq(Predicate("anon_0", "anon_0.prop = 1")),
        relationshipPredicates = Seq(),
        sameNodeMode = AllowSameNode
      )
      .cartesianProduct()
      .|.filter("b.prop = 1")
      .|.allNodeScan("b")
      .filter("a.prop = 1")
      .allNodeScan("a")
      .build())(SymmetricalLogicalPlanEquality)
  }

  // TODO: This currently does not get rewritten due to the quantified nodes being named. And solveds contains the original predicate a.prop <> b.prop and not
  //  all(r in relationships(p) WHERE startNode(r).prop <> endNode(r).prop), the later being inlineable in a legacy shortest with no reference to quantified nodes.
  ignore(
    "Shortest QPP with no references to the quantified nodes outside of the Shortest should be rewritten if able"
  ) {
    val query =
      s"""
         |MATCH (s), (t)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST (s)((a)-[r:User]->(b) WHERE a.prop <> b.prop)+(t)
         |RETURN r
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (a)")
      .addTransition(1, 2, "(a)-[anon_0:User WHERE NOT startNode(anon_0).prop = endNode(anon_0).prop]->(b)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (t)")
      .setFinalState(3)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "s",
          "t",
          "SHORTEST 1 ((s) ((a)-[anon_0:User]->(b) WHERE NOT `a`.prop = `b`.prop){1, } (t) WHERE unique(`anon_5`))",
          None,
          Set(("a", "a"), ("b", "b")),
          Set(),
          Set(),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandInto
        )
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("t")
        .allNodeScan("s")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  def multiOutgoingRelationshipPath(fromNode: String, relationships: String, toNode: String): PathExpression = {
    PathExpression(
      NodePathStep(
        node = varFor(fromNode),
        MultiRelationshipPathStep(
          rel = varFor(relationships),
          direction = OUTGOING,
          toNode = Some(varFor(toNode)),
          next = NilPathStep()(pos)
        )(pos)
      )(pos)
    )(InputPosition.NONE)
  }
}
