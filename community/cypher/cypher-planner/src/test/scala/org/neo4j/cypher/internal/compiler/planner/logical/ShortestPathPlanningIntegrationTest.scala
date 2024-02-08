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
import org.neo4j.configuration.GraphDatabaseInternalSettings.StatefulShortestPlanningMode.CARDINALITY_HEURISTIC
import org.neo4j.configuration.GraphDatabaseInternalSettings.StatefulShortestPlanningMode.INTO_ONLY
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.helpers.WindowsSafeAnyRef
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.NodeRelPair
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.RepeatPathStep
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.expressions.functions.EndNode
import org.neo4j.cypher.internal.expressions.functions.StartNode
import org.neo4j.cypher.internal.ir.EagernessReason.Conflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadCreateConflict
import org.neo4j.cypher.internal.ir.EagernessReason.TypeReadSetConflict
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NFA.NodeJuxtapositionPredicate
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionPredicate
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanGetByNameExpression
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import java.lang.Boolean.FALSE

class ShortestPathPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  // We compare "solvedExpressionString" nested inside LogicalPlans.
  // This saves us from windows line break mismatches in those strings.
  implicit val windowsSafe: WindowsSafeAnyRef[LogicalPlan] = new WindowsSafeAnyRef[LogicalPlan]

  private val plannerBase = plannerBuilder()
    .setAllNodesCardinality(100)
    .setAllRelationshipsCardinality(40)
    .setLabelCardinality("User", 4)
    .setLabelCardinality("N", 6)
    .setLabelCardinality("NN", 5)
    .setLabelCardinality("NN", 5)
    .setLabelCardinality("B", 8)
    .addNodeIndex("User", Seq("prop"), 1.0, 0.25, withValues = true)
    .setRelationshipCardinality("()-[:R]->()", 10)
    .setRelationshipCardinality("(:User)-[:R]->()", 10)
    .setRelationshipCardinality("(:User)-[:R]->(:B)", 10)
    .setRelationshipCardinality("(:B)-[:R]->(:B)", 10)
    .setRelationshipCardinality("(:B)-[:R]->(:User)", 10)
    .setRelationshipCardinality("(:B)-[:R]->()", 10)
    .setRelationshipCardinality("(:B)-[]->()", 10)
    .setRelationshipCardinality("(:User)-[]->(:B)", 10)
    .setRelationshipCardinality("()-[]->(:B)", 10)
    .setRelationshipCardinality("()-[:R]->(:B)", 10)
    .setRelationshipCardinality("()-[]->(:N)", 10)
    .setRelationshipCardinality("()-[]->(:NN)", 10)
    .setRelationshipCardinality("()-[]->(:User)", 10)
    .setRelationshipCardinality("(:User)-[]->(:User)", 10)
    .setRelationshipCardinality("(:User)-[]->()", 10)
    .setRelationshipCardinality("(:User)-[]->(:NN)", 10)
    .setRelationshipCardinality("(:B)-[]->(:N)", 10)
    .setRelationshipCardinality("()-[:T]->()", 10)
    .setRelationshipCardinality("(:N)-[]->()", 10)
    .addSemanticFeature(SemanticFeature.GpmShortestPath)
    // This makes it deterministic which plans ends up on what side of a CartesianProduct.
    .setExecutionModel(Volcano)

  private val planner = plannerBase.build()

  private val all_if_possible_planner = plannerBase
    .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, ALL_IF_POSSIBLE)
    .build()

  private val shortest_without_legacy = plannerBase
    .withSetting(GraphDatabaseInternalSettings.gpm_shortest_to_legacy_shortest_enabled, FALSE)
    .build()

  private val nonDeduplicatingPlanner =
    plannerBase
      .enableDeduplicateNames(false)
      .build()

  test("should plan SHORTEST with 1 QPP, + quantifier, no predicates, left-to-right") {
    val query = "MATCH ANY SHORTEST (u:User)((n)-[r]->(m))+(v) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .setFinalState(3)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v) WHERE unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set(("v", "v")),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("should plan SHORTEST with QPP with several relationships and path assignment") {
    val query =
      """MATCH p = SHORTEST 1 (s:User) (()-[:R]->()-[:T]-()-[:T]-()-[:T]-()-[:R]->())+ (t)
        |RETURN p""".stripMargin
    val plan = planner.plan(query)

    val pathExpression = PathExpression(NodePathStep(
      v"s",
      RepeatPathStep(
        List(
          NodeRelPair(v"anon_13", v"anon_20"),
          NodeRelPair(v"anon_14", v"anon_12"),
          NodeRelPair(v"anon_18", v"anon_15"),
          NodeRelPair(v"anon_11", v"anon_16"),
          NodeRelPair(v"anon_17", v"anon_19")
        ),
        v"t",
        NilPathStep()(pos)
      )(pos)
    )(pos))(pos)

    plan should equal(
      planner.planBuilder()
        .produceResults("p")
        .projection(Map("p" -> pathExpression))
        .statefulShortestPath(
          "s",
          "t",
          "SHORTEST 1 ((s) ((`anon_0`)-[`anon_1`:R]->(`anon_2`)-[`anon_3`:T]-(`anon_4`)-[`anon_5`:T]-(`anon_6`)-[`anon_7`:T]-(`anon_8`)-[`anon_9`:R]->(`anon_10`)){1, } (t) WHERE NOT `anon_5` = `anon_3` AND NOT `anon_7` = `anon_3` AND NOT `anon_7` = `anon_5` AND NOT `anon_9` = `anon_1` AND unique((((`anon_20` + `anon_12`) + `anon_15`) + `anon_16`) + `anon_19`))",
          None,
          groupNodes = Set(
            ("anon_0", "anon_13"),
            ("anon_6", "anon_11"),
            ("anon_4", "anon_18"),
            ("anon_8", "anon_17"),
            ("anon_2", "anon_14")
          ),
          groupRelationships = Set(
            ("anon_7", "anon_16"),
            ("anon_3", "anon_12"),
            ("anon_5", "anon_15"),
            ("anon_9", "anon_19"),
            ("anon_1", "anon_20")
          ),
          singletonNodeVariables = Set(("t", "t")),
          singletonRelationshipVariables = Set(),
          selector = StatefulShortestPath.Selector.Shortest(1),
          nfa = new TestNFABuilder(0, "s")
            .addTransition(0, 1, "(s) (anon_0)")
            .addTransition(1, 2, "(anon_0)-[anon_1:R]->(anon_2)")
            .addTransition(2, 3, "(anon_2)-[anon_3:T]-(anon_4)")
            .addTransition(3, 4, "(anon_4)-[anon_5:T]-(anon_6)")
            .addTransition(4, 5, "(anon_6)-[anon_7:T]-(anon_8)")
            .addTransition(5, 6, "(anon_8)-[anon_9:R]->(anon_10)")
            .addTransition(6, 1, "(anon_10) (anon_0)")
            .addTransition(6, 7, "(anon_10) (t)")
            .setFinalState(7)
            .build(),
          ExpandAll
        )
        .nodeByLabelScan("s", "User")
        .build()
    )
  }

  test("should plan SHORTEST with var-length relationship") {
    val query = "MATCH ANY SHORTEST (u:User)-[r:R*]->(v) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (anon_0)")
      .addTransition(1, 2, "(anon_0)-[r:R]->(anon_1)")
      .addTransition(2, 2, "(anon_1)-[r:R]->(anon_1)")
      .addTransition(2, 3, "(anon_1) (v)")
      .setFinalState(3)
      .build()

    val plan = shortest_without_legacy.plan(query).stripProduceResults
    plan should equal(
      shortest_without_legacy.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u)-[r:R*]->(v) WHERE size(r) >= 1 AND unique(r))",
          None,
          groupNodes = Set(),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set(("v", "v")),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("should plan SHORTEST with previously bound var-length relationship") {
    val query =
      """
        |MATCH (from:User)-[next:R*1..5]->(to:B)
        |MATCH ANY SHORTEST (u:User)-[next:R*1..5]->(b)
        |RETURN *""".stripMargin

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (`  UNNAMED0`)")
      .addTransition(1, 2, "(`  UNNAMED0`)-[`  next@2`:R]->(`  UNNAMED1`)")
      .addTransition(2, 3, "(`  UNNAMED1`)-[`  next@2`:R]->(`  UNNAMED2`)")
      .addTransition(2, 7, "(`  UNNAMED1`) (`  b@1`)")
      .addTransition(3, 4, "(`  UNNAMED2`)-[`  next@2`:R]->(`  UNNAMED3`)")
      .addTransition(3, 7, "(`  UNNAMED2`) (`  b@1`)")
      .addTransition(4, 5, "(`  UNNAMED3`)-[`  next@2`:R]->(`  UNNAMED4`)")
      .addTransition(4, 7, "(`  UNNAMED3`) (`  b@1`)")
      .addTransition(5, 6, "(`  UNNAMED4`)-[`  next@2`:R]->(`  UNNAMED5`)")
      .addTransition(5, 7, "(`  UNNAMED4`) (`  b@1`)")
      .addTransition(6, 7, "(`  UNNAMED5`) (`  b@1`)")
      .setFinalState(7)
      .build()

    val plan = nonDeduplicatingPlanner.plan(query).stripProduceResults
    plan should equal(
      nonDeduplicatingPlanner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "b",
          "SHORTEST 1 ((u)-[`  next@0`:R*1..5]->(b) WHERE `  next@0` = next)",
          Some("`  next@0` = next"),
          Set(),
          Set(("  next@2", "  next@0")),
          Set(("  b@1", "b")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        // Yup, not pretty, we could drop these predicates.
        // But currently they are inserted quite early in ASTRewriter,
        // and only later the variable `next` gets renamed to `  next@2` in the 2nd match clause.
        // In general, it is beneficial to NOT rewrite references to `next` to use `  next@2` in predicates,
        // because that allows the predicates to be solved earlier.
        // These implicitly solved relationship predicates are an exception.
        .filter(
          "size(next) >= 1",
          "size(next) <= 5",
          "all(`  UNNAMED6` IN next WHERE single(`  UNNAMED7` IN next WHERE `  UNNAMED6` = `  UNNAMED7`))"
        )
        .apply()
        .|.nodeByLabelScan("u", "User", "from", "to", "next")
        .filter("to:B")
        .expand("(from)-[next:R*1..5]->(to)")
        .nodeByLabelScan("from", "User")
        .build()
    )
  }

  test("should plan SHORTEST with var-length relationship and predicates") {
    val query =
      "MATCH ANY SHORTEST (u:User)-[r:R* {prop: 42}]->(v {prop: 3})-[s]->(w {prop: 4})-[t:R|T*1..2]->(x) RETURN *"

    val nfa =
      new TestNFABuilder(0, "u")
        .addTransition(0, 1, "(u) (anon_1)")
        .addTransition(1, 2, "(anon_1)-[r:R]->(anon_2)")
        .addTransition(2, 2, "(anon_2)-[r:R]->(anon_2)")
        .addTransition(2, 3, "(anon_2) (v WHERE v.prop = 3)")
        .addTransition(3, 4, "(v)-[s]->(w WHERE w.prop = 4)")
        .addTransition(4, 5, "(w) (anon_3)")
        .addTransition(5, 6, "(anon_3)-[t:R|T]->(anon_4)")
        .addTransition(6, 7, "(anon_4)-[t:R|T]->(anon_5)")
        .addTransition(6, 8, "(anon_4) (x)")
        .addTransition(7, 8, "(anon_5) (x)")
        .setFinalState(8)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "x",
          "SHORTEST 1 ((u)-[r:R*]->(v)-[s]->(w)-[t:R|T*1..2]->(x) WHERE" +
            " NOT s IN r AND NOT s IN t AND all(`anon_0` IN r WHERE `anon_0`.prop IN [42])" +
            " AND disjoint(t, r) AND size(r) >= 1 AND size(t) <= 2 AND size(t) >= 1 AND unique(r) AND unique(t)" +
            " AND v.prop IN [3] AND w.prop IN [4])",
          Some("all(anon_0 IN r WHERE anon_0.prop = 42)"),
          groupNodes = Set(),
          groupRelationships = Set(("r", "r"), ("t", "t")),
          singletonNodeVariables = Set("v" -> "v", "w" -> "w", "x" -> "x"),
          singletonRelationshipVariables = Set("s" -> "s"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("should allow planning of shortest with already bound interior node") {
    val query =
      "MATCH (d:User) MATCH ANY SHORTEST (a:User) ((b)-[r]->(c))* (d) ((e)-[s]->(f))* (g) RETURN *"

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (b)")
        .addTransition(0, 3, "(a) (d WHERE d = d)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (d WHERE d = d)")
        .addTransition(3, 4, "(d) (e)")
        .addTransition(3, 6, "(d) (g)")
        .addTransition(4, 5, "(e)-[s]->(f)")
        .addTransition(5, 4, "(f) (e)")
        .addTransition(5, 6, "(f) (g)")
        .setFinalState(6)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "g",
          "SHORTEST 1 ((a) ((`b`)-[`r`]->(`c`)){0, } (`d`) ((`e`)-[`s`]->(`f`)){0, } (g) WHERE `d` = d AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
          None,
          groupNodes = Set(("b", "b"), ("c", "c"), ("e", "e"), ("f", "f")),
          groupRelationships = Set(("r", "r"), ("s", "s")),
          singletonNodeVariables = Set("d" -> "d", "g" -> "g"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .apply()
        .|.nodeByLabelScan("a", "User", "d")
        .nodeByLabelScan("d", "User")
        .build()
    )
  }

  test("should allow planning of shortest with already bound end nodes") {
    val query =
      "MATCH (d:B), (a:User) WITH * SKIP 0 MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (d) RETURN *"

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (b)")
        .addTransition(0, 3, "(a) (d)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (d)")
        .setFinalState(3)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "d",
          "SHORTEST 1 ((a) ((`b`)-[`r`]->(`c`)){0, } (d) WHERE unique(`r`))",
          None,
          Set(("b", "b"), ("c", "c")),
          Set(("r", "r")),
          Set(),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandInto
        )
        .skip(0)
        .cartesianProduct()
        .|.nodeByLabelScan("d", "B")
        .nodeByLabelScan("a", "User")
        .build()
    )
  }

  test("should allow planning of shortest with already bound interior node with predicate on interior node") {
    val query =
      "MATCH (d:User) MATCH ANY SHORTEST (a:N) ((b)-[r]->(c))* (d {prop: 5}) ((e)-[s]->(f))* (g) RETURN *"

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (b)")
        .addTransition(0, 3, "(a) (d WHERE d = d)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (d WHERE d = d)")
        .addTransition(3, 4, "(d) (e)")
        .addTransition(3, 6, "(d) (g)")
        .addTransition(4, 5, "(e)-[s]->(f)")
        .addTransition(5, 4, "(f) (e)")
        .addTransition(5, 6, "(f) (g)")
        .setFinalState(6)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "g",
          "SHORTEST 1 ((a) ((`b`)-[`r`]->(`c`)){0, } (`d`) ((`e`)-[`s`]->(`f`)){0, } (g) WHERE `d` = d AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
          None,
          Set(("b", "b"), ("c", "c"), ("e", "e"), ("f", "f")),
          Set(("r", "r"), ("s", "s")),
          Set("d" -> "d", "g" -> "g"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .filter("cacheN[d.prop] = 5")
        .apply()
        .|.nodeByLabelScan("a", "N", "d")
        .cacheProperties("cacheNFromStore[d.prop]")
        .nodeByLabelScan("d", "User")
        .build()
    )
  }

  test("should allow planning of shortest with repeated interior node") {
    val query =
      "MATCH ANY SHORTEST (a:User) ((b)-[r]->(c))* (d WHERE d.prop = 5) ((e)-[s]->(f))* (d) RETURN *"

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (`  b@1`)")
        .addTransition(0, 3, "(a) (`  d@13`)")
        .addTransition(1, 2, "(`  b@1`)-[`  r@2`]->(`  c@3`)")
        .addTransition(2, 1, "(`  c@3`) (`  b@1`)")
        .addTransition(2, 3, "(`  c@3`) (`  d@13`)")
        .addTransition(3, 4, "(`  d@13`) (`  e@7`)")
        .addTransition(3, 6, "(`  d@13`) (`  d@14` WHERE `  d@14`.prop = 5)")
        .addTransition(4, 5, "(`  e@7`)-[`  s@8`]->(`  f@9`)")
        .addTransition(5, 4, "(`  f@9`) (`  e@7`)")
        .addTransition(5, 6, "(`  f@9`) (`  d@14` WHERE `  d@14`.prop = 5)")
        .setFinalState(6)
        .build()

    val plan = nonDeduplicatingPlanner.plan(query).stripProduceResults
    plan should equal(
      nonDeduplicatingPlanner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "d",
          "SHORTEST 1 ((a) ((`  b@1`)-[`  r@2`]->(`  c@3`)){0, } (`  d@0`) ((`  e@7`)-[`  s@8`]->(`  f@9`)){0, } (d) WHERE `  d@0` = d AND d.prop IN [5] AND disjoint(`  r@5`, `  s@11`) AND unique(`  r@5`) AND unique(`  s@11`))",
          Some("`  d@0` = d"),
          Set(("  b@1", "  b@4"), ("  c@3", "  c@6"), ("  e@7", "  e@10"), ("  f@9", "  f@12")),
          Set(("  r@2", "  r@5"), ("  s@8", "  s@11")),
          Set("  d@13" -> "  d@0", ("  d@14", "d")),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("a", "User")
        .build()
    )
  }

  test("should allow planning of shortest with repeated interior node - reverse") {
    val planner = plannerBase
      .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, ALL_IF_POSSIBLE)
      .enableDeduplicateNames(false)
      .build()

    val query =
      "MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (d WHERE d.prop = 5) ((e)-[s]->(f))* (d:User) RETURN *"

    val nfa =
      new TestNFABuilder(0, "d")
        .addTransition(0, 1, "(d) (`  f@9`)")
        .addTransition(0, 3, "(d) (`  d@14` WHERE `  d@14` = d)")
        .addTransition(1, 2, "(`  f@9`)<-[`  s@8`]-(`  e@7`)")
        .addTransition(2, 1, "(`  e@7`) (`  f@9`)")
        .addTransition(2, 3, "(`  e@7`) (`  d@14` WHERE `  d@14` = d)")
        .addTransition(3, 4, "(`  d@14`) (`  c@3`)")
        .addTransition(3, 6, "(`  d@14`) (`  a@13`)")
        .addTransition(4, 5, "(`  c@3`)<-[`  r@2`]-(`  b@1`)")
        .addTransition(5, 4, "(`  b@1`) (`  c@3`)")
        .addTransition(5, 6, "(`  b@1`) (`  a@13`)")
        .setFinalState(6)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "d",
          "a",
          "SHORTEST 1 ((a) ((`  b@1`)-[`  r@2`]->(`  c@3`)){0, } (`  d@0`) ((`  e@7`)-[`  s@8`]->(`  f@9`)){0, } (d) WHERE `  d@0` = d AND disjoint(`  r@5`, `  s@11`) AND unique(`  r@5`) AND unique(`  s@11`))",
          None,
          Set(("  b@1", "  b@4"), ("  c@3", "  c@6"), ("  e@7", "  e@10"), ("  f@9", "  f@12")),
          Set(("  r@2", "  r@5"), ("  s@8", "  s@11")),
          Set(("  a@13", "a"), ("  d@14", "  d@0")),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = true
        )
        .nodeIndexOperator("d:User(prop = 5)")
        .build()
    )
  }

  test("should plan shortest with bound interior node with predicate - reversed") {
    val planner = plannerBase
      .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, ALL_IF_POSSIBLE)
      .enableDeduplicateNames(false)
      .build()

    val query =
      "MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (a WHERE a.prop = 5) ((e)-[s]->(f))* (d:User) RETURN *"

    val nfa =
      new TestNFABuilder(0, "d")
        .addTransition(0, 1, "(d) (`  f@9`)")
        .addTransition(0, 3, "(d) (`  a@14`)") // it would be more efficient to add "WHERE `  a@14`.prop = 5"
        .addTransition(1, 2, "(`  f@9`)<-[`  s@8`]-(`  e@7`)")
        .addTransition(2, 1, "(`  e@7`) (`  f@9`)")
        .addTransition(2, 3, "(`  e@7`) (`  a@14`)") // it would be more efficient to add "WHERE `  a@14`.prop = 5"
        .addTransition(3, 4, "(`  a@14`) (`  c@3`)")
        .addTransition(3, 6, "(`  a@14`) (`  a@13` WHERE `  a@13`.prop = 5)")
        .addTransition(4, 5, "(`  c@3`)<-[`  r@2`]-(`  b@1`)")
        .addTransition(5, 4, "(`  b@1`) (`  c@3`)")
        .addTransition(5, 6, "(`  b@1`) (`  a@13` WHERE `  a@13`.prop = 5)")
        .setFinalState(6)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "d",
          "a",
          "SHORTEST 1 ((a) ((`  b@1`)-[`  r@2`]->(`  c@3`)){0, } (`  a@0`) ((`  e@7`)-[`  s@8`]->(`  f@9`)){0, } (d) WHERE `  a@0` = a AND a.prop IN [5] AND disjoint(`  r@5`, `  s@11`) AND unique(`  r@5`) AND unique(`  s@11`))",
          Some("`  a@0` = a"),
          Set(("  b@1", "  b@4"), ("  c@3", "  c@6"), ("  e@7", "  e@10"), ("  f@9", "  f@12")),
          Set(("  r@2", "  r@5"), ("  s@8", "  s@11")),
          Set("  a@14" -> "  a@0", "  a@13" -> "a"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = true
        )
        .nodeByLabelScan("d", "User")
        .build()
    )
  }

  test(
    "should allow planning of shortest with already bound interior start and end node with predicate on interior node"
  ) {
    val query =
      """MATCH (a:N), (d)
        |  // This makes it deterministic which plans ends up on what side of a CartesianProduct.
        |  WHERE a.prop = 5
        |WITH * SKIP 0
        |MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (d WHERE d.prop = 5) ((e)-[s]->(f))* (d)
        |RETURN *""".stripMargin

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (`  b@1`)")
        .addTransition(0, 3, "(a) (`  d@13` WHERE `  d@13` = d)")
        .addTransition(1, 2, "(`  b@1`)-[`  r@2`]->(`  c@3`)")
        .addTransition(2, 1, "(`  c@3`) (`  b@1`)")
        .addTransition(2, 3, "(`  c@3`) (`  d@13` WHERE `  d@13` = d)")
        .addTransition(3, 4, "(`  d@13`) (`  e@7`)")
        .addTransition(3, 6, "(`  d@13`) (d)")
        .addTransition(4, 5, "(`  e@7`)-[`  s@8`]->(`  f@9`)")
        .addTransition(5, 4, "(`  f@9`) (`  e@7`)")
        .addTransition(5, 6, "(`  f@9`) (d)")
        .setFinalState(6)
        .build()

    val plan = nonDeduplicatingPlanner.plan(query).stripProduceResults
    plan should equal(
      nonDeduplicatingPlanner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "d",
          "SHORTEST 1 ((a) ((`  b@1`)-[`  r@2`]->(`  c@3`)){0, } (`  d@0`) ((`  e@7`)-[`  s@8`]->(`  f@9`)){0, } (d) WHERE `  d@0` = d AND disjoint(`  r@5`, `  s@11`) AND unique(`  r@5`) AND unique(`  s@11`))",
          None,
          Set(("  b@1", "  b@4"), ("  c@3", "  c@6"), ("  e@7", "  e@10"), ("  f@9", "  f@12")),
          Set(("  r@2", "  r@5"), ("  s@8", "  s@11")),
          Set(("  d@13", "  d@0")),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandInto
        )
        .filter("d.prop = 5")
        .skip(0)
        .cartesianProduct()
        .|.allNodeScan("d")
        .filter("a.prop = 5")
        .nodeByLabelScan("a", "N")
        .build()
    )
  }

  test(
    "should allow planning of shortest with already bound strict interior node with predicate"
  ) {
    val query =
      "MATCH (d) WITH * SKIP 0 MATCH ANY SHORTEST (a:User) ((b)-[r]->(c))* (d WHERE d.prop = 5) ((e)-[s]->(f))* (g) RETURN *"

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (`  b@1`)")
        .addTransition(0, 3, "(a) (`  d@13` WHERE `  d@13` = d)")
        .addTransition(1, 2, "(`  b@1`)-[`  r@2`]->(`  c@3`)")
        .addTransition(2, 1, "(`  c@3`) (`  b@1`)")
        .addTransition(2, 3, "(`  c@3`) (`  d@13` WHERE `  d@13` = d)")
        .addTransition(3, 4, "(`  d@13`) (`  e@7`)")
        .addTransition(3, 6, "(`  d@13`) (`  g@14`)")
        .addTransition(4, 5, "(`  e@7`)-[`  s@8`]->(`  f@9`)")
        .addTransition(5, 4, "(`  f@9`) (`  e@7`)")
        .addTransition(5, 6, "(`  f@9`) (`  g@14`)")
        .setFinalState(6)
        .build()

    val plan = nonDeduplicatingPlanner.plan(query).stripProduceResults
    plan should equal(
      nonDeduplicatingPlanner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "g",
          "SHORTEST 1 ((a) ((`  b@1`)-[`  r@2`]->(`  c@3`)){0, } (`  d@0`) ((`  e@7`)-[`  s@8`]->(`  f@9`)){0, } (g) WHERE `  d@0` = d AND disjoint(`  r@5`, `  s@11`) AND unique(`  r@5`) AND unique(`  s@11`))",
          None,
          Set(("  b@1", "  b@4"), ("  c@3", "  c@6"), ("  e@7", "  e@10"), ("  f@9", "  f@12")),
          Set(("  r@2", "  r@5"), ("  s@8", "  s@11")),
          Set(("  d@13", "  d@0"), ("  g@14", "g")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .filter("cacheN[d.prop] = 5")
        .apply()
        .|.nodeByLabelScan("a", "User", "d")
        .cacheProperties("cacheNFromStore[d.prop]")
        .skip(0)
        .allNodeScan("d")
        .build()
    )
  }

  test("should plan SHORTEST with previously bound relationship") {
    val query =
      """
        |MATCH (from:User)-[r:R]->(to:B)
        |MATCH ANY SHORTEST (from)-[r:R WHERE r.prop = 5]->(b) ((c)-[r2]->(d))* (e)
        |RETURN *""".stripMargin

    val nfa = new TestNFABuilder(0, "from")
      .addTransition(0, 1, "(from)-[`  r@7`:R WHERE `  r@7` = r]->(`  b@8`)")
      .addTransition(1, 2, "(`  b@8`) (`  c@1`)")
      .addTransition(1, 4, "(`  b@8`) (`  e@9`)")
      .addTransition(2, 3, "(`  c@1`)-[`  r2@2`]->(`  d@3`)")
      .addTransition(3, 2, "(`  d@3`) (`  c@1`)")
      .addTransition(3, 4, "(`  d@3`) (`  e@9`)")
      .setFinalState(4)
      .build()

    val plan = nonDeduplicatingPlanner.plan(query).stripProduceResults
    plan should equal(
      nonDeduplicatingPlanner.subPlanBuilder()
        .statefulShortestPath(
          "from",
          "e",
          "SHORTEST 1 ((from)-[`  r@0`:R]->(b) ((`  c@1`)-[`  r2@2`]->(`  d@3`)){0, } (e) WHERE NOT r IN `  r2@5` AND `  r@0` = r AND unique(`  r2@5`))",
          None,
          Set(("  c@1", "  c@4"), ("  d@3", "  d@6")),
          Set(("  r2@2", "  r2@5")),
          Set(("  b@8", "b"), ("  e@9", "e")),
          Set(("  r@7", "  r@0")),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .filter("r.prop = 5", "to:B")
        .expandAll("(from)-[r:R]->(to)")
        .nodeByLabelScan("from", "User")
        .build()
    )
  }

  test("should allow planning of shortest with multiple repeated interior node") {
    val query =
      "MATCH ANY SHORTEST (a:User) ((b)-[r]->(c))* (d) ((e)-[s]->(f))* (d) ((g)-[t]->(h))* (d) RETURN *"

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (`  b@2`)")
        .addTransition(0, 3, "(a) (`  d@20`)")
        .addTransition(1, 2, "(`  b@2`)-[`  r@3`]->(`  c@4`)")
        .addTransition(2, 1, "(`  c@4`) (`  b@2`)")
        .addTransition(2, 3, "(`  c@4`) (`  d@20`)")
        .addTransition(3, 4, "(`  d@20`) (`  e@8`)")
        .addTransition(3, 6, "(`  d@20`) (`  d@21`)")
        .addTransition(4, 5, "(`  e@8`)-[`  s@9`]->(`  f@10`)")
        .addTransition(5, 4, "(`  f@10`) (`  e@8`)")
        .addTransition(5, 6, "(`  f@10`) (`  d@21`)")
        .addTransition(6, 7, "(`  d@21`) (`  g@14`)")
        .addTransition(6, 9, "(`  d@21`) (`  d@22`)")
        .addTransition(7, 8, "(`  g@14`)-[`  t@15`]->(`  h@16`)")
        .addTransition(8, 7, "(`  h@16`) (`  g@14`)")
        .addTransition(8, 9, "(`  h@16`) (`  d@22`)")
        .setFinalState(9)
        .build()

    val plan = nonDeduplicatingPlanner.plan(query).stripProduceResults
    plan should equal(
      nonDeduplicatingPlanner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "d",
          "SHORTEST 1 ((a) ((`  b@2`)-[`  r@3`]->(`  c@4`)){0, } (`  d@0`) ((`  e@8`)-[`  s@9`]->(`  f@10`)){0, } (`  d@1`) ((`  g@14`)-[`  t@15`]->(`  h@16`)){0, } (d) WHERE `  d@0` = d AND `  d@1` = d AND disjoint(`  r@6`, `  s@12`) AND disjoint(`  r@6`, `  t@18`) AND disjoint(`  s@12`, `  t@18`) AND unique(`  r@6`) AND unique(`  s@12`) AND unique(`  t@18`))",
          Some("`  d@0` = d AND `  d@1` = d"),
          Set(
            ("  h@16", "  h@19"),
            ("  f@10", "  f@13"),
            ("  c@4", "  c@7"),
            ("  e@8", "  e@11"),
            ("  g@14", "  g@17"),
            ("  b@2", "  b@5")
          ),
          Set(("  r@3", "  r@6"), ("  s@9", "  s@12"), ("  t@15", "  t@18")),
          Set("  d@20" -> "  d@0", "  d@21" -> "  d@1", "  d@22" -> "d"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("a", "User")
        .build()
    )
  }

  test("should allow planning of shortest with leaf node repeated as interior nodes") {
    val query =
      "MATCH ANY SHORTEST (a:User) ((b)-[r]->(c))* (a) ((e)-[s]->(f))* (a) ((g)-[t]->(h))* (d) RETURN *"

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (`  b@2`)")
        .addTransition(0, 3, "(a) (`  a@20` WHERE `  a@20` = a)")
        .addTransition(1, 2, "(`  b@2`)-[`  r@3`]->(`  c@4`)")
        .addTransition(2, 1, "(`  c@4`) (`  b@2`)")
        .addTransition(2, 3, "(`  c@4`) (`  a@20` WHERE `  a@20` = a)")
        .addTransition(3, 4, "(`  a@20`) (`  e@8`)")
        .addTransition(3, 6, "(`  a@20`) (`  a@21` WHERE `  a@21` = a)")
        .addTransition(4, 5, "(`  e@8`)-[`  s@9`]->(`  f@10`)")
        .addTransition(5, 4, "(`  f@10`) (`  e@8`)")
        .addTransition(5, 6, "(`  f@10`) (`  a@21` WHERE `  a@21` = a)")
        .addTransition(6, 7, "(`  a@21`) (`  g@14`)")
        .addTransition(6, 9, "(`  a@21`) (`  d@22`)")
        .addTransition(7, 8, "(`  g@14`)-[`  t@15`]->(`  h@16`)")
        .addTransition(8, 7, "(`  h@16`) (`  g@14`)")
        .addTransition(8, 9, "(`  h@16`) (`  d@22`)")
        .setFinalState(9)
        .build()

    val plan = nonDeduplicatingPlanner.plan(query).stripProduceResults
    plan should equal(
      nonDeduplicatingPlanner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "d",
          "SHORTEST 1 ((a) ((`  b@2`)-[`  r@3`]->(`  c@4`)){0, } (`  a@0`) ((`  e@8`)-[`  s@9`]->(`  f@10`)){0, } (`  a@1`) ((`  g@14`)-[`  t@15`]->(`  h@16`)){0, } (d) WHERE `  a@0` = a AND `  a@1` = a AND disjoint(`  r@6`, `  s@12`) AND disjoint(`  r@6`, `  t@18`) AND disjoint(`  s@12`, `  t@18`) AND unique(`  r@6`) AND unique(`  s@12`) AND unique(`  t@18`))",
          None,
          Set(
            ("  h@16", "  h@19"),
            ("  f@10", "  f@13"),
            ("  c@4", "  c@7"),
            ("  e@8", "  e@11"),
            ("  g@14", "  g@17"),
            ("  b@2", "  b@5")
          ),
          Set(("  r@3", "  r@6"), ("  s@9", "  s@12"), ("  t@15", "  t@18")),
          Set("  a@20" -> "  a@0", "  a@21" -> "  a@1", "  d@22" -> "d"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("a", "User")
        .build()
    )
  }

  test("should allow planning of shortest with repeated strictly interior node") {
    val query =
      "MATCH ANY SHORTEST (a:User) ((b)-[r]->(c))* (d) ((e)-[s]->(f))* (d) ((g)-[t]->(h))* (i) RETURN *"

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (`  b@1`)")
        .addTransition(0, 3, "(a) (`  d@19`)")
        .addTransition(1, 2, "(`  b@1`)-[`  r@2`]->(`  c@3`)")
        .addTransition(2, 1, "(`  c@3`) (`  b@1`)")
        .addTransition(2, 3, "(`  c@3`) (`  d@19`)")
        .addTransition(3, 4, "(`  d@19`) (`  e@7`)")
        .addTransition(3, 6, "(`  d@19`) (`  d@20`)")
        .addTransition(4, 5, "(`  e@7`)-[`  s@8`]->(`  f@9`)")
        .addTransition(5, 4, "(`  f@9`) (`  e@7`)")
        .addTransition(5, 6, "(`  f@9`) (`  d@20`)")
        .addTransition(6, 7, "(`  d@20`) (`  g@13`)")
        .addTransition(6, 9, "(`  d@20`) (`  i@21`)")
        .addTransition(7, 8, "(`  g@13`)-[`  t@14`]->(`  h@15`)")
        .addTransition(8, 7, "(`  h@15`) (`  g@13`)")
        .addTransition(8, 9, "(`  h@15`) (`  i@21`)")
        .setFinalState(9)
        .build()

    val plan = nonDeduplicatingPlanner.plan(query).stripProduceResults
    plan should equal(
      nonDeduplicatingPlanner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "i",
          "SHORTEST 1 ((a) ((`  b@1`)-[`  r@2`]->(`  c@3`)){0, } (d) ((`  e@7`)-[`  s@8`]->(`  f@9`)){0, } (`  d@0`) ((`  g@13`)-[`  t@14`]->(`  h@15`)){0, } (i) WHERE `  d@0` = d AND disjoint(`  r@5`, `  s@11`) AND disjoint(`  r@5`, `  t@17`) AND disjoint(`  s@11`, `  t@17`) AND unique(`  r@5`) AND unique(`  s@11`) AND unique(`  t@17`))",
          Some("`  d@0` = d"),
          Set(
            ("  e@7", "  e@10"),
            ("  g@13", "  g@16"),
            ("  b@1", "  b@4"),
            ("  c@3", "  c@6"),
            ("  h@15", "  h@18"),
            ("  f@9", "  f@12")
          ),
          Set(("  r@2", "  r@5"), ("  s@8", "  s@11"), ("  t@14", "  t@17")),
          Set("  d@19" -> "d", "  d@20" -> "  d@0", "  i@21" -> "i"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("a", "User")
        .build()
    )
  }

  test("should allow planning of shortest with repeated exterior node") {
    val query =
      "MATCH ANY SHORTEST (a:User) ((b)-[r]->(c))* (a) RETURN *"

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (b)")
        .addTransition(0, 3, "(a) (a)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (a)")
        .setFinalState(3)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "a",
          "SHORTEST 1 ((a) ((`b`)-[`r`]->(`c`)){0, } (a) WHERE unique(`r`))",
          None,
          Set(("b", "b"), ("c", "c")),
          Set(("r", "r")),
          Set(),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandInto
        )
        .nodeByLabelScan("a", "User")
        .build()
    )
  }

  test("should plan SHORTEST with 1 QPP, + quantifier, no predicates, right-to-left") {
    val query = "MATCH ANY SHORTEST (u)((n)-[r]->(m))+(v:User) RETURN *"

    val nfa = new TestNFABuilder(0, "v")
      .addTransition(0, 1, "(v) (m)")
      .addTransition(1, 2, "(m)<-[r]-(n)")
      .addTransition(2, 1, "(n) (m)")
      .addTransition(2, 3, "(n) (u)")
      .setFinalState(3)
      .build()

    val plan = all_if_possible_planner.plan(query).stripProduceResults
    plan should equal(
      all_if_possible_planner.subPlanBuilder()
        .statefulShortestPath(
          "v",
          "u",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v) WHERE unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("u" -> "u"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = true
        )
        .nodeByLabelScan("v", "User")
        .build()
    )
  }

  test("should plan SHORTEST with single node") {
    val query = "MATCH ANY SHORTEST (u:User) RETURN *"
    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should create a relationship outgoing from a strict interior node of a shortest path pattern") {
    val query =
      """MATCH ANY SHORTEST (u:User) ((a)-[r]->(b))+ (v) ((c)-[s]->(d))+ (w)
        |CREATE (v)-[t:R]->(x)
        |RETURN *""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (v)")
      .addTransition(3, 4, "(v) (c)")
      .addTransition(4, 5, "(c)-[s]->(d)")
      .addTransition(5, 4, "(d) (c)")
      .addTransition(5, 6, "(d) (w)")
      .setFinalState(6)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .create(createNode("x"), createRelationship("t", "v", "R", "x", OUTGOING))
        .eager(ListSet(
          ReadCreateConflict.withConflict(Conflict(Id(1), Id(3))),
          TypeReadSetConflict(relTypeName("R")).withConflict(Conflict(Id(1), Id(3)))
        ))
        .statefulShortestPath(
          sourceNode = "u",
          targetNode = "w",
          solvedExpressionString =
            "SHORTEST 1 ((u) ((`a`)-[`r`]->(`b`)){1, } (v) ((`c`)-[`s`]->(`d`)){1, } (w) WHERE disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
          nonInlinedPreFilters = None,
          groupNodes = Set(("a", "a"), ("b", "b"), ("c", "c"), ("d", "d")),
          groupRelationships = Set(("r", "r"), ("s", "s")),
          singletonNodeVariables = Set("v" -> "v", "w" -> "w"),
          singletonRelationshipVariables = Set.empty,
          selector = StatefulShortestPath.Selector.Shortest(1),
          nfa = expectedNfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("should inline predicates on interior and boundary end node") {
    val query =
      """MATCH ANY SHORTEST ((u:User) ((a)-[r:R]->(b:B))+ (v)-[s]->(w:N) WHERE v.prop = 42)
        |RETURN *""".stripMargin
    val plan =
      all_if_possible_planner.plan(query).stripProduceResults

    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a)")
      .addTransition(1, 2, "(a)-[r:R]->(b:B)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (v WHERE v.prop = 42 AND v:B)")
      .addTransition(3, 4, "(v)-[s]->(w:N)")
      .setFinalState(4)
      .build()

    plan should equal(
      all_if_possible_planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((`a`)-[`r`:R]->(`b`) WHERE `b`:B){1, } (v)-[s]->(w) WHERE NOT s IN `r` AND unique(`r`) AND v.prop IN [42] AND v:B AND w:N)",
          None,
          Set(("a", "a"), ("b", "b")),
          Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v", "w" -> "w"),
          singletonRelationshipVariables = Set("s" -> "s"),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("should inline predicates in QPP with undirected relationship") {
    val query =
      """
        |MATCH ANY SHORTEST ((u:User) ((a)-[r:R WHERE r.prop > 0]-(b:B))+ (v))
        |RETURN *
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a)")
      .addTransition(1, 2, "(a)-[r:R WHERE r.prop > 0]-(b:B)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (v WHERE v:B)")
      .setFinalState(3)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((`a`)-[`r`:R]-(`b`) WHERE `b`:B AND `r`.prop > 0){1, } (v) WHERE unique(`r`) AND v:B)",
          None,
          Set(("a", "a"), ("b", "b")),
          Set(("r", "r")),
          singletonNodeVariables = Set(("v", "v")),
          singletonRelationshipVariables = Set(),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("should inline predicates that depend on interior and boundary start node") {
    val query =
      """MATCH ANY SHORTEST ((u:User) ((a)-[r:R]->(b:B))+ (v)-[s]->(w:N) WHERE v.prop = u.prop AND s.prop = u.prop)
        |RETURN *""".stripMargin
    val plan =
      planner.plan(query).stripProduceResults

    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a)")
      .addTransition(1, 2, "(a)-[r:R]->(b:B)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (v WHERE v.prop = cacheNFromStore[u.prop] AND v:B)")
      .addTransition(3, 4, "(v)-[s WHERE s.prop = cacheNFromStore[u.prop]]->(w WHERE w:N)")
      .setFinalState(4)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((`a`)-[`r`:R]->(`b`) WHERE `b`:B){1, } (v)-[s]->(w) WHERE NOT s IN `r` AND s.prop = u.prop AND unique(`r`) AND v.prop = u.prop AND v:B AND w:N)",
          None,
          Set(("a", "a"), ("b", "b")),
          Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v", "w" -> "w"),
          singletonRelationshipVariables = Set("s" -> "s"),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("should plan non inlined predicates") {
    val query =
      """MATCH ANY SHORTEST ((u:User) ((a)-[r]->(b))+ (v)-[s]-(w) WHERE v.prop = w.prop AND size(a) <> 5)
        |RETURN *""".stripMargin
    val plan =
      all_if_possible_planner.plan(query).stripProduceResults

    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (v)")
      .addTransition(3, 4, "(v)-[s]-(w)")
      .setFinalState(4)
      .build()

    plan should equal(
      all_if_possible_planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((`a`)-[`r`]->(`b`)){1, } (v)-[s]-(w) WHERE NOT s IN `r` AND NOT size(`a`) IN [5] AND unique(`r`) AND v.prop = w.prop)",
          Some("v.prop = w.prop AND NOT size(a) = 5"),
          Set(("a", "a"), ("b", "b")),
          Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v", "w" -> "w"),
          singletonRelationshipVariables = Set("s" -> "s"),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should match on a strict interior node of a shortest path pattern from previous MATCH clause") {
    val query =
      """MATCH ANY SHORTEST (u:User) ((a)-[r]->(b))+ (v) ((c)-[s]->(d))+ (w)-[t]->(x)
        |MATCH (v)-[p:R]->(e)
        |RETURN *""".stripMargin
    val plan =
      planner.plan(query).stripProduceResults

    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (v)")
      .addTransition(3, 4, "(v) (c)")
      .addTransition(4, 5, "(c)-[s]->(d)")
      .addTransition(5, 4, "(d) (c)")
      .addTransition(5, 6, "(d) (w)")
      .addTransition(6, 7, "(w)-[t]->(x)")
      .setFinalState(7)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .expandAll("(v)-[p:R]->(e)")
        .statefulShortestPath(
          sourceNode = "u",
          targetNode = "x",
          solvedExpressionString =
            "SHORTEST 1 ((u) ((`a`)-[`r`]->(`b`)){1, } (v) ((`c`)-[`s`]->(`d`)){1, } (w)-[t]->(x) WHERE NOT t IN `r` AND NOT t IN `s` AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
          nonInlinedPreFilters = None,
          groupNodes = Set(("a", "a"), ("b", "b"), ("c", "c"), ("d", "d")),
          groupRelationships = Set(("r", "r"), ("s", "s")),
          singletonNodeVariables = Set("v" -> "v", "w" -> "w", "x" -> "x"),
          singletonRelationshipVariables = Set("t" -> "t"),
          selector = StatefulShortestPath.Selector.Shortest(1),
          nfa = expectedNfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should support a strict interior node of a shortest path pattern to be repeated, inside QPP") {
    val query =
      """MATCH ANY SHORTEST (u:User) ((b)--(b))* (c)
        |RETURN *""".stripMargin

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (b)")
      .addTransition(0, 3, "(u) (c)")
      .addTransition(1, 2, "(b)-[anon_0]-(b)")
      .addTransition(2, 1, "(b) (b)")
      .addTransition(2, 3, "(b) (c)")
      .setFinalState(3)
      .build()

    val plan = planner.plan(query).stripProduceResults
    val expected = planner.subPlanBuilder()
      .statefulShortestPath(
        "u",
        "c",
        "SHORTEST 1 ((u) ((`b`)-[`anon_0`]-(`b`)){0, } (c) WHERE `b` = `b` AND unique(`anon_6`))",
        Some("all(anon_1 IN range(0, size(b) - 1) WHERE b[anon_1] = b[anon_1])"),
        Set(("b", "b")),
        Set(),
        Set(("c", "c")),
        Set(),
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll
      )
      .nodeByLabelScan("u", "User")
      .build()
    plan should equal(expected)
  }

  test("Should support a shortest path pattern with a predicate on several entities inside a QPP") {
    val query =
      """MATCH ANY SHORTEST (u:User) ((b)--(c) WHERE b.prop < c.prop)* (d)
        |RETURN *""".stripMargin

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (b)")
      .addTransition(0, 3, "(u) (d)")
      .addTransition(1, 2, "(b)-[anon_0]-(c)")
      .addTransition(2, 1, "(c) (b)")
      .addTransition(2, 3, "(c) (d)")
      .setFinalState(3)
      .build()

    val plan = planner.plan(query).stripProduceResults
    val expected = planner.subPlanBuilder()
      .statefulShortestPath(
        "u",
        "d",
        "SHORTEST 1 ((u) ((`b`)-[`anon_0`]-(`c`)){0, } (d) WHERE `b`.prop < `c`.prop AND unique(`anon_5`))",
        Some("all(anon_1 IN range(0, size(b) - 1) WHERE (b[anon_1]).prop < (c[anon_1]).prop)"),
        Set(("b", "b"), ("c", "c")),
        Set(),
        Set(("d", "d")),
        Set(),
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll
      )
      .nodeByLabelScan("u", "User")
      .build()
    plan should equal(expected)
  }

  test(
    "Should support a shortest path pattern with a predicate on undirected relationship and boundary node inside a QPP"
  ) {
    val query =
      """MATCH ANY SHORTEST (u:User) ((b)-[r]-(c) WHERE r.prop < c.prop)* (d)
        |RETURN *""".stripMargin

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (b)")
      .addTransition(0, 3, "(u) (d)")
      .addTransition(1, 2, "(b)-[r]-(c)")
      .addTransition(2, 1, "(c) (b)")
      .addTransition(2, 3, "(c) (d)")
      .setFinalState(3)
      .build()

    val plan = planner.plan(query).stripProduceResults
    val expected = planner.subPlanBuilder()
      .statefulShortestPath(
        "u",
        "d",
        "SHORTEST 1 ((u) ((`b`)-[`r`]-(`c`)){0, } (d) WHERE `r`.prop < `c`.prop AND unique(`r`))",
        Some("all(anon_0 IN range(0, size(c) - 1) WHERE (r[anon_0]).prop < (c[anon_0]).prop)"),
        Set(("b", "b"), ("c", "c")),
        Set(("r", "r")),
        Set(("d", "d")),
        Set(),
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll
      )
      .nodeByLabelScan("u", "User")
      .build()
    plan should equal(expected)
  }

  test(
    "should plan SHORTEST with predicate depending on no variables as a filter before the statefulShortestPath"
  ) {
    val query = "MATCH ANY SHORTEST ((u:User)((n)-[r]->(m))+(v) WHERE $param) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .setFinalState(3)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v) WHERE unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set(("v", "v")),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .filter("CoerceToPredicate($param)")
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test(
    "should plan SHORTEST with predicate depending on arguments only as a filter before the statefulShortestPath"
  ) {
    val query =
      """MATCH (arg)
        |WITH DISTINCT arg
        |MATCH ANY SHORTEST ((u:User)((n)-[r]->(m))+(v) WHERE arg.prop > 5)
        |RETURN *
        |""".stripMargin

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .setFinalState(3)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v) WHERE unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set(("v", "v")),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .filter("cacheN[arg.prop] > 5")
        .apply()
        .|.nodeByLabelScan("u", "User", "arg")
        .cacheProperties("cacheNFromStore[arg.prop]")
        .allNodeScan("arg")
        .build()
    )
  }

  test(
    "should plan SHORTEST with predicate depending on arguments and start node only as a filter before the statefulShortestPath"
  ) {
    val query =
      """MATCH (arg)
        |WITH DISTINCT arg
        |MATCH ANY SHORTEST ((u:User)((n)-[r]->(m))+(v) WHERE arg.prop > u.prop)
        |RETURN *
        |""".stripMargin

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .setFinalState(3)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v) WHERE unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set(("v", "v")),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .filter("cacheN[arg.prop] > u.prop")
        .apply()
        .|.nodeByLabelScan("u", "User", "arg")
        .cacheProperties("cacheNFromStore[arg.prop]")
        .allNodeScan("arg")
        .build()
    )
  }

  test(
    "should plan SHORTEST with predicate depending on target boundary variable inside the NFA"
  ) {
    val query = "MATCH ANY SHORTEST (u:User)((n)-[r]->(m))+(v:NN) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v:NN)")
      .setFinalState(3)
      .build()

    val plan = all_if_possible_planner.plan(query).stripProduceResults
    plan should equal(
      all_if_possible_planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v) WHERE unique(`r`) AND v:NN)",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should plan SHORTEST if both start and end are already bound") {
    val query =
      s"""
         |MATCH (n:User), (m)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST (n)((n_inner)-[r_inner]->(m_inner))+ (m)
         |RETURN *
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val nfa = new TestNFABuilder(0, "n")
      .addTransition(0, 1, "(n) (n_inner)")
      .addTransition(1, 2, "(n_inner)-[r_inner]->(m_inner)")
      .addTransition(2, 1, "(m_inner) (n_inner)")
      .addTransition(2, 3, "(m_inner) (m)")
      .setFinalState(3)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "n",
          "m",
          "SHORTEST 1 ((n) ((`n_inner`)-[`r_inner`]->(`m_inner`)){1, } (m) WHERE unique(`r_inner`))",
          None,
          groupNodes = Set(("n_inner", "n_inner"), ("m_inner", "m_inner")),
          groupRelationships = Set(("r_inner", "r_inner")),
          singletonNodeVariables = Set(),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandInto
        )
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("m")
        .nodeByLabelScan("n", "User")
        .build()
    )
  }

  test("Should plan SHORTEST if both start and end are already bound with rewritten selection on boundary nodes") {
    val query =
      s"""
         |MATCH (n:N), (o)
         |  // This makes it deterministic which plans ends up on what side of a CartesianProduct.
         |  WHERE n.prop = 5
         |WITH * SKIP 1
         |MATCH ANY SHORTEST ((n)((n_inner)-[r_inner]->(m_inner))+ (m)-[r2]->(o) WHERE m.prop = o.prop)
         |RETURN *
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val nfa = new TestNFABuilder(0, "n")
      .addTransition(0, 1, "(n) (n_inner)")
      .addTransition(1, 2, "(n_inner)-[r_inner]->(m_inner)")
      .addTransition(2, 1, "(m_inner) (n_inner)")
      .addTransition(2, 3, "(m_inner) (m WHERE m.prop = o.prop)")
      .addTransition(3, 4, "(m)-[r2]->(o)")
      .setFinalState(4)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "n",
          "o",
          "SHORTEST 1 ((n) ((`n_inner`)-[`r_inner`]->(`m_inner`)){1, } (m)-[r2]->(o) WHERE NOT r2 IN `r_inner` AND m.prop = o.prop AND unique(`r_inner`))",
          None,
          groupNodes = Set(("n_inner", "n_inner"), ("m_inner", "m_inner")),
          groupRelationships = Set(("r_inner", "r_inner")),
          singletonNodeVariables = Set("m" -> "m"),
          singletonRelationshipVariables = Set("r2" -> "r2"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandInto
        )
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("o")
        .filter("n.prop = 5")
        .nodeByLabelScan("n", "N")
        .build()
    )
  }

  test("Should use cached properties from previously bound variables inside NFA") {
    val query = "MATCH ANY SHORTEST ((u:User WHERE u.prop > 5)((n)-[r]->(m))+(v) WHERE v.prop = u.prop) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v WHERE v.prop = cacheN[u.prop])")
      .setFinalState(3)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v) WHERE unique(`r`) AND v.prop = u.prop)",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set(("v", "v")),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeIndexOperator("u:User(prop > 5)", getValue = Map("prop" -> GetValue))
        .build()
    )
  }

  test("should plan pattern expression predicates") {
    // We compare "solvedExpressionAsString" nested inside NestedPlanExpressions.
    // This saves us from windows line break mismatches in those strings.
    implicit val windowsSafe: WindowsSafeAnyRef[LogicalPlan] = new WindowsSafeAnyRef[LogicalPlan]

    val query =
      """MATCH ANY SHORTEST ((u:User) ((a)-[r]->(b))+ (v)-[s]->(w) WHERE (v)-->(:N))
        |RETURN *""".stripMargin
    val plan =
      planner.plan(query).stripProduceResults

    val nestedPlan = planner.subPlanBuilder()
      .filter("anon_1:N")
      .expand("(v)-[anon_0]->(anon_1)")
      .argument("v")
      .build()

    val solvedNestedExpressionAsString =
      """EXISTS { MATCH (v)-[`anon_0`]->(`anon_1`)
        |  WHERE `anon_1`:N }""".stripMargin
    val patternExpressionPredicate = NestedPlanExistsExpression(
      plan = nestedPlan,
      solvedExpressionAsString = solvedNestedExpressionAsString
    )(pos)
    val juxtapositionPredicate = NodeJuxtapositionPredicate(Some(VariablePredicate(v"v", patternExpressionPredicate)))

    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2 -> "b", 3 -> "v", juxtapositionPredicate)
      .addTransition(3, 4, "(v)-[s]->(w)")
      .setFinalState(4)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPathExpr(
          "u",
          "w",
          s"SHORTEST 1 ((u) ((`a`)-[`r`]->(`b`)){1, } (v)-[s]->(w) WHERE $solvedNestedExpressionAsString AND NOT s IN `r` AND unique(`r`))",
          None,
          Set(("a", "a"), ("b", "b")),
          Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v", "w" -> "w"),
          singletonRelationshipVariables = Set("s" -> "s"),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("should plan pattern expression predicates inside QPP") {
    // We compare "solvedExpressionAsString" nested inside NestedPlanExpressions.
    // This saves us from windows line break mismatches in those strings.
    implicit val windowsSafe: WindowsSafeAnyRef[LogicalPlan] = new WindowsSafeAnyRef[LogicalPlan]

    val query =
      """MATCH ANY SHORTEST ((u:User)(
        |  (n)-[r]->(m)
        |    WHERE (m)-[]->(:N)
        |  )+(v))
        |RETURN *""".stripMargin
    val planner = nonDeduplicatingPlanner

    val plan = planner.plan(query)

    val nestedPlan = planner.subPlanBuilder()
      .filter("`  UNNAMED1`:N")
      .expand("(`  m@2`)-[`  UNNAMED0`]->(`  UNNAMED1`)")
      .argument("  m@2")
      .build()

    val solvedNestedExpressionAsString =
      """EXISTS { MATCH (`  m@2`)-[`  UNNAMED0`]->(`  UNNAMED1`)
        |  WHERE `  UNNAMED1`:N }""".stripMargin
    val nestedPlanExpression = NestedPlanExistsExpression(
      plan = nestedPlan,
      solvedExpressionAsString =
        solvedNestedExpressionAsString
    )(pos)
    val expansionPredicate = RelationshipExpansionPredicate(
      v"  r@1",
      None,
      Seq.empty,
      OUTGOING,
      Some(VariablePredicate(v"  m@2", nestedPlanExpression))
    )

    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (`  n@0`)")
      .addTransition(1 -> "  n@0", 2 -> "  m@2", expansionPredicate)
      .addTransition(2, 1, "(`  m@2`) (`  n@0`)")
      .addTransition(2, 3, "(`  m@2`) (`  v@6`)")
      .setFinalState(3)
      .build()

    plan should equal(
      planner.planBuilder()
        .produceResults("`  m@5`", "`  n@3`", "`  r@4`", "u", "v")
        .statefulShortestPathExpr(
          "u",
          "v",
          """SHORTEST 1 ((u) ((`  n@0`)-[`  r@1`]->(`  m@2`) WHERE EXISTS { MATCH (`  m@2`)-[`  UNNAMED0`]->(`  UNNAMED1`)
            |  WHERE `  UNNAMED1`:N }){1, } (v) WHERE unique(`  r@4`))""".stripMargin,
          None,
          Set(("  n@0", "  n@3"), ("  m@2", "  m@5")),
          Set(("  r@1", "  r@4")),
          Set(("  v@6", "v")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("should plan non-inlineable pattern expression predicates combined with normal predicate inside QPP") {
    val query =
      """MATCH ANY SHORTEST ((u:User)(
        |  (n)-[r]->(m)
        |    WHERE CASE
        |      WHEN (m)-[]->(:N) THEN n.prop > m.prop
        |      ELSE false
        |    END
        |  )+(v))
        |RETURN *""".stripMargin
    val planner = nonDeduplicatingPlanner

    val plan = planner.plan(query)

    val nestedPlan = planner.subPlanBuilder()
      .filter("`  UNNAMED1`:N")
      .expand("(`  m@2`)-[`  UNNAMED0`]->(`  UNNAMED1`)")
      .projection("`  m@5`[`  UNNAMED2`] AS `  m@2`")
      .argument("  m@5", "  UNNAMED2")
      .build()

    val solvedNestedExpressionAsString =
      """EXISTS { MATCH (`  m@2`)-[`  UNNAMED0`]->(`  UNNAMED1`)
        |  WHERE `  UNNAMED1`:N }""".stripMargin
    val nestedPlanExpression = NestedPlanExistsExpression(
      plan = nestedPlan,
      solvedExpressionAsString =
        solvedNestedExpressionAsString
    )(pos)

    val nonInlineablePredicate = allInList(
      v"  UNNAMED2",
      function("range", literalInt(0), subtract(function("size", v"  m@5"), literalInt(1))),
      caseExpression(
        None,
        Some(literalBoolean(false)),
        (
          nestedPlanExpression,
          greaterThan(
            propExpression(containerIndex(v"  n@3", v"  UNNAMED2"), "prop"),
            propExpression(containerIndex(v"  m@5", v"  UNNAMED2"), "prop")
          )
        )
      )
    )

    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (`  n@0`)")
      .addTransition(1, 2, "(`  n@0`)-[`  r@1`]->(`  m@2`)")
      .addTransition(2, 1, "(`  m@2`) (`  n@0`)")
      .addTransition(2, 3, "(`  m@2`) (`  v@6`)")
      .setFinalState(3)
      .build()

    plan should equal(
      planner.planBuilder()
        .produceResults("`  m@5`", "`  n@3`", "`  r@4`", "u", "v")
        .statefulShortestPathExpr(
          "u",
          "v",
          """SHORTEST 1 ((u) ((`  n@0`)-[`  r@1`]->(`  m@2`)){1, } (v) WHERE CASE
            |  WHEN EXISTS { MATCH (`  m@2`)-[`  UNNAMED0`]->(`  UNNAMED1`)
            |  WHERE `  UNNAMED1`:N } THEN `  n@0`.prop > `  m@2`.prop
            |  ELSE false
            |END AND unique(`  r@4`))""".stripMargin,
          Some(nonInlineablePredicate),
          Set(("  n@0", "  n@3"), ("  m@2", "  m@5")),
          Set(("  r@1", "  r@4")),
          Set(("  v@6", "v")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("should plan subquery expression inside QPP") {
    // We compare "solvedExpressionAsString" nested inside NestedPlanExpressions.
    // This saves us from windows line break mismatches in those strings.
    implicit val windowsSafe: WindowsSafeAnyRef[LogicalPlan] = new WindowsSafeAnyRef[LogicalPlan]

    // GIVEN
    val query =
      """MATCH ANY SHORTEST ((u:User)(
        |  (n)-[r]->(m)
        |    WHERE COUNT { (m)-[]->(:N) } = 2
        |  )+(v))
        |RETURN *""".stripMargin
    val planner = nonDeduplicatingPlanner

    // WHEN
    val plan = planner.plan(query)

    // THEN
    val nestedPlan = planner.subPlanBuilder()
      .aggregation(Seq.empty, Seq("count(*) AS `  UNNAMED2`"))
      .filter("`  UNNAMED1`:N")
      .expand("(`  m@2`)-[`  UNNAMED0`]->(`  UNNAMED1`)")
      .argument("  m@2")
      .build()

    val solvedNestedExpressionAsString =
      """COUNT { MATCH (`  m@2`)-[`  UNNAMED0`]->(`  UNNAMED1`)
        |  WHERE `  UNNAMED1`:N }""".stripMargin
    val nestedPlanExpression = NestedPlanGetByNameExpression(
      plan = nestedPlan,
      v"  UNNAMED2",
      solvedExpressionAsString = solvedNestedExpressionAsString
    )(pos)
    val eq2 = equals(nestedPlanExpression, literalInt(2))
    val expansionPredicate = RelationshipExpansionPredicate(
      v"  r@1",
      None,
      Seq.empty,
      OUTGOING,
      Some(VariablePredicate(v"  m@2", eq2))
    )

    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (`  n@0`)")
      .addTransition(1 -> "  n@0", 2 -> "  m@2", expansionPredicate)
      .addTransition(2, 1, "(`  m@2`) (`  n@0`)")
      .addTransition(2, 3, "(`  m@2`) (`  v@6`)")
      .setFinalState(3)
      .build()

    plan should equal(
      planner.planBuilder()
        .produceResults("`  m@5`", "`  n@3`", "`  r@4`", "u", "v")
        .statefulShortestPathExpr(
          "u",
          "v",
          """SHORTEST 1 ((u) ((`  n@0`)-[`  r@1`]->(`  m@2`) WHERE COUNT { MATCH (`  m@2`)-[`  UNNAMED0`]->(`  UNNAMED1`)
            |  WHERE `  UNNAMED1`:N } = 2){1, } (v) WHERE unique(`  r@4`))""".stripMargin,
          None,
          Set(("  n@0", "  n@3"), ("  m@2", "  m@5")),
          Set(("  r@1", "  r@4")),
          Set(("  v@6", "v")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("should plan subquery expression predicates with multiple dependencies") {
    // We compare "solvedExpressionAsString" nested inside NestedPlanExpressions.
    // This saves us from windows line break mismatches in those strings.
    implicit val windowsSafe: WindowsSafeAnyRef[LogicalPlan] = new WindowsSafeAnyRef[LogicalPlan]

    val query =
      """MATCH ANY SHORTEST ((u:User) ((a)-[r]->(b))+ (v)-[s]->(w)-[t]->(x) WHERE EXISTS { (v)<--(w) })
        |RETURN *""".stripMargin
    val plan =
      planner.plan(query).stripProduceResults

    val nestedPlan = planner.subPlanBuilder()
      .expandInto("(v)<-[anon_0]-(w)")
      .argument("v", "w")
      .build()

    val solvedNestedExpressionAsString = "EXISTS { MATCH (v)<-[`anon_0`]-(w) }"
    val nestedPlanExpression = NestedPlanExistsExpression(
      plan = nestedPlan,
      solvedExpressionAsString = solvedNestedExpressionAsString
    )(pos)

    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (v)")
      .addTransition(3, 4, "(v)-[s]->(w)")
      .addTransition(4, 5, "(w)-[t]->(x)")
      .setFinalState(5)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPathExpr(
          "u",
          "x",
          s"SHORTEST 1 ((u) ((`a`)-[`r`]->(`b`)){1, } (v)-[s]->(w)-[t]->(x) WHERE $solvedNestedExpressionAsString AND NOT s IN `r` AND NOT t = s AND NOT t IN `r` AND unique(`r`))",
          Some(nestedPlanExpression),
          Set(("a", "a"), ("b", "b")),
          Set(("r", "r")),
          singletonNodeVariables = Set("w" -> "w", "v" -> "v", "x" -> "x"),
          singletonRelationshipVariables = Set("t" -> "t", "s" -> "s"),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should handle shortest path in subquery expression") {
    val query =
      """MATCH (m:User)
        |  WHERE CASE
        |    WHEN m.prop IS NOT NULL
        |      THEN EXISTS { MATCH SHORTEST 1 (m) (()--())+ (other:User) }
        |    ELSE false
        |  END
        |RETURN m""".stripMargin

    println(planner.plan(query))
  }

  test("Should handle path assignment for shortest path containing qpp with two juxtaposed nodes") {
    val query = "MATCH p = ANY SHORTEST (a:User) ((b)-[r]->(c))+ (d) RETURN p"
    val plan = shortest_without_legacy.plan(query).stripProduceResults

    val path = PathExpression(NodePathStep(
      v"a",
      MultiRelationshipPathStep(v"r", OUTGOING, Some(v"d"), NilPathStep()(pos))(pos)
    )(pos))(pos)

    plan shouldEqual shortest_without_legacy.subPlanBuilder()
      .projection(Map("p" -> path))
      .statefulShortestPath(
        "a",
        "d",
        "SHORTEST 1 ((a) ((`b`)-[`r`]->(`c`)){1, } (d) WHERE unique(`r`))",
        None,
        Set.empty,
        Set(("r", "r")),
        Set(("d", "d")),
        Set.empty,
        StatefulShortestPath.Selector.Shortest(1),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a) (b)")
          .addTransition(1, 2, "(b)-[r]->(c)")
          .addTransition(2, 1, "(c) (b)")
          .addTransition(2, 3, "(c) (d)")
          .setFinalState(3)
          .build(),
        ExpandAll
      )
      .nodeByLabelScan("a", "User")
      .build()
  }

  test("Should handle path assignment for shortest path containing qpp with two juxtaposed patterns") {
    val query = "MATCH p = ANY SHORTEST (a:User)--(b) ((c)-[r]->(d))+ (e)--(f) RETURN p"
    val plan = planner.plan(query).stripProduceResults

    val path = PathExpression(
      NodePathStep(
        v"a",
        SingleRelationshipPathStep(
          v"anon_0",
          BOTH,
          Some(v"b"),
          MultiRelationshipPathStep(
            v"r",
            OUTGOING,
            Some(v"e"),
            SingleRelationshipPathStep(
              v"anon_1",
              BOTH,
              Some(v"f"),
              NilPathStep()(pos)
            )(pos)
          )(pos)
        )(pos)
      )(pos)
    )(pos)

    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("p" -> path))
      .statefulShortestPath(
        "a",
        "f",
        "SHORTEST 1 ((a)-[`anon_0`]-(b) ((`c`)-[`r`]->(`d`)){1, } (e)-[`anon_1`]-(f) WHERE NOT `anon_0` = `anon_1` AND NOT `anon_0` IN `r` AND NOT `anon_1` IN `r` AND unique(`r`))",
        None,
        Set.empty,
        Set(("r", "r")),
        Set("e" -> "e", "b" -> "b", "f" -> "f"),
        Set("anon_2" -> "anon_0", "anon_3" -> "anon_1"),
        StatefulShortestPath.Selector.Shortest(1),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a)-[anon_2]-(b)")
          .addTransition(1, 2, "(b) (c)")
          .addTransition(2, 3, "(c)-[r]->(d)")
          .addTransition(3, 2, "(d) (c)")
          .addTransition(3, 4, "(d) (e)")
          .addTransition(4, 5, "(e)-[anon_3]-(f)")
          .setFinalState(5)
          .build(),
        ExpandAll
      )
      .nodeByLabelScan("a", "User")
      .build()
  }

  test("Should handle path assignment for shortest path with simple pattern") {
    val query = "MATCH p = ANY SHORTEST (a:User)-[r]->(b) RETURN p"
    val plan = planner.plan(query).stripProduceResults

    val path = PathExpression(
      NodePathStep(
        v"a",
        SingleRelationshipPathStep(
          v"r",
          OUTGOING,
          Some(v"b"),
          NilPathStep()(pos)
        )(pos)
      )(pos)
    )(pos)

    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("p" -> path))
      .statefulShortestPath(
        "a",
        "b",
        "SHORTEST 1 ((a)-[r]->(b))",
        None,
        Set(),
        Set(),
        Set("b" -> "b"),
        Set("r" -> "r"),
        StatefulShortestPath.Selector.Shortest(1),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a)-[r]->(b)")
          .setFinalState(1)
          .build(),
        ExpandAll
      )
      .nodeByLabelScan("a", "User")
      .build()
  }

  test("should plan SHORTEST from the lower cardinality side") {
    val query = "MATCH ANY SHORTEST (a:A)((n)-[r]->(m))+(b:B) RETURN *"

    val nfaLR = new TestNFABuilder(0, "a")
      .addTransition(0, 1, "(a) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (b:B)")
      .setFinalState(3)
      .build()
    val planLR = planner.subPlanBuilder()
      .statefulShortestPath(
        "a",
        "b",
        "SHORTEST 1 ((a) ((`n`)-[`r`]->(`m`)){1, } (b) WHERE b:B AND unique(`r`))",
        None,
        groupNodes = Set(("n", "n"), ("m", "m")),
        groupRelationships = Set(("r", "r")),
        singletonNodeVariables = Set("b" -> "b"),
        singletonRelationshipVariables = Set(),
        StatefulShortestPath.Selector.Shortest(1),
        nfaLR,
        ExpandAll
      )
      .nodeByLabelScan("a", "A")
      .build()

    val nfaRL = new TestNFABuilder(0, "b")
      .addTransition(0, 1, "(b) (m)")
      .addTransition(1, 2, "(m)<-[r]-(n)")
      .addTransition(2, 1, "(n) (m)")
      .addTransition(2, 3, "(n) (a:A)")
      .setFinalState(3)
      .build()
    val planRL = planner.subPlanBuilder()
      .statefulShortestPath(
        "b",
        "a",
        "SHORTEST 1 ((a) ((`n`)-[`r`]->(`m`)){1, } (b) WHERE a:A AND unique(`r`))",
        None,
        groupNodes = Set(("n", "n"), ("m", "m")),
        groupRelationships = Set(("r", "r")),
        singletonNodeVariables = Set("a" -> "a"),
        singletonRelationshipVariables = Set(),
        StatefulShortestPath.Selector.Shortest(1),
        nfaRL,
        ExpandAll,
        reverseGroupVariableProjections = true
      )
      .nodeByLabelScan("b", "B")
      .build()

    // If :A is cheaper
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(40)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 20)
      .setRelationshipCardinality("(:A)-[]->()", 20)
      .setRelationshipCardinality("(:A)-[]->(:B)", 20)
      .setRelationshipCardinality("()-[]->(:B)", 20)
      .addSemanticFeature(SemanticFeature.GpmShortestPath)
      .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, ALL_IF_POSSIBLE)
      .build()
      .plan(query).stripProduceResults should equal(planLR)

    // If :B is cheaper
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(40)
      .setLabelCardinality("A", 20)
      .setLabelCardinality("B", 10)
      .setRelationshipCardinality("(:A)-[]->()", 20)
      .setRelationshipCardinality("(:A)-[]->(:B)", 20)
      .setRelationshipCardinality("()-[]->(:B)", 20)
      .addSemanticFeature(SemanticFeature.GpmShortestPath)
      .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, ALL_IF_POSSIBLE)
      .build()
      .plan(query).stripProduceResults should equal(planRL)
  }

  test("Should handle sub-path assignment with pre-filter predicates for shortest path") {
    val query = "MATCH ANY SHORTEST (p = (a:User) ((b)-[r]->(c))+ (d) ((e)<-[s]-(f))+ (g) WHERE length(p) > 3) RETURN p"
    val plan = planner.plan(query).stripProduceResults

    val path = PathExpression(NodePathStep(
      v"a",
      MultiRelationshipPathStep(
        v"r",
        OUTGOING,
        Some(v"d"),
        MultiRelationshipPathStep(v"s", OUTGOING, Some(v"g"), NilPathStep()(pos))(pos)
      )(pos)
    )(pos))(pos)

    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("p" -> path))
      .statefulShortestPathExpr(
        "a",
        "g",
        s"SHORTEST 1 ((a) ((`b`)-[`r`]->(`c`)){1, } (d) ((`e`)<-[`s`]-(`f`)){1, } (g) WHERE disjoint(`r`, `s`) AND length((a) ((b)-[r]-())* (d) ((e)-[s]-())* (g)) > 3 AND unique(`r`) AND unique(`s`))",
        Some(greaterThan(length(path), literalInt(3))),
        Set.empty,
        Set(("r", "r"), ("s", "s")),
        Set("d" -> "d", "g" -> "g"),
        Set(),
        StatefulShortestPath.Selector.Shortest(1),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a) (b)")
          .addTransition(1, 2, "(b)-[r]->(c)")
          .addTransition(2, 1, "(c) (b)")
          .addTransition(2, 3, "(c) (d)")
          .addTransition(3, 4, "(d) (e)")
          .addTransition(4, 5, "(e)<-[s]-(f)")
          .addTransition(5, 4, "(f) (e)")
          .addTransition(5, 6, "(f) (g)")
          .setFinalState(6)
          .build(),
        ExpandAll
      )
      .nodeByLabelScan("a", "User")
      .build()
  }

  // There was a subtlety in unwrapParenthesizedPath leading to issues for sub-paths with no predicates
  test("Should handle sub-path assignment with no predicates for shortest path") {
    val query = "MATCH ANY SHORTEST (p = (a) ((b)-[r]->(c))+ (d) ((e)<-[s]-(f))+ (g)) RETURN p"
    val plan = planner.plan(query).stripProduceResults

    val path = PathExpression(NodePathStep(
      v"a",
      MultiRelationshipPathStep(
        v"r",
        OUTGOING,
        Some(v"d"),
        MultiRelationshipPathStep(v"s", OUTGOING, Some(v"g"), NilPathStep()(pos))(pos)
      )(pos)
    )(pos))(pos)

    val nfa = new TestNFABuilder(0, "a")
      .addTransition(0, 1, "(a) (b)")
      .addTransition(1, 2, "(b)-[r]->(c)")
      .addTransition(2, 1, "(c) (b)")
      .addTransition(2, 3, "(c) (d)")
      .addTransition(3, 4, "(d) (e)")
      .addTransition(4, 5, "(e)<-[s]-(f)")
      .addTransition(5, 4, "(f) (e)")
      .addTransition(5, 6, "(f) (g)")
      .setFinalState(6)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .projection(Map("p" -> path))
        .statefulShortestPathExpr(
          "a",
          "g",
          s"SHORTEST 1 ((a) ((`b`)-[`r`]->(`c`)){1, } (d) ((`e`)<-[`s`]-(`f`)){1, } (g) WHERE disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
          None,
          Set.empty,
          Set(("r", "r"), ("s", "s")),
          Set("d" -> "d", "g" -> "g"),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .allNodeScan("a")
        .build()
    )
  }

  test("Should handle both path and sub-path assignment") {
    val query =
      "MATCH p = ANY SHORTEST (q = (a:User) ((b)-[r]->(c))+ (d) ((e)<-[s]-(f))+ (g) WHERE length(q) > 3) RETURN p, q"
    val plan = planner.plan(query).stripProduceResults

    val path = PathExpression(NodePathStep(
      v"a",
      MultiRelationshipPathStep(
        v"r",
        OUTGOING,
        Some(v"d"),
        MultiRelationshipPathStep(v"s", OUTGOING, Some(v"g"), NilPathStep()(pos))(pos)
      )(pos)
    )(pos))(pos)

    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("p" -> path, "q" -> path))
      .statefulShortestPathExpr(
        "a",
        "g",
        s"SHORTEST 1 ((a) ((`b`)-[`r`]->(`c`)){1, } (d) ((`e`)<-[`s`]-(`f`)){1, } (g) WHERE disjoint(`r`, `s`) AND length((a) ((b)-[r]-())* (d) ((e)-[s]-())* (g)) > 3 AND unique(`r`) AND unique(`s`))",
        Some(greaterThan(length(path), literalInt(3))),
        Set.empty,
        Set(("r", "r"), ("s", "s")),
        Set("d" -> "d", "g" -> "g"),
        Set(),
        StatefulShortestPath.Selector.Shortest(1),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a) (b)")
          .addTransition(1, 2, "(b)-[r]->(c)")
          .addTransition(2, 1, "(c) (b)")
          .addTransition(2, 3, "(c) (d)")
          .addTransition(3, 4, "(d) (e)")
          .addTransition(4, 5, "(e)<-[s]-(f)")
          .addTransition(5, 4, "(f) (e)")
          .addTransition(5, 6, "(f) (g)")
          .setFinalState(6)
          .build(),
        ExpandAll
      )
      .nodeByLabelScan("a", "User")
      .build()
  }

  test("Should inline relationship local (start and end node) predicate into NFA: outside QPP") {
    val query = "MATCH ANY SHORTEST ((u:User)((n)-[r]->(m))+(v)-[r2]->(w) WHERE v.prop > r2.prop + w.prop) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .addTransition(3, 4, "(v)-[r2 WHERE startNode(r2).prop > r2.prop + endNode(r2).prop]->(w)")
      .setFinalState(4)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v)-[r2]->(w) WHERE NOT r2 IN `r` AND unique(`r`) AND v.prop > r2.prop + w.prop)",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v", "w" -> "w"),
          singletonRelationshipVariables = Set("r2" -> "r2"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should inline relationship local (start and end node) predicate into NFA: inside QPP") {
    val query = "MATCH ANY SHORTEST ((u:User)((n)-[r]->(m) WHERE n.prop > m.prop)+(v)-[r2]->(w)) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r WHERE startNode(r).prop > endNode(r).prop]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .addTransition(3, 4, "(v)-[r2]->(w)")
      .setFinalState(4)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`) WHERE `n`.prop > `m`.prop){1, } (v)-[r2]->(w) WHERE NOT r2 IN `r` AND unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v", "w" -> "w"),
          singletonRelationshipVariables = Set("r2" -> "r2"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should inline relationship local (start and end node) predicate into NFA: direction INCOMING") {
    val query = "MATCH ANY SHORTEST ((u:User)((n)-[r]->(m))+(v)<-[r2]-(w) WHERE v.prop > r2.prop + w.prop) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .addTransition(3, 4, "(v)<-[r2 WHERE endNode(r2).prop > r2.prop + startNode(r2).prop]-(w)")
      .setFinalState(4)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v)<-[r2]-(w) WHERE NOT r2 IN `r` AND unique(`r`) AND v.prop > r2.prop + w.prop)",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v", "w" -> "w"),
          singletonRelationshipVariables = Set("r2" -> "r2"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should inline relationship local (start and end node) predicate into NFA: from left") {
    val query = "MATCH ANY SHORTEST ((u)((n)-[r]->(m))+(v)<-[r2]-(w:User) WHERE v.prop > r2.prop + w.prop) RETURN *"

    val nfa = new TestNFABuilder(0, "w")
      .addTransition(0, 1, "(w)-[r2 WHERE endNode(r2).prop > r2.prop + startNode(r2).prop]->(v)")
      .addTransition(1, 2, "(v) (m)")
      .addTransition(2, 3, "(m)<-[r]-(n)")
      .addTransition(3, 2, "(n) (m)")
      .addTransition(3, 4, "(n) (u)")
      .setFinalState(4)
      .build()

    val plan = all_if_possible_planner.plan(query).stripProduceResults
    plan should equal(
      all_if_possible_planner.subPlanBuilder()
        .statefulShortestPath(
          "w",
          "u",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v)<-[r2]-(w) WHERE NOT r2 IN `r` AND unique(`r`) AND v.prop > r2.prop + w.prop)",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("u" -> "u", "v" -> "v"),
          singletonRelationshipVariables = Set("r2" -> "r2"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = true
        )
        .nodeByLabelScan("w", "User")
        .build()
    )
  }

  test("Should inline relationship local (start and end node) predicate into NFA: from left, direction INCOMING") {
    val query = "MATCH ANY SHORTEST ((u)((n)-[r]->(m))+(v)-[r2]->(w:User) WHERE v.prop > r2.prop + w.prop) RETURN *"

    val nfa = new TestNFABuilder(0, "w")
      .addTransition(0, 1, "(w)<-[r2 WHERE startNode(r2).prop > r2.prop + endNode(r2).prop]-(v)")
      .addTransition(1, 2, "(v) (m)")
      .addTransition(2, 3, "(m)<-[r]-(n)")
      .addTransition(3, 2, "(n) (m)")
      .addTransition(3, 4, "(n) (u)")
      .setFinalState(4)
      .build()

    val plan = all_if_possible_planner.plan(query).stripProduceResults
    plan should equal(
      all_if_possible_planner.subPlanBuilder()
        .statefulShortestPath(
          "w",
          "u",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v)-[r2]->(w) WHERE NOT r2 IN `r` AND unique(`r`) AND v.prop > r2.prop + w.prop)",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("u" -> "u", "v" -> "v"),
          singletonRelationshipVariables = Set("r2" -> "r2"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = true
        )
        .nodeByLabelScan("w", "User")
        .build()
    )
  }

  test("Should not inline relationship local (start and end node) predicate into NFA: direction BOTH") {
    val query = "MATCH ANY SHORTEST ((u:User)((n)-[r]->(m))+(v)-[r2]-(w) WHERE v.prop > r2.prop + w.prop) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .addTransition(3, 4, "(v)-[r2]-(w)")
      .setFinalState(4)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v)-[r2]-(w) WHERE NOT r2 IN `r` AND unique(`r`) AND v.prop > r2.prop + w.prop)",
          Some("v.prop > r2.prop + w.prop"),
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v", "w" -> "w"),
          singletonRelationshipVariables = Set("r2" -> "r2"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should not inline relationship local (start and end node) predicate into NFA: NestedPlanExpression") {
    // We compare "solvedExpressionAsString" nested inside NestedPlanExpressions.
    // This saves us from windows line break mismatches in those strings.
    implicit val windowsSafe: WindowsSafeAnyRef[LogicalPlan] = new WindowsSafeAnyRef[LogicalPlan]

    val query =
      "MATCH ANY SHORTEST ((u:User)((n)-[r]->(m))+(v)-[r2]->(w) WHERE EXISTS { (v)-->({prop: v.prop + r2.prop + w.prop}) } ) RETURN *"

    val expectedNestedPlan = planner.subPlanBuilder()
      .filter("anon_1.prop = v.prop + r2.prop + w.prop")
      .expandAll("(v)-[anon_0]->(anon_1)")
      .argument("w", "r2", "v")
      .build()

    val solvedNestedExpressionAsString =
      """EXISTS { MATCH (v)-[`anon_0`]->(`anon_1`)
        |  WHERE `anon_1`.prop IN [(v.prop + r2.prop) + w.prop] }""".stripMargin
    val nestedPlanExpression = NestedPlanExistsExpression(
      plan = expectedNestedPlan,
      solvedExpressionAsString =
        solvedNestedExpressionAsString
    )(pos)

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .addTransition(3, 4, "(v)-[r2]->(w)")
      .setFinalState(4)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPathExpr(
          "u",
          "w",
          s"SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v)-[r2]->(w) WHERE $solvedNestedExpressionAsString AND NOT r2 IN `r` AND unique(`r`))",
          Some(nestedPlanExpression),
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v", "w" -> "w"),
          singletonRelationshipVariables = Set("r2" -> "r2"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should inline relationship local (start and end node) predicate into NFA: DesugaredMapProjection") {
    val query =
      "MATCH ANY SHORTEST ((u:User)((n)-[r]->(m))+(v)-[r2]->(w) WHERE v{.foo,.bar} = w{.foo,.bar, baz: r2.baz}) RETURN *"

    val expr = equals(
      DesugaredMapProjection(
        StartNode(v"r2")(pos),
        Seq(
          LiteralEntry(propName("foo"), prop(StartNode(v"r2")(pos), "foo"))(pos),
          LiteralEntry(propName("bar"), prop(StartNode(v"r2")(pos), "bar"))(pos)
        ),
        includeAllProps = false
      )(pos),
      DesugaredMapProjection(
        EndNode(v"r2")(pos),
        Seq(
          LiteralEntry(propName("foo"), prop(EndNode(v"r2")(pos), "foo"))(pos),
          LiteralEntry(propName("bar"), prop(EndNode(v"r2")(pos), "bar"))(pos),
          LiteralEntry(propName("baz"), prop("r2", "baz"))(pos)
        ),
        includeAllProps = false
      )(pos)
    )

    val nfaPredicate = RelationshipExpansionPredicate(
      v"r2",
      Some(Expand.VariablePredicate(v"r2", expr)),
      Seq.empty,
      OUTGOING,
      None
    )

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .addTransition((3, "v"), (4, "w"), nfaPredicate)
      .setFinalState(4)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v)-[r2]->(w) WHERE NOT r2 IN `r` AND unique(`r`) AND v{foo: v.foo, bar: v.bar} = w{foo: w.foo, bar: w.bar, baz: r2.baz})",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v", "w" -> "w"),
          singletonRelationshipVariables = Set("r2" -> "r2"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test(
    "Should support a shortest path pattern with a predicate on several entities in different pattern parts inside a QPP"
  ) {
    val query = "MATCH ANY SHORTEST (u:User)(((n)-[r]->(c:B)-->(m)) WHERE n.prop <= m.prop)+ (v) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(c:B)")
      .addTransition(2, 3, "(c)-[anon_0]->(m)")
      .addTransition(3, 1, "(m) (n)")
      .addTransition(3, 4, "(m) (v)")
      .setFinalState(4)
      .build()

    val plan = planner.plan(query).stripProduceResults
    val expected = planner.subPlanBuilder()
      .statefulShortestPath(
        "u",
        "v",
        "SHORTEST 1 ((u) ((`n`)-[`r`]->(`c`)-[`anon_0`]->(`m`) WHERE `c`:B){1, } (v) WHERE NOT `anon_0` = `r` AND `n`.prop <= `m`.prop AND unique(`r` + `anon_7`))",
        Some("all(anon_1 IN range(0, size(m) - 1) WHERE (n[anon_1]).prop <= (m[anon_1]).prop)"),
        Set(("n", "n"), ("c", "c"), ("m", "m")),
        Set(("r", "r")),
        Set(("v", "v")),
        Set(),
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll
      )
      .nodeByLabelScan("u", "User")
      .build()
    plan should equal(expected)
  }

  test("Should plan shortest with dependencies to previous match in inlined relationship predicate") {
    val query = "MATCH (n) MATCH ANY SHORTEST (a:User)-[r WHERE r.prop = n.prop]->(b) RETURN *"

    val nfa = new TestNFABuilder(0, "a")
      .addTransition(0, 1, "(a)-[r WHERE r.prop = cacheN[n.prop]]->(b)")
      .setFinalState(1)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 ((a)-[r]->(b) WHERE r.prop = n.prop)",
          None,
          Set(),
          Set(),
          Set(("b", "b")),
          Set(("r", "r")),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .apply()
        .|.nodeByLabelScan("a", "User", "n")
        .cacheProperties("cacheNFromStore[n.prop]")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "Should plan shortest with dependencies to previous match in inlined node predicate"
  ) {
    val query = "MATCH (a) MATCH ANY SHORTEST ((u)((n WHERE a.prop = n.prop)-[r]->(m))+(v)) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n WHERE cacheN[a.prop] = cacheNFromStore[n.prop])")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n WHERE cacheN[a.prop] = cacheNFromStore[n.prop])")
      .addTransition(2, 3, "(m) (v)")
      .setFinalState(3)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`) WHERE a.prop = `n`.prop){1, } (v) WHERE unique(`r`))",
          None,
          Set(("n", "n"), ("m", "m")),
          Set(("r", "r")),
          Set(("v", "v")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .filter("cacheN[a.prop] = u.prop")
        .apply()
        .|.allNodeScan("u", "a")
        .cacheProperties("cacheNFromStore[a.prop]")
        .allNodeScan("a")
        .build()
    )
  }

  test(
    "should plan SHORTEST with predicate depending on no path variables as a filter inside the QPP"
  ) {
    val query = "MATCH ANY SHORTEST ((u:User)((n)-[r]->(m) WHERE $param)+(v)) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .setFinalState(3)
      .build()

    val plan = all_if_possible_planner.plan(query).stripProduceResults
    plan should equal(
      all_if_possible_planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v) WHERE $param AND unique(`r`))",
          Some("all(anon_0 IN range(0, size(m) - 1) WHERE $param)"),
          Set(("n", "n"), ("m", "m")),
          Set(("r", "r")),
          Set(("v", "v")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test(
    "should plan SHORTEST with node property comparison with parameter inlined in NFA"
  ) {
    val query = "MATCH ANY SHORTEST ((u:User)((n)-[r]->(m) WHERE m.prop = $param)+(v)) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m WHERE m.prop = $param)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v WHERE v.prop = $param)")
      .setFinalState(3)
      .build()

    val plan = all_if_possible_planner.plan(query).stripProduceResults
    plan should equal(
      all_if_possible_planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`) WHERE `m`.prop IN [$param]){1, } (v) WHERE unique(`r`) AND v.prop IN [$param])",
          None,
          Set(("n", "n"), ("m", "m")),
          Set(("r", "r")),
          Set(("v", "v")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test(
    "should plan SHORTEST with node property comparison with variable from previous MATCH inlined in NFA"
  ) {
    val query =
      """MATCH (foo)
        |MATCH ANY SHORTEST ((u:User)((n)-[r]->(m) WHERE m.prop = foo.prop)+(v))
        |RETURN *""".stripMargin

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m WHERE m.prop = cacheN[foo.prop])")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v WHERE v.prop = cacheN[foo.prop])")
      .setFinalState(3)
      .build()

    val plan = all_if_possible_planner.plan(query).stripProduceResults
    plan should equal(
      all_if_possible_planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`) WHERE `m`.prop = foo.prop){1, } (v) WHERE unique(`r`) AND v.prop = foo.prop)",
          None,
          Set(("n", "n"), ("m", "m")),
          Set(("r", "r")),
          Set(("v", "v")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .apply()
        .|.nodeByLabelScan("u", "User", "foo")
        .cacheProperties("cacheNFromStore[foo.prop]")
        .allNodeScan("foo")
        .build()
    )
  }

  test(
    "With statefulShortestPlanningMode=into_only should produce an Into plan even if boundary nodes are not previously bound"
  ) {
    val planner = plannerBase
      .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, INTO_ONLY)
      .build()

    val query = "MATCH ANY SHORTEST (u:User)((n)-[r]->(m))+(v) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .setFinalState(3)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v) WHERE unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set(),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandInto
        )
        .cartesianProduct()
        .|.allNodeScan("v")
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("With statefulShortestPlanningMode=into_only should plan Into if both start and end are already bound") {
    val planner = plannerBase
      .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, INTO_ONLY)
      .build()

    val query =
      s"""
         |MATCH (n:User), (m)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST (n)((n_inner)-[r_inner]->(m_inner))+ (m)
         |RETURN *
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val nfa = new TestNFABuilder(0, "n")
      .addTransition(0, 1, "(n) (n_inner)")
      .addTransition(1, 2, "(n_inner)-[r_inner]->(m_inner)")
      .addTransition(2, 1, "(m_inner) (n_inner)")
      .addTransition(2, 3, "(m_inner) (m)")
      .setFinalState(3)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "n",
          "m",
          "SHORTEST 1 ((n) ((`n_inner`)-[`r_inner`]->(`m_inner`)){1, } (m) WHERE unique(`r_inner`))",
          None,
          groupNodes = Set(("n_inner", "n_inner"), ("m_inner", "m_inner")),
          groupRelationships = Set(("r_inner", "r_inner")),
          singletonNodeVariables = Set(),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandInto
        )
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("m")
        .nodeByLabelScan("n", "User")
        .build()
    )
  }

  test(
    "With statefulShortestPlanningMode=all_if_possible should produce an All plan if boundary nodes are not previously bound"
  ) {
    val query = "MATCH ANY SHORTEST (u:User)((n)-[r]->(m))+(v) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .setFinalState(3)
      .build()

    val plan = all_if_possible_planner.plan(query).stripProduceResults
    plan should equal(
      all_if_possible_planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((`n`)-[`r`]->(`m`)){1, } (v) WHERE unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("With statefulShortestPlanningMode=all_if_possible should plan Into if both start and end are already bound") {
    val query =
      s"""
         |MATCH (n:User), (m)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST (n)((n_inner)-[r_inner]->(m_inner))+ (m)
         |RETURN *
         |""".stripMargin

    val plan = all_if_possible_planner.plan(query).stripProduceResults
    val nfa = new TestNFABuilder(0, "n")
      .addTransition(0, 1, "(n) (n_inner)")
      .addTransition(1, 2, "(n_inner)-[r_inner]->(m_inner)")
      .addTransition(2, 1, "(m_inner) (n_inner)")
      .addTransition(2, 3, "(m_inner) (m)")
      .setFinalState(3)
      .build()

    plan should equal(
      all_if_possible_planner.subPlanBuilder()
        .statefulShortestPath(
          "n",
          "m",
          "SHORTEST 1 ((n) ((`n_inner`)-[`r_inner`]->(`m_inner`)){1, } (m) WHERE unique(`r_inner`))",
          None,
          groupNodes = Set(("n_inner", "n_inner"), ("m_inner", "m_inner")),
          groupRelationships = Set(("r_inner", "r_inner")),
          singletonNodeVariables = Set(),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandInto
        )
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("m")
        .nodeByLabelScan("n", "User")
        .build()
    )
  }

  test("long pattern with lots of anonymous variables") {
    val query =
      s"""
         |MATCH SHORTEST 1
         |  (u:User {prop: 5})<-[:R]-
         |  (:B)-[:R]->
         |  (:B)-[:R]->+()
         |  (
         |    (sx1:B)<-[:R]-
         |    (:B)-[:R]->
         |    (x:B)<-[:R]-
         |    (:B)-[:R]->
         |    (sx2:B)
         |    WHERE  sx1.prop + sx2.prop <= 60
         |  ){0,1}
         |  ()-[:R]->*
         |  (:B)<-[:R]-
         |  (:B)-[:R]->
         |  (end:B)
         |RETURN *
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u)<-[anon_22:R]-(anon_23 WHERE anon_23:B)")
      .addTransition(1, 2, "(anon_23)-[anon_24:R]->(anon_25 WHERE anon_25:B)")
      .addTransition(2, 3, "(anon_25) (anon_10)")
      .addTransition(3, 4, "(anon_10)-[anon_11:R]->(anon_12)")
      .addTransition(4, 3, "(anon_12) (anon_10)")
      .addTransition(4, 5, "(anon_12) (anon_26)")
      .addTransition(5, 6, "(anon_26) (sx1 WHERE sx1:B)")
      .addTransition(5, 11, "(anon_26) (anon_27)")
      .addTransition(6, 7, "(sx1)<-[anon_13:R]-(anon_14 WHERE anon_14:B)")
      .addTransition(7, 8, "(anon_14)-[anon_15:R]->(x WHERE x:B)")
      .addTransition(8, 9, "(x)<-[anon_16:R]-(anon_17 WHERE anon_17:B)")
      .addTransition(9, 10, "(anon_17)-[anon_18:R]->(sx2 WHERE sx2:B)")
      .addTransition(10, 11, "(sx2) (anon_27)")
      .addTransition(11, 12, "(anon_27) (anon_19)")
      .addTransition(11, 14, "(anon_27) (anon_28 WHERE anon_28:B)")
      .addTransition(12, 13, "(anon_19)-[anon_20:R]->(anon_21)")
      .addTransition(13, 12, "(anon_21) (anon_19)")
      .addTransition(13, 14, "(anon_21) (anon_28 WHERE anon_28:B)")
      .addTransition(14, 15, "(anon_28)<-[anon_29:R]-(anon_30 WHERE anon_30:B)")
      .addTransition(15, 16, "(anon_30)-[anon_31:R]->(end WHERE end:B)")
      .setFinalState(16)
      .build()

    plan should equal(planner.subPlanBuilder()
      .statefulShortestPath(
        "u",
        "end",
        "SHORTEST 1 ((u)<-[`anon_0`:R]-(`anon_1`)-[`anon_2`:R]->(`anon_3`) ((`anon_10`)-[`anon_11`:R]->(`anon_12`)){1, } (`anon_4`) ((`sx1`)<-[`anon_13`:R]-(`anon_14`)-[`anon_15`:R]->(`x`)<-[`anon_16`:R]-(`anon_17`)-[`anon_18`:R]->(`sx2`) WHERE `sx1`:B AND `anon_17`:B AND `x`:B AND `sx2`:B AND `anon_14`:B){0, 1} (`anon_5`) ((`anon_19`)-[`anon_20`:R]->(`anon_21`)){0, } (`anon_6`)<-[`anon_7`:R]-(`anon_8`)-[`anon_9`:R]->(end) WHERE NOT `anon_0` = `anon_7` AND NOT `anon_0` = `anon_9` AND NOT `anon_0` IN ((`anon_37` + `anon_38`) + `anon_42`) + `anon_40` AND NOT `anon_0` IN `anon_26` AND NOT `anon_0` IN `anon_50` AND NOT `anon_7` IN ((`anon_37` + `anon_38`) + `anon_42`) + `anon_40` AND NOT `anon_7` IN `anon_26` AND NOT `anon_7` IN `anon_50` AND NOT `anon_9` = `anon_7` AND NOT `anon_9` IN ((`anon_37` + `anon_38`) + `anon_42`) + `anon_40` AND NOT `anon_9` IN `anon_26` AND NOT `anon_9` IN `anon_50` AND NOT `anon_2` = `anon_0` AND NOT `anon_2` = `anon_7` AND NOT `anon_2` = `anon_9` AND NOT `anon_2` IN ((`anon_37` + `anon_38`) + `anon_42`) + `anon_40` AND NOT `anon_2` IN `anon_26` AND NOT `anon_2` IN `anon_50` AND NOT `anon_15` = `anon_13` AND NOT `anon_16` = `anon_13` AND NOT `anon_16` = `anon_15` AND NOT `anon_18` = `anon_13` AND NOT `anon_18` = `anon_15` AND NOT `anon_18` = `anon_16` AND `anon_6`:B AND `anon_1`:B AND `anon_8`:B AND `anon_3`:B AND `sx1`.prop + `sx2`.prop <= 60 AND disjoint(((`anon_37` + `anon_38`) + `anon_42`) + `anon_40`, `anon_50`) AND disjoint(`anon_26`, ((`anon_37` + `anon_38`) + `anon_42`) + `anon_40`) AND disjoint(`anon_26`, `anon_50`) AND end:B AND unique(((`anon_37` + `anon_38`) + `anon_42`) + `anon_40`) AND unique(`anon_26`) AND unique(`anon_50`))",
        Some("all(anon_32 IN range(0, size(sx1) - 1) WHERE (sx1[anon_32]).prop + (sx2[anon_32]).prop <= 60)"),
        Set(("sx1", "sx1"), ("x", "x"), ("sx2", "sx2")),
        Set(),
        Set(
          ("anon_28", "anon_6"),
          ("anon_30", "anon_8"),
          ("anon_25", "anon_3"),
          ("anon_23", "anon_1"),
          ("anon_26", "anon_4"),
          ("anon_27", "anon_5"),
          ("end", "end")
        ),
        Set(("anon_22", "anon_0"), ("anon_24", "anon_2"), ("anon_29", "anon_7"), ("anon_31", "anon_9")),
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll
      )
      .nodeIndexOperator("u:User(prop = 5)")
      .build())
  }

  test("long pattern and everything is named") {
    val query =
      s"""
         |MATCH SHORTEST 1
         |  (u:User {prop: 5})<-[r1:R]-
         |  (b1:B)-[r2:R]->
         |  (b2:B) ((b3)-[r3:R]->(b4))+ (b5)
         |  (
         |    (sx1:B)<-[r4:R]-
         |    (b6:B)-[r5:R]->
         |    (x:B)<-[r6:R]-
         |    (b7:B)-[r7:R]->
         |    (sx2:B)
         |    WHERE  sx1.prop + sx2.prop <= 60
         |  ){0,1}
         |  (b8) ((b9)-[r8:R]->(b10))*
         |  (b11:B)<-[r9:R]-
         |  (b12:B)-[r10:R]->
         |  (end:B)
         |RETURN *
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u)<-[r1:R]-(b1 WHERE b1:B)")
      .addTransition(1, 2, "(b1)-[r2:R]->(b2 WHERE b2:B)")
      .addTransition(2, 3, "(b2) (b3)")
      .addTransition(3, 4, "(b3)-[r3:R]->(b4)")
      .addTransition(4, 3, "(b4) (b3)")
      .addTransition(4, 5, "(b4) (b5)")
      .addTransition(5, 6, "(b5) (sx1 WHERE sx1:B)")
      .addTransition(5, 11, "(b5) (b8)")
      .addTransition(6, 7, "(sx1)<-[r4:R]-(b6 WHERE b6:B)")
      .addTransition(7, 8, "(b6)-[r5:R]->(x WHERE x:B)")
      .addTransition(8, 9, "(x)<-[r6:R]-(b7 WHERE b7:B)")
      .addTransition(9, 10, "(b7)-[r7:R]->(sx2 WHERE sx2:B)")
      .addTransition(10, 11, "(sx2) (b8)")
      .addTransition(11, 12, "(b8) (b9)")
      .addTransition(11, 14, "(b8) (b11 WHERE b11:B)")
      .addTransition(12, 13, "(b9)-[r8:R]->(b10)")
      .addTransition(13, 12, "(b10) (b9)")
      .addTransition(13, 14, "(b10) (b11 WHERE b11:B)")
      .addTransition(14, 15, "(b11)<-[r9:R]-(b12 WHERE b12:B)")
      .addTransition(15, 16, "(b12)-[r10:R]->(end WHERE end:B)")
      .setFinalState(16)
      .build()

    plan should equal(planner.subPlanBuilder()
      .statefulShortestPath(
        "u",
        "end",
        "SHORTEST 1 ((u)<-[r1:R]-(b1)-[r2:R]->(b2) ((`b3`)-[`r3`:R]->(`b4`)){1, } (b5) ((`sx1`)<-[`r4`:R]-(`b6`)-[`r5`:R]->(`x`)<-[`r6`:R]-(`b7`)-[`r7`:R]->(`sx2`) WHERE `sx1`:B AND `b6`:B AND `x`:B AND `b7`:B AND `sx2`:B){0, 1} (b8) ((`b9`)-[`r8`:R]->(`b10`)){0, } (b11)<-[r9:R]-(b12)-[r10:R]->(end) WHERE NOT `r5` = `r4` AND NOT `r6` = `r4` AND NOT `r6` = `r5` AND NOT `r7` = `r4` AND NOT `r7` = `r5` AND NOT `r7` = `r6` AND NOT r1 = r10 AND NOT r1 = r9 AND NOT r1 IN ((`r4` + `r5`) + `r6`) + `r7` AND NOT r1 IN `r3` AND NOT r1 IN `r8` AND NOT r10 = r9 AND NOT r10 IN ((`r4` + `r5`) + `r6`) + `r7` AND NOT r10 IN `r3` AND NOT r10 IN `r8` AND NOT r2 = r1 AND NOT r2 = r10 AND NOT r2 = r9 AND NOT r2 IN ((`r4` + `r5`) + `r6`) + `r7` AND NOT r2 IN `r3` AND NOT r2 IN `r8` AND NOT r9 IN ((`r4` + `r5`) + `r6`) + `r7` AND NOT r9 IN `r3` AND NOT r9 IN `r8` AND `sx1`.prop + `sx2`.prop <= 60 AND b11:B AND b12:B AND b1:B AND b2:B AND disjoint(((`r4` + `r5`) + `r6`) + `r7`, `r8`) AND disjoint(`r3`, ((`r4` + `r5`) + `r6`) + `r7`) AND disjoint(`r3`, `r8`) AND end:B AND unique(((`r4` + `r5`) + `r6`) + `r7`) AND unique(`r3`) AND unique(`r8`))",
        Some("all(anon_0 IN range(0, size(sx1) - 1) WHERE (sx1[anon_0]).prop + (sx2[anon_0]).prop <= 60)"),
        Set(
          ("b10", "b10"),
          ("b7", "b7"),
          ("b3", "b3"),
          ("sx1", "sx1"),
          ("b6", "b6"),
          ("sx2", "sx2"),
          ("x", "x"),
          ("b4", "b4"),
          ("b9", "b9")
        ),
        Set(("r4", "r4"), ("r6", "r6"), ("r7", "r7"), ("r5", "r5"), ("r8", "r8"), ("r3", "r3")),
        Set(("b1", "b1"), ("b11", "b11"), ("b2", "b2"), ("b8", "b8"), ("b12", "b12"), ("b5", "b5"), ("end", "end")),
        Set(("r1", "r1"), ("r2", "r2"), ("r9", "r9"), ("r10", "r10")),
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll
      )
      .nodeIndexOperator("u:User(prop = 5)")
      .build())
  }

  private val plannerBaseAB = plannerBuilder()
    .setAllNodesCardinality(100)
    .setAllRelationshipsCardinality(500)
    .setLabelCardinality("A", 10)
    .setLabelCardinality("B", 30)
    .setRelationshipCardinality("(:A)-[]->()", 500)
    .setRelationshipCardinality("(:A)-[]->(:B)", 500)
    .setRelationshipCardinality("()-[]->(:B)", 500)
    .setRelationshipCardinality("(:B)-[]->()", 500)
    .addSemanticFeature(SemanticFeature.GpmShortestPath)

  test("may plan post-filter on boundary node as pre-filter") {
    val query =
      """MATCH ANY SHORTEST (a:A)((n)-[r]->(m))+(b:B)
        |  WHERE b.prop > 0 // post-filter
        |RETURN *""".stripMargin

    val planner = plannerBaseAB
      .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, ALL_IF_POSSIBLE)
      .build()

    val nfa = new TestNFABuilder(0, "a")
      .addTransition(0, 1, "(a) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (b WHERE b:B AND b.prop > 0)")
      .setFinalState(3)
      .build()

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "b", "m", "n", "r")
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 ((a) ((`n`)-[`r`]->(`m`)){1, } (b) WHERE b.prop > 0 AND b:B AND unique(`r`))",
          None,
          Set(("n", "n"), ("m", "m")),
          Set(("r", "r")),
          Set(("b", "b")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test("should plan post-filter as such") {
    val query =
      """MATCH ANY SHORTEST (a:A)((n)-[r]->(m))+(b:B)((o)-[s]->(p))+(c)
        |  WHERE b.prop > 0 // post-filter
        |RETURN *""".stripMargin

    val planner = plannerBaseAB
      .build()

    val nfa = new TestNFABuilder(0, "a")
      .addTransition(0, 1, "(a) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (b WHERE b:B)")
      .addTransition(3, 4, "(b) (o)")
      .addTransition(4, 5, "(o)-[s]->(p)")
      .addTransition(5, 4, "(p) (o)")
      .addTransition(5, 6, "(p) (c)")
      .setFinalState(6)
      .build()

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "b", "c", "m", "n", "o", "p", "r", "s")
        .filter("b.prop > 0")
        .statefulShortestPath(
          "a",
          "c",
          "SHORTEST 1 ((a) ((`n`)-[`r`]->(`m`)){1, } (b) ((`o`)-[`s`]->(`p`)){1, } (c) WHERE b:B AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
          None,
          Set(("n", "n"), ("m", "m"), ("o", "o"), ("p", "p")),
          Set(("r", "r"), ("s", "s")),
          Set(("b", "b"), ("c", "c")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test("should plan post-filter on quantified part as such") {
    val query =
      """MATCH ANY SHORTEST (a:A)((n)-[r]->(m))+(b:B)
        |  WHERE size(r) > 5 // post-filter
        |RETURN *""".stripMargin

    val planner = plannerBaseAB
      .build()

    val nfa = new TestNFABuilder(0, "a")
      .addTransition(0, 1, "(a) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (b WHERE b:B)")
      .setFinalState(3)
      .build()

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "b", "m", "n", "r")
        .filter("size(r) > 5")
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 ((a) ((`n`)-[`r`]->(`m`)){1, } (b) WHERE b:B AND unique(`r`))",
          None,
          Set(("n", "n"), ("m", "m")),
          Set(("r", "r")),
          Set(("b", "b")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeByLabelScan("a", "A")
        .build()
    )
  }

  test(
    "With statefulShortestPlanningMode=cardinality_heuristic should plan SHORTEST Into/All depending on boundary nodes cardinalities"
  ) {
    val query = "MATCH ANY SHORTEST (a:A)((n)-[r]->(m))+(b:B) RETURN *"

    // If both endpoints have cardinality > 1
    {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(500)
        .setLabelCardinality("A", 10)
        .setLabelCardinality("B", 30)
        .setRelationshipCardinality("(:A)-[]->()", 500)
        .setRelationshipCardinality("(:A)-[]->(:B)", 500)
        .setRelationshipCardinality("()-[]->(:B)", 500)
        .addSemanticFeature(SemanticFeature.GpmShortestPath)
        .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
        .build()

      planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 ((a) ((`n`)-[`r`]->(`m`)){1, } (b) WHERE b:B AND unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("b" -> "b"),
          singletonRelationshipVariables = Set(),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "a")
            .addTransition(0, 1, "(a) (n)")
            .addTransition(1, 2, "(n)-[r]->(m)")
            .addTransition(2, 1, "(m) (n)")
            .addTransition(2, 3, "(m) (b:B)")
            .setFinalState(3)
            .build(),
          ExpandAll
        )
        .nodeByLabelScan("a", "A")
        .build())
    }

    // If one endpoint has cardinality > 1
    {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(500)
        .setLabelCardinality("A", 1)
        .setLabelCardinality("B", 30)
        .setRelationshipCardinality("(:A)-[]->()", 500)
        .setRelationshipCardinality("(:A)-[]->(:B)", 500)
        .setRelationshipCardinality("()-[]->(:B)", 500)
        .addSemanticFeature(SemanticFeature.GpmShortestPath)
        .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
        .build()

      planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 ((a) ((`n`)-[`r`]->(`m`)){1, } (b) WHERE b:B AND unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("b" -> "b"),
          singletonRelationshipVariables = Set(),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "a")
            .addTransition(0, 1, "(a) (n)")
            .addTransition(1, 2, "(n)-[r]->(m)")
            .addTransition(2, 1, "(m) (n)")
            .addTransition(2, 3, "(m) (b:B)")
            .setFinalState(3)
            .build(),
          ExpandAll
        )
        .nodeByLabelScan("a", "A")
        .build())
    }

    // If endpoints have cardinality <= 1
    {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(500)
        .setLabelCardinality("A", 1)
        .setLabelCardinality("B", 1)
        .setRelationshipCardinality("(:A)-[]->()", 40)
        .setRelationshipCardinality("(:A)-[]->(:B)", 40)
        .setRelationshipCardinality("()-[]->(:B)", 40)
        .addSemanticFeature(SemanticFeature.GpmShortestPath)
        .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
        .build()

      planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 ((a) ((`n`)-[`r`]->(`m`)){1, } (b) WHERE unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set(),
          singletonRelationshipVariables = Set(),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "a")
            .addTransition(0, 1, "(a) (n)")
            .addTransition(1, 2, "(n)-[r]->(m)")
            .addTransition(2, 1, "(m) (n)")
            .addTransition(2, 3, "(m) (b)")
            .setFinalState(3)
            .build(),
          ExpandInto
        )
        .cartesianProduct()
        .|.nodeByLabelScan("b", "B")
        .nodeByLabelScan("a", "A")
        .build())
    }
  }

  test(
    "With statefulShortestPlanningMode=cardinality_heuristic should plan SHORTEST Into/All depending on boundary nodes cardinalities, using a unique index"
  ) {
    val query = "MATCH ANY SHORTEST (a:A {p:1})((n)-[r]->(m))+(b:B {p:1}) RETURN *"

    // If both endpoints have cardinality > 1
    {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(500)
        .setLabelCardinality("A", 10)
        .addNodeIndex("A", Seq("p"), 1.0, 1 / 5d)
        .setLabelCardinality("B", 30)
        .addNodeIndex("B", Seq("p"), 1.0, 1 / 5d)
        .setRelationshipCardinality("(:A)-[]->()", 500)
        .setRelationshipCardinality("(:A)-[]->(:B)", 500)
        .setRelationshipCardinality("()-[]->(:B)", 500)
        .addSemanticFeature(SemanticFeature.GpmShortestPath)
        .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
        .build()

      planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 ((a) ((`n`)-[`r`]->(`m`)){1, } (b) WHERE b.p IN [1] AND b:B AND unique(`r`))",
          None,
          Set(("n", "n"), ("m", "m")),
          Set(("r", "r")),
          Set(("b", "b")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "a")
            .addTransition(0, 1, "(a) (n)")
            .addTransition(1, 2, "(n)-[r]->(m)")
            .addTransition(2, 1, "(m) (n)")
            .addTransition(2, 3, "(m) (b WHERE b.p = 1 AND b:B)")
            .setFinalState(3)
            .build(),
          ExpandAll
        )
        .nodeIndexOperator("a:A(p = 1)")
        .build())
    }

    // If endpoints have cardinality <= 1
    {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(500)
        .setLabelCardinality("A", 10)
        .addNodeIndex("A", Seq("p"), 1.0, 1 / 10d, isUnique = true)
        .setLabelCardinality("B", 30)
        .addNodeIndex("B", Seq("p"), 1.0, 1 / 30d, isUnique = true)
        .setRelationshipCardinality("(:A)-[]->()", 40)
        .setRelationshipCardinality("(:A)-[]->(:B)", 40)
        .setRelationshipCardinality("()-[]->(:B)", 40)
        .addSemanticFeature(SemanticFeature.GpmShortestPath)
        .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
        .build()

      planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 ((a) ((`n`)-[`r`]->(`m`)){1, } (b) WHERE unique(`r`))",
          None,
          Set(("n", "n"), ("m", "m")),
          Set(("r", "r")),
          Set(),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "a")
            .addTransition(0, 1, "(a) (n)")
            .addTransition(1, 2, "(n)-[r]->(m)")
            .addTransition(2, 1, "(m) (n)")
            .addTransition(2, 3, "(m) (b)")
            .setFinalState(3)
            .build(),
          ExpandInto
        )
        .cartesianProduct()
        .|.nodeIndexOperator("b:B(p = 1)", unique = true)
        .nodeIndexOperator("a:A(p = 1)", unique = true)
        .build())(SymmetricalLogicalPlanEquality)
    }
  }

  test(
    "With statefulShortestPlanningMode=cardinality_heuristic should plan SHORTEST Into/All depending on boundary nodes cardinalities, with SHORTEST 10 GROUPS"
  ) {
    val query = "MATCH SHORTEST 10 GROUPS (a:A)((n)-[r]->(m))+(b:B) RETURN *"

    // If both endpoints have cardinality > 1
    {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(500)
        .setLabelCardinality("A", 10)
        .setLabelCardinality("B", 30)
        .setRelationshipCardinality("(:A)-[]->()", 500)
        .setRelationshipCardinality("(:A)-[]->(:B)", 500)
        .setRelationshipCardinality("()-[]->(:B)", 500)
        .addSemanticFeature(SemanticFeature.GpmShortestPath)
        .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
        .build()

      planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 10 GROUPS ((a) ((`n`)-[`r`]->(`m`)){1, } (b) WHERE b:B AND unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("b" -> "b"),
          singletonRelationshipVariables = Set(),
          StatefulShortestPath.Selector.ShortestGroups(10),
          new TestNFABuilder(0, "a")
            .addTransition(0, 1, "(a) (n)")
            .addTransition(1, 2, "(n)-[r]->(m)")
            .addTransition(2, 1, "(m) (n)")
            .addTransition(2, 3, "(m) (b:B)")
            .setFinalState(3)
            .build(),
          ExpandAll
        )
        .nodeByLabelScan("a", "A")
        .build())
    }

    // If endpoints have cardinality <= 1
    {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(500)
        .setLabelCardinality("A", 1)
        .setLabelCardinality("B", 1)
        .setRelationshipCardinality("(:A)-[]->()", 40)
        .setRelationshipCardinality("(:A)-[]->(:B)", 40)
        .setRelationshipCardinality("()-[]->(:B)", 40)
        .addSemanticFeature(SemanticFeature.GpmShortestPath)
        .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
        .build()

      planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 10 GROUPS ((a) ((`n`)-[`r`]->(`m`)){1, } (b) WHERE unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set(),
          singletonRelationshipVariables = Set(),
          StatefulShortestPath.Selector.ShortestGroups(10),
          new TestNFABuilder(0, "a")
            .addTransition(0, 1, "(a) (n)")
            .addTransition(1, 2, "(n)-[r]->(m)")
            .addTransition(2, 1, "(m) (n)")
            .addTransition(2, 3, "(m) (b)")
            .setFinalState(3)
            .build(),
          ExpandInto
        )
        .cartesianProduct()
        .|.nodeByLabelScan("b", "B")
        .nodeByLabelScan("a", "A")
        .build())
    }
  }

  test(
    "With statefulShortestPlanningMode=cardinality_heuristic should adhere to USING JOIN hint"
  ) {
    val query = "MATCH ANY SHORTEST (a:A)((n)-[r]->(m))+(b:B) USING JOIN ON a RETURN *"

    // If both endpoints have cardinality > 1
    {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(500)
        .setLabelCardinality("A", 10)
        .setLabelCardinality("B", 30)
        .setRelationshipCardinality("(:A)-[]->()", 500)
        .setRelationshipCardinality("(:A)-[]->(:B)", 500)
        .setRelationshipCardinality("()-[]->(:B)", 500)
        .addSemanticFeature(SemanticFeature.GpmShortestPath)
        .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
        .build()

      val plan = planner.plan(query).stripProduceResults
      plan shouldBe a[NodeHashJoin]
      plan.folder.treeFind[StatefulShortestPath] {
        case ssp: StatefulShortestPath if ssp.mode == ExpandInto => true
      } should be(empty)
    }

    // If endpoints have cardinality <= 1
    {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(500)
        .setLabelCardinality("A", 1)
        .setLabelCardinality("B", 1)
        .setRelationshipCardinality("(:A)-[]->()", 40)
        .setRelationshipCardinality("(:A)-[]->(:B)", 40)
        .setRelationshipCardinality("()-[]->(:B)", 40)
        .addSemanticFeature(SemanticFeature.GpmShortestPath)
        .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
        .build()

      // The JOIN hint weighs stronger than the requirement to use ExpandInto,
      // even if that would be preferable cardinality-wise.
      val plan = planner.plan(query).stripProduceResults
      plan shouldBe a[NodeHashJoin]
      plan.folder.treeFind[StatefulShortestPath] {
        case ssp: StatefulShortestPath if ssp.mode == ExpandInto => true
      } should be(empty)
    }
  }

  test(
    "With statefulShortestPlanningMode=cardinality_heuristic should plan SHORTEST Into/All depending on boundary nodes cardinalities, in tail query"
  ) {
    val query =
      """
        |MATCH (foo:Foo)
        |WITH * SKIP 0
        |MATCH ANY SHORTEST (a:A)((n)-[r]->(m))+(b:B)
        |RETURN *
        |""".stripMargin

    // If endpoints have cardinality > 1
    {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(500)
        .setLabelCardinality("A", 10)
        .setLabelCardinality("B", 30)
        .setLabelCardinality("Foo", 10)
        .setRelationshipCardinality("(:A)-[]->()", 500)
        .setRelationshipCardinality("(:A)-[]->(:B)", 500)
        .setRelationshipCardinality("()-[]->(:B)", 500)
        .addSemanticFeature(SemanticFeature.GpmShortestPath)
        .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
        .build()

      planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 ((a) ((`n`)-[`r`]->(`m`)){1, } (b) WHERE b:B AND unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("b" -> "b"),
          singletonRelationshipVariables = Set(),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "a")
            .addTransition(0, 1, "(a) (n)")
            .addTransition(1, 2, "(n)-[r]->(m)")
            .addTransition(2, 1, "(m) (n)")
            .addTransition(2, 3, "(m) (b:B)")
            .setFinalState(3)
            .build(),
          ExpandAll
        )
        .apply()
        .|.nodeByLabelScan("a", "A", "foo")
        .skip(0)
        .nodeByLabelScan("foo", "Foo")
        .build())
    }

    // If endpoints have cardinality <= 1
    {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(500)
        .setLabelCardinality("A", 1)
        .setLabelCardinality("B", 1)
        .setLabelCardinality("Foo", 10)
        .setRelationshipCardinality("(:A)-[]->()", 40)
        .setRelationshipCardinality("(:A)-[]->(:B)", 40)
        .setRelationshipCardinality("()-[]->(:B)", 40)
        .addSemanticFeature(SemanticFeature.GpmShortestPath)
        .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
        .build()

      planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 ((a) ((`n`)-[`r`]->(`m`)){1, } (b) WHERE unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set(),
          singletonRelationshipVariables = Set(),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "a")
            .addTransition(0, 1, "(a) (n)")
            .addTransition(1, 2, "(n)-[r]->(m)")
            .addTransition(2, 1, "(m) (n)")
            .addTransition(2, 3, "(m) (b)")
            .setFinalState(3)
            .build(),
          ExpandInto
        )
        .apply()
        .|.cartesianProduct()
        .|.|.nodeByLabelScan("b", "B", "foo")
        .|.nodeByLabelScan("a", "A", "foo")
        .skip(0)
        .nodeByLabelScan("foo", "Foo")
        .build())
    }
  }

  test(
    "With statefulShortestPlanningMode=cardinality_heuristic should plan SHORTEST Into/All depending on boundary nodes cardinalities, if part of a larger connected component"
  ) {
    val query =
      """
        |MATCH (aa:AA)-[r1]->(a:A), (b:B)<-[r2]-(bb:BB)
        |MATCH ANY SHORTEST (a:A)((n)-[r]-(m))+(b:B)
        |RETURN *
        |""".stripMargin

    // If endpoints have cardinality > 1
    {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(500)
        .setLabelCardinality("A", 10)
        .setLabelCardinality("B", 10)
        .setLabelCardinality("AA", 5)
        .setLabelCardinality("BB", 15)
        .setLabelCardinality("Foo", 10)
        .setRelationshipCardinality("(:A)-[]->(:B)", 10)
        .setRelationshipCardinality("(:B)-[]->(:A)", 10)
        .setRelationshipCardinality("(:A)-[]->()", 10)
        .setRelationshipCardinality("(:B)-[]->()", 10)
        .setRelationshipCardinality("()-[]->(:A)", 10)
        .setRelationshipCardinality("()-[]->(:B)", 10)
        .setRelationshipCardinality("(:BB)-[]->()", 10)
        .setRelationshipCardinality("(:BB)-[]->(:B)", 10)
        .setRelationshipCardinality("(:AA)-[]->()", 10)
        .setRelationshipCardinality("(:AA)-[]->(:A)", 10)
        .addSemanticFeature(SemanticFeature.GpmShortestPath)
        .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
        .build()

      planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
        .filter("NOT r1 = r2", "bb:BB")
        .expand("(b)<-[r2]-(bb)")
        .statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 ((a) ((`n`)-[`r`]-(`m`)){1, } (b) WHERE b:B AND unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("b" -> "b"),
          singletonRelationshipVariables = Set(),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "a")
            .addTransition(0, 1, "(a) (n)")
            .addTransition(1, 2, "(n)-[r]-(m)")
            .addTransition(2, 1, "(m) (n)")
            .addTransition(2, 3, "(m) (b:B)")
            .setFinalState(3)
            .build(),
          ExpandAll
        )
        .filter("a:A")
        .expand("(aa)-[r1]->(a)")
        .nodeByLabelScan("aa", "AA")
        .build())
    }

    // If endpoints have cardinality <= 1
    {
      val planner = plannerBuilder()
        .setAllNodesCardinality(100)
        .setAllRelationshipsCardinality(500)
        .setLabelCardinality("A", 1)
        .setLabelCardinality("B", 1)
        .setLabelCardinality("AA", 5)
        .setLabelCardinality("BB", 15)
        .setLabelCardinality("Foo", 10)
        .setRelationshipCardinality("(:A)-[]->(:B)", 10)
        .setRelationshipCardinality("(:B)-[]->(:A)", 10)
        .setRelationshipCardinality("(:A)-[]->()", 10)
        .setRelationshipCardinality("(:B)-[]->()", 10)
        .setRelationshipCardinality("()-[]->(:A)", 10)
        .setRelationshipCardinality("()-[]->(:B)", 10)
        .setRelationshipCardinality("(:BB)-[]->()", 10)
        .setRelationshipCardinality("(:BB)-[]->(:B)", 10)
        .setRelationshipCardinality("(:AA)-[]->()", 10)
        .setRelationshipCardinality("(:AA)-[]->(:A)", 10)
        .addSemanticFeature(SemanticFeature.GpmShortestPath)
        .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
        .build()

      planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
        .filter("NOT r1 = r2")
        .nodeHashJoin("a")
        .|.filter("bb:BB")
        .|.expandAll("(b)<-[r2]-(bb)")
        .|.statefulShortestPath(
          "a",
          "b",
          "SHORTEST 1 ((a) ((`n`)-[`r`]-(`m`)){1, } (b) WHERE unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set(),
          singletonRelationshipVariables = Set(),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "a")
            .addTransition(0, 1, "(a) (n)")
            .addTransition(1, 2, "(n)-[r]-(m)")
            .addTransition(2, 1, "(m) (n)")
            .addTransition(2, 3, "(m) (b)")
            .setFinalState(3)
            .build(),
          ExpandInto
        )
        .|.cartesianProduct()
        .|.|.nodeByLabelScan("b", "B")
        .|.nodeByLabelScan("a", "A")
        .filter("aa:AA")
        .expandAll("(a)<-[r1]-(aa)")
        .nodeByLabelScan("a", "A")
        .build())
    }
  }

  test(
    "With statefulShortestPlanningMode=cardinality_heuristic should be able to force using INTO by using a subquery"
  ) {
    val queryWithSubqueryWorkaround =
      """MATCH (a:A), (b:B)
        |CALL {
        |  WITH a, b
        |  MATCH ANY SHORTEST (a)((n)-[r]->(m)){3,}(b)
        |  RETURN r
        |}
        |RETURN *""".stripMargin

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(500)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 30)
      .setRelationshipCardinality("(:A)-[]->()", 500)
      .setRelationshipCardinality("(:A)-[]->(:B)", 500)
      .setRelationshipCardinality("()-[]->(:B)", 500)
      .addSemanticFeature(SemanticFeature.GpmShortestPath)
      .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
      .build()

    planner.plan(queryWithSubqueryWorkaround).stripProduceResults should equal(planner.subPlanBuilder()
      .statefulShortestPath(
        "a",
        "b",
        "SHORTEST 1 ((a) ((`n`)-[`r`]->(`m`)){3, } (b) WHERE unique(`r`))",
        None,
        groupNodes = Set(),
        groupRelationships = Set(("r", "r")),
        singletonNodeVariables = Set(),
        singletonRelationshipVariables = Set(),
        StatefulShortestPath.Selector.Shortest(1),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a) (n)")
          .addTransition(1, 2, "(n)-[r]->(m)")
          .addTransition(2, 3, "(m) (n)")
          .addTransition(3, 4, "(n)-[r]->(m)")
          .addTransition(4, 5, "(m) (n)")
          .addTransition(5, 6, "(n)-[r]->(m)")
          .addTransition(6, 5, "(m) (n)")
          .addTransition(6, 7, "(m) (b)")
          .setFinalState(7)
          .build(),
        ExpandInto
      )
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B")
      .nodeByLabelScan("a", "A")
      .build())
  }

  test(
    "With statefulShortestPlanningMode=cardinality_heuristic, when only INTO is possible and it is not 1:1, should still find a plan and solve hint"
  ) {
    val query =
      """MATCH (a:A), (b:B)
        |WITH * SKIP 0
        |
        |MATCH (a)-[r:REL]->(b)
        |USING SCAN r:REL
        |MATCH ANY SHORTEST (a)-[q:KNOWS]->{2,}(b)
        |
        |RETURN *
        |""".stripMargin

    val planner = plannerBuilder()
      .setAllNodesCardinality(7)
      .setAllRelationshipsCardinality(5005)
      .setLabelCardinality("A", 2)
      .setLabelCardinality("B", 2)
      .setRelationshipCardinality("()-[:REL]->()", 1001)
      .setRelationshipCardinality("(:A)-[:REL]->(:B)", 1001)
      .setRelationshipCardinality("(:A)-[:KNOWS]->(:B)", 4)
      .setRelationshipCardinality("(:A)-[:KNOWS]->()", 4)
      .setRelationshipCardinality("()-[:KNOWS]->()", 4)
      .setRelationshipCardinality("()-[:KNOWS]->(:B)", 4)
      .addSemanticFeature(SemanticFeature.GpmShortestPath)
      .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
      .build()

    planner.plan(query).stripProduceResults should equal(planner.subPlanBuilder()
      .statefulShortestPath(
        "a",
        "b",
        "SHORTEST 1 ((a) ((`anon_0`)-[`q`:KNOWS]->(`anon_1`)){2, } (b) WHERE unique(`q`))",
        None,
        groupNodes = Set(),
        groupRelationships = Set(("q", "q")),
        singletonNodeVariables = Set(),
        singletonRelationshipVariables = Set(),
        StatefulShortestPath.Selector.Shortest(1),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a) (anon_0)")
          .addTransition(1, 2, "(anon_0)-[q:KNOWS]->(anon_1)")
          .addTransition(2, 3, "(anon_1) (anon_0)")
          .addTransition(3, 4, "(anon_0)-[q:KNOWS]->(anon_1)")
          .addTransition(4, 3, "(anon_1) (anon_0)")
          .addTransition(4, 5, "(anon_1) (b)")
          .setFinalState(5)
          .build(),
        ExpandInto
      )
      .filter("a = anon_2", "b = anon_3")
      .apply()
      .|.relationshipTypeScan("(anon_2)-[r:REL]->(anon_3)", "a", "b")
      .skip(0)
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B")
      .nodeByLabelScan("a", "A")
      .build())
  }

  test(
    "With statefulShortestPlanningMode=cardinality_heuristic, should accept small rounding errors and still choose INTO"
  ) {
    val query =
      """MATCH p = ALL SHORTEST (start:AccountHolder {accountHolderID: $startID})-[r:MAKES_PAYMENTS_TO]->{1,2}
        |                       (end:AccountHolder {accountHolderID: $endID})
        |RETURN p
        |""".stripMargin

    val accountHolders = 105491.toDouble
    val planner = plannerBuilder()
      .setAllNodesCardinality(64398064)
      .setLabelCardinality("AccountHolder", accountHolders)
      .setRelationshipCardinality("(:AccountHolder)-[:MAKES_PAYMENTS_TO]->(:AccountHolder)", 911026)
      .setRelationshipCardinality("(:AccountHolder)-[:MAKES_PAYMENTS_TO]->()", 911026)
      .setRelationshipCardinality("()-[:MAKES_PAYMENTS_TO]->(:AccountHolder)", 911026)
      .setRelationshipCardinality("()-[:MAKES_PAYMENTS_TO]->()", 911026)
      .addNodeIndex("AccountHolder", Seq("accountHolderID"), 1.0, 1 / accountHolders, isUnique = true)
      .addSemanticFeature(SemanticFeature.GpmShortestPath)
      .withSetting(GraphDatabaseInternalSettings.stateful_shortest_planning_mode, CARDINALITY_HEURISTIC)
      .build()

    // Should not use ExpandAll
    val plan = planner.plan(query).stripProduceResults
    plan.folder.treeFind[StatefulShortestPath] {
      case ssp: StatefulShortestPath if ssp.mode == ExpandAll => true
    } should be(empty)
  }
}
