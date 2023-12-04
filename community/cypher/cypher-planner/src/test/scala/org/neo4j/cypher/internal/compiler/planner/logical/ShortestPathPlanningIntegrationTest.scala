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
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.helpers.WindowsSafeAnyRef
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.NodeRelPair
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.RepeatPathStep
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.ListSet

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
    .setRelationshipCardinality("()-[:R]->(:B)", 10)
    .setRelationshipCardinality("()-[]->(:N)", 10)
    .setRelationshipCardinality("()-[]->(:NN)", 10)
    .setRelationshipCardinality("()-[]->(:User)", 10)
    .setRelationshipCardinality("(:User)-[]->(:User)", 10)
    .setRelationshipCardinality("(:User)-[]->()", 10)
    .setRelationshipCardinality("(:User)-[]->(:NN)", 10)
    .setRelationshipCardinality("(:B)-[]->(:N)", 10)
    .setRelationshipCardinality("()-[:T]->()", 10)
    .addSemanticFeature(SemanticFeature.GpmShortestPath)

  private val planner = plannerBase.build()

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
          "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v) WHERE unique(`r`))",
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

  test("should plan SHORTEST with QPP with several relationships and path assignment") {
    val query =
      """MATCH p = SHORTEST 1 (s:User) (()-[:R]->()-[:T]-()-[:T]-()-[:T]-()-[:R]->())+ (t)
        |RETURN p""".stripMargin
    val plan = planner.plan(query)

    val pathExpression = PathExpression(NodePathStep(
      varFor("s"),
      RepeatPathStep(
        List(
          NodeRelPair(varFor("anon_13"), varFor("anon_20")),
          NodeRelPair(varFor("anon_14"), varFor("anon_12")),
          NodeRelPair(varFor("anon_18"), varFor("anon_15")),
          NodeRelPair(varFor("anon_11"), varFor("anon_16")),
          NodeRelPair(varFor("anon_17"), varFor("anon_19"))
        ),
        varFor("t"),
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
          "SHORTEST 1 ((s) ((anon_0)-[anon_1:R]->(anon_2)-[anon_3:T]-(anon_4)-[anon_5:T]-(anon_6)-[anon_7:T]-(anon_8)-[anon_9:R]->(anon_10)){1, } (t) WHERE NOT `anon_5` = `anon_3` AND NOT `anon_7` = `anon_3` AND NOT `anon_7` = `anon_5` AND NOT `anon_9` = `anon_1` AND unique((((`anon_20` + `anon_12`) + `anon_15`) + `anon_16`) + `anon_19`))",
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
          singletonNodeVariables = Set("t" -> "t"),
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

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u)-[r:R*]->(v) WHERE size(r) >= 1 AND unique(r))",
          None,
          groupNodes = Set(),
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

  test("should plan SHORTEST with previously bound var-length relationship") {
    val query =
      """
        |MATCH (from:User)-[next:R*1..5]->(to:B)
        |MATCH ANY SHORTEST ()-[next:R*1..5]->()
        |RETURN *""".stripMargin

    val nfa = new TestNFABuilder(0, "  UNNAMED0")
      .addTransition(0, 1, "(`  UNNAMED0`) (`  UNNAMED3`)")
      .addTransition(1, 2, "(`  UNNAMED3`)-[`  next@4`:R]->(`  UNNAMED4`)")
      .addTransition(2, 3, "(`  UNNAMED4`)-[`  next@4`:R]->(`  UNNAMED5`)")
      .addTransition(2, 7, "(`  UNNAMED4`) (`  UNNAMED2`)")
      .addTransition(3, 4, "(`  UNNAMED5`)-[`  next@4`:R]->(`  UNNAMED6`)")
      .addTransition(3, 7, "(`  UNNAMED5`) (`  UNNAMED2`)")
      .addTransition(4, 5, "(`  UNNAMED6`)-[`  next@4`:R]->(`  UNNAMED7`)")
      .addTransition(4, 7, "(`  UNNAMED6`) (`  UNNAMED2`)")
      .addTransition(5, 6, "(`  UNNAMED7`)-[`  next@4`:R]->(`  UNNAMED8`)")
      .addTransition(5, 7, "(`  UNNAMED7`) (`  UNNAMED2`)")
      .addTransition(6, 7, "(`  UNNAMED8`) (`  UNNAMED2`)")
      .setFinalState(7)
      .build()

    val plan = nonDeduplicatingPlanner.plan(query).stripProduceResults
    plan should equal(
      nonDeduplicatingPlanner.subPlanBuilder()
        .statefulShortestPath(
          "  UNNAMED0",
          "  UNNAMED1",
          "SHORTEST 1 ((  UNNAMED0)-[  next@2:R*1..5]->(  UNNAMED1) WHERE `  next@2` = next)",
          Some("`  next@2` = next"),
          Set(),
          Set(("  next@4", "  next@2")),
          Set(("  UNNAMED2", "  UNNAMED1")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        // Yup, not pretty, we could drop these predicates.
        // But currently they are inserted quite early in ASTRewriter,
        // and only later the variable `next` gets renamed to `  next@2` in the 2ns match clause.
        // In general, it is beneficial to NOT rewrite references to `next` to use `  next@2` in predicates,
        // because that allows the predicates to be solved earlier.
        // These implicitly solved relationship predicates are an exception.
        .filter(
          "size(next) >= 1",
          "size(next) <= 5",
          "all(`  UNNAMED9` IN next WHERE single(`  UNNAMED10` IN next WHERE `  UNNAMED9` = `  UNNAMED10`))"
        )
        .apply()
        .|.allNodeScan("`  UNNAMED0`", "from", "to", "next")
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
      "MATCH (d:User) MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (d) ((e)-[s]->(f))* (g) RETURN *"

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
          "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (d) ((e)-[s]->(f)){0, } (g) WHERE `d` = d AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
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
        .|.allNodeScan("a", "d")
        .nodeByLabelScan("d", "User")
        .build()
    )
  }

  test("should allow planning of shortest with already bound end nodes") {
    val query =
      "MATCH (d:User), (a:User) WITH * SKIP 0 MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (d) RETURN *"

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (b)")
        .addTransition(0, 3, "(a) (d WHERE d = d)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (d WHERE d = d)")
        .setFinalState(3)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "d",
          "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (d) WHERE `d` = d AND unique(`r`))",
          None,
          Set(("b", "b"), ("c", "c")),
          Set(("r", "r")),
          Set("d" -> "d"),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .skip(0)
        .cartesianProduct()
        .|.nodeByLabelScan("a", "User")
        .nodeByLabelScan("d", "User")
        .build()
    )
  }

  test("should allow planning of shortest with already bound interior node with predicate on interior node") {
    val query =
      "MATCH (d:User) MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (d {prop: 5}) ((e)-[s]->(f))* (g) RETURN *"

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
          "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (d) ((e)-[s]->(f)){0, } (g) WHERE `d` = d AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
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
        .|.allNodeScan("a", "d")
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
        .addTransition(0, 3, "(a) (`  d@13`)") // it would be more efficient to add "WHERE `  d@13`.prop = 5"
        .addTransition(1, 2, "(`  b@1`)-[`  r@2`]->(`  c@3`)")
        .addTransition(2, 1, "(`  c@3`) (`  b@1`)")
        .addTransition(2, 3, "(`  c@3`) (`  d@13`)") // it would be more efficient to add "WHERE `  d@13`.prop = 5"
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
          "SHORTEST 1 ((a) ((  b@1)-[  r@2]->(  c@3)){0, } (  d@0) ((  e@7)-[  s@8]->(  f@9)){0, } (d) WHERE `  d@0` = d AND d.prop IN [5] AND disjoint(`  r@5`, `  s@11`) AND unique(`  r@5`) AND unique(`  s@11`))",
          Some("`  d@0` = d"),
          Set(("  b@1", "  b@4"), ("  c@3", "  c@6"), ("  e@7", "  e@10"), ("  f@9", "  f@12")),
          Set(("  r@2", "  r@5"), ("  s@8", "  s@11")),
          Set("  d@13" -> "  d@0", "  d@14" -> "d"),
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

    val plan = nonDeduplicatingPlanner.plan(query).stripProduceResults
    plan should equal(
      nonDeduplicatingPlanner.subPlanBuilder()
        .statefulShortestPath(
          "d",
          "a",
          "SHORTEST 1 ((a) ((  b@1)-[  r@2]->(  c@3)){0, } (  d@0) ((  e@7)-[  s@8]->(  f@9)){0, } (d) WHERE `  d@0` = d AND disjoint(`  r@5`, `  s@11`) AND unique(`  r@5`) AND unique(`  s@11`))",
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

    val plan = nonDeduplicatingPlanner.plan(query).stripProduceResults
    plan should equal(
      nonDeduplicatingPlanner.subPlanBuilder()
        .statefulShortestPath(
          "d",
          "a",
          "SHORTEST 1 ((a) ((  b@1)-[  r@2]->(  c@3)){0, } (  a@0) ((  e@7)-[  s@8]->(  f@9)){0, } (d) WHERE `  a@0` = a AND a.prop IN [5] AND disjoint(`  r@5`, `  s@11`) AND unique(`  r@5`) AND unique(`  s@11`))",
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
      "MATCH (a), (d) WITH * SKIP 0 MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (d WHERE d.prop = 5) ((e)-[s]->(f))* (d) RETURN *"

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (`  b@1`)")
        .addTransition(0, 3, "(a) (`  d@14` WHERE `  d@14` = d)")
        .addTransition(1, 2, "(`  b@1`)-[`  r@2`]->(`  c@3`)")
        .addTransition(2, 1, "(`  c@3`) (`  b@1`)")
        .addTransition(2, 3, "(`  c@3`) (`  d@14` WHERE `  d@14` = d)")
        .addTransition(3, 4, "(`  d@14`) (`  e@7`)")
        .addTransition(3, 6, "(`  d@14`) (`  d@15` WHERE `  d@15` = d)")
        .addTransition(4, 5, "(`  e@7`)-[`  s@8`]->(`  f@9`)")
        .addTransition(5, 4, "(`  f@9`) (`  e@7`)")
        .addTransition(5, 6, "(`  f@9`) (`  d@15` WHERE `  d@15` = d)")
        .setFinalState(6)
        .build()

    val plan = nonDeduplicatingPlanner.plan(query).stripProduceResults
    plan should equal(
      nonDeduplicatingPlanner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "  d@13",
          "SHORTEST 1 ((a) ((  b@1)-[  r@2]->(  c@3)){0, } (  d@0) ((  e@7)-[  s@8]->(  f@9)){0, } (  d@13) WHERE `  d@0` = d AND `  d@13` = d AND disjoint(`  r@5`, `  s@11`) AND unique(`  r@5`) AND unique(`  s@11`))",
          None,
          Set(("  b@1", "  b@4"), ("  c@3", "  c@6"), ("  e@7", "  e@10"), ("  f@9", "  f@12")),
          Set(("  r@2", "  r@5"), ("  s@8", "  s@11")),
          Set(("  d@14", "  d@0"), ("  d@15", "  d@13")),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .filter("cacheN[d.prop] = 5")
        .skip(0)
        .cartesianProduct()
        .|.cacheProperties("cacheNFromStore[d.prop]")
        .|.allNodeScan("d")
        .allNodeScan("a")
        .build()
    )
  }

  test(
    "should allow planning of shortest with already bound strict interior node with predicate"
  ) {
    val query =
      "MATCH (d) WITH * SKIP 0 MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (d WHERE d.prop = 5) ((e)-[s]->(f))* (g) RETURN *"

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
          "SHORTEST 1 ((a) ((  b@1)-[  r@2]->(  c@3)){0, } (  d@0) ((  e@7)-[  s@8]->(  f@9)){0, } (g) WHERE `  d@0` = d AND disjoint(`  r@5`, `  s@11`) AND unique(`  r@5`) AND unique(`  s@11`))",
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
        .|.allNodeScan("a", "d")
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
          "SHORTEST 1 ((from)-[  r@0:R]->(b) ((  c@1)-[  r2@2]->(  d@3)){0, } (e) WHERE NOT r IN `  r2@5` AND `  r@0` = r AND unique(`  r2@5`))",
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
          "SHORTEST 1 ((a) ((  b@2)-[  r@3]->(  c@4)){0, } (  d@0) ((  e@8)-[  s@9]->(  f@10)){0, } (  d@1) ((  g@14)-[  t@15]->(  h@16)){0, } (d) WHERE `  d@0` = d AND `  d@1` = d AND disjoint(`  r@6`, `  s@12`) AND disjoint(`  r@6`, `  t@18`) AND disjoint(`  s@12`, `  t@18`) AND unique(`  r@6`) AND unique(`  s@12`) AND unique(`  t@18`))",
          Some("`  d@1` = d AND `  d@0` = d"),
          Set(
            ("  h@16", "  h@19"),
            ("  f@10", "  f@13"),
            ("  c@4", "  c@7"),
            ("  e@8", "  e@11"),
            ("  g@14", "  g@17"),
            ("  b@2", "  b@5")
          ),
          Set(("  r@3", "  r@6"), ("  s@9", "  s@12"), ("  t@15", "  t@18")),
          Set("  d@22" -> "d", "  d@20" -> "  d@0", "  d@21" -> "  d@1"),
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
          "SHORTEST 1 ((a) ((  b@2)-[  r@3]->(  c@4)){0, } (  a@0) ((  e@8)-[  s@9]->(  f@10)){0, } (  a@1) ((  g@14)-[  t@15]->(  h@16)){0, } (d) WHERE `  a@0` = a AND `  a@1` = a AND disjoint(`  r@6`, `  s@12`) AND disjoint(`  r@6`, `  t@18`) AND disjoint(`  s@12`, `  t@18`) AND unique(`  r@6`) AND unique(`  s@12`) AND unique(`  t@18`))",
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
          "SHORTEST 1 ((a) ((  b@1)-[  r@2]->(  c@3)){0, } (d) ((  e@7)-[  s@8]->(  f@9)){0, } (  d@0) ((  g@13)-[  t@14]->(  h@15)){0, } (i) WHERE `  d@0` = d AND disjoint(`  r@5`, `  s@11`) AND disjoint(`  r@5`, `  t@17`) AND disjoint(`  s@11`, `  t@17`) AND unique(`  r@5`) AND unique(`  s@11`) AND unique(`  t@17`))",
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
        .addTransition(0, 3, "(a) (a WHERE a = a)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (a WHERE a = a)")
        .setFinalState(3)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "a",
          "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (a) WHERE `a` = a AND unique(`r`))",
          None,
          Set(("b", "b"), ("c", "c")),
          Set(("r", "r")),
          Set("a" -> "a"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
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

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "v",
          "u",
          "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v) WHERE unique(`r`))",
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
        .eager(ListSet(EagernessReason.Unknown))
        .statefulShortestPath(
          sourceNode = "u",
          targetNode = "w",
          solvedExpressionString =
            "SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v) ((c)-[s]->(d)){1, } (w) WHERE disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
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
      planner.plan(query).stripProduceResults

    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a)")
      .addTransition(1, 2, "(a)-[r:R]->(b:B)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (v WHERE v.prop = 42 AND v:B)")
      .addTransition(3, 4, "(v)-[s]->(w:N)")
      .setFinalState(4)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((a)-[r:R]->(b) WHERE `b`:B){1, } (v)-[s]->(w) WHERE NOT s IN `r` AND unique(`r`) AND v.prop IN [42] AND v:B AND w:N)",
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
      .addTransition(3, 4, "(v)-[s WHERE s.prop = cacheNFromStore[u.prop]]->(w:N)")
      .setFinalState(4)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((a)-[r:R]->(b) WHERE `b`:B){1, } (v)-[s]->(w) WHERE NOT s IN `r` AND s.prop = u.prop AND unique(`r`) AND v.prop = u.prop AND v:B AND w:N)",
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
      planner.plan(query).stripProduceResults

    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (v)")
      .addTransition(3, 4, "(v)-[s]-(w)")
      .setFinalState(4)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v)-[s]-(w) WHERE NOT s IN `r` AND NOT size(`a`) IN [5] AND unique(`r`) AND v.prop = w.prop)",
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
            "SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v) ((c)-[s]->(d)){1, } (w)-[t]->(x) WHERE NOT t IN `r` AND NOT t IN `s` AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
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
      """MATCH ANY SHORTEST (a) ((b)--(b))* (c)
        |RETURN *""".stripMargin

    val nfa = new TestNFABuilder(0, "a")
      .addTransition(0, 1, "(a) (b)")
      .addTransition(0, 3, "(a) (c)")
      .addTransition(1, 2, "(b)-[anon_0]-(b)")
      .addTransition(2, 1, "(b) (b)")
      .addTransition(2, 3, "(b) (c)")
      .setFinalState(3)
      .build()

    val plan = planner.plan(query).stripProduceResults
    val expected = planner.subPlanBuilder()
      .statefulShortestPath(
        "a",
        "c",
        "SHORTEST 1 ((a) ((b)-[anon_0]-(b)){0, } (c) WHERE `b` = `b` AND unique(`anon_6`))",
        Some("all(anon_1 IN range(0, size(b) - 1) WHERE b[anon_1] = b[anon_1])"),
        Set(("b", "b")),
        Set(),
        Set(("c", "c")),
        Set(),
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll,
        false
      )
      .allNodeScan("a")
      .build()
    plan should equal(expected)
  }

  test("Should support a shortest path pattern with a predicate on several entities inside a QPP") {
    val query =
      """MATCH ANY SHORTEST (a) ((b)--(c) WHERE b.prop < c.prop)* (d)
        |RETURN *""".stripMargin

    val nfa = new TestNFABuilder(0, "a")
      .addTransition(0, 1, "(a) (b)")
      .addTransition(0, 3, "(a) (d)")
      .addTransition(1, 2, "(b)-[anon_0]-(c)")
      .addTransition(2, 1, "(c) (b)")
      .addTransition(2, 3, "(c) (d)")
      .setFinalState(3)
      .build()

    val plan = planner.plan(query).stripProduceResults
    val expected = planner.subPlanBuilder()
      .statefulShortestPath(
        "a",
        "d",
        "SHORTEST 1 ((a) ((b)-[anon_0]-(c)){0, } (d) WHERE `b`.prop < `c`.prop AND unique(`anon_5`))",
        Some("all(anon_1 IN range(0, size(b) - 1) WHERE (b[anon_1]).prop < (c[anon_1]).prop)"),
        Set(("b", "b"), ("c", "c")),
        Set(),
        Set(("d", "d")),
        Set(),
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll,
        false
      )
      .allNodeScan("a")
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
          "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v) WHERE unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v"),
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
          "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v) WHERE unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .filter("cache[arg.prop] > 5")
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
          "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v) WHERE unique(`r`))",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .filter("cache[arg.prop] > u.prop")
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

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v) WHERE unique(`r`) AND v:NN)",
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
         |MATCH (n), (m)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST (n)((n_inner)-[r_inner]->(m_inner))+ (m)
         |RETURN *
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val nfa = new TestNFABuilder(0, "n")
      .addTransition(0, 1, "(n) (n_inner)")
      .addTransition(1, 2, "(n_inner)-[r_inner]->(m_inner)")
      .addTransition(2, 1, "(m_inner) (n_inner)")
      .addTransition(2, 3, "(m_inner) (m WHERE m = m)")
      .setFinalState(3)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "n",
          "m",
          "SHORTEST 1 ((n) ((n_inner)-[r_inner]->(m_inner)){1, } (m) WHERE `m` = m AND unique(`r_inner`))",
          None,
          groupNodes = Set(("n_inner", "n_inner"), ("m_inner", "m_inner")),
          groupRelationships = Set(("r_inner", "r_inner")),
          singletonNodeVariables = Set("m" -> "m"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should plan SHORTEST if both start and end are already bound with rewritten selection on boundary nodes") {
    val query =
      s"""
         |MATCH (n), (o)
         |WITH * SKIP 1
         |MATCH ANY SHORTEST ((n)((n_inner)-[r_inner]->(m_inner))+ (m)-[r2]->(o) WHERE m.prop = o.prop)
         |RETURN *
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val nfa = new TestNFABuilder(0, "n")
      .addTransition(0, 1, "(n) (n_inner)")
      .addTransition(1, 2, "(n_inner)-[r_inner]->(m_inner)")
      .addTransition(2, 1, "(m_inner) (n_inner)")
      .addTransition(2, 3, "(m_inner) (m WHERE m.prop = cacheN[o.prop])")
      .addTransition(3, 4, "(m)-[r2]->(o WHERE o = o)")
      .setFinalState(4)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "n",
          "o",
          "SHORTEST 1 ((n) ((n_inner)-[r_inner]->(m_inner)){1, } (m)-[r2]->(o) WHERE NOT r2 IN `r_inner` AND `o` = o AND m.prop = o.prop AND unique(`r_inner`))",
          None,
          groupNodes = Set(("n_inner", "n_inner"), ("m_inner", "m_inner")),
          groupRelationships = Set(("r_inner", "r_inner")),
          singletonNodeVariables = Set("m" -> "m", "o" -> "o"),
          singletonRelationshipVariables = Set("r2" -> "r2"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .skip(1)
        .cartesianProduct()
        .|.cacheProperties("cacheNFromStore[o.prop]")
        .|.allNodeScan("o")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should use cached properties from previously bound variables inside NFA") {
    val query = "MATCH ANY SHORTEST ((u:User WHERE u.prop > 5)((n)-[r]->(m))+(v) WHERE v.prop = u.prop) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v WHERE v.prop = cache[u.prop])")
      .setFinalState(3)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v) WHERE unique(`r`) AND v.prop = u.prop)",
          None,
          groupNodes = Set(("n", "n"), ("m", "m")),
          groupRelationships = Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .nodeIndexOperator("u:User(prop > 5)", getValue = _ => GetValue)
        .build()
    )
  }

  test("should plan pattern expression predicates") {
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

    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition(2, 3, "(b) (v)")
      .addTransition(3, 4, "(v)-[s]->(w)")
      .setFinalState(4)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPathExpr(
          "u",
          "w",
          s"SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v)-[s]->(w) WHERE $solvedNestedExpressionAsString AND NOT s IN `r` AND unique(`r`))",
          Some(patternExpressionPredicate),
          Set(("a", "a"), ("b", "b")),
          Set(("r", "r")),
          singletonNodeVariables = Set("v" -> "v", "w" -> "w"),
          singletonRelationshipVariables = Set("s" -> "s"),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
          ExpandAll,
          reverseGroupVariableProjections = false
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("should plan subquery expression predicates with multiple dependencies") {
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
      solvedExpressionAsString =
        solvedNestedExpressionAsString
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
          s"SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v)-[s]->(w)-[t]->(x) WHERE $solvedNestedExpressionAsString AND NOT s IN `r` AND NOT t = s AND NOT t IN `r` AND unique(`r`))",
          Some(nestedPlanExpression),
          Set(("a", "a"), ("b", "b")),
          Set(("r", "r")),
          singletonNodeVariables = Set("w" -> "w", "v" -> "v", "x" -> "x"),
          singletonRelationshipVariables = Set("t" -> "t", "s" -> "s"),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
          ExpandAll,
          reverseGroupVariableProjections = false
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should handle path assignment for shortest path containing qpp with two juxtaposed nodes") {
    val query = "MATCH p = ANY SHORTEST (a) ((b)-[r]->(c))+ (d) RETURN p"
    val plan = planner.plan(query).stripProduceResults

    val path = PathExpression(NodePathStep(
      varFor("a"),
      RepeatPathStep(
        List(NodeRelPair(varFor("b"), varFor("r"))),
        varFor("d"),
        NilPathStep()(pos)
      )(pos)
    )(pos))(pos)

    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("p" -> path))
      .statefulShortestPath(
        "a",
        "d",
        "SHORTEST 1 ((a) ((b)-[r]->(c)){1, } (d) WHERE unique(`r`))",
        None,
        Set(("b", "b")),
        Set(("r", "r")),
        Set("d" -> "d"),
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
      .allNodeScan("a")
      .build()
  }

  test("Should handle path assignment for shortest path containing qpp with two juxtaposed patterns") {
    val query = "MATCH p = ANY SHORTEST (a)--(b) ((c)-[r]->(d))+ (e)--(f) RETURN p"
    val plan = planner.plan(query).stripProduceResults

    val path = PathExpression(
      NodePathStep(
        varFor("a"),
        SingleRelationshipPathStep(
          varFor("anon_0"),
          BOTH,
          Some(varFor("b")),
          RepeatPathStep(
            List(NodeRelPair(varFor("c"), varFor("r"))),
            varFor("e"),
            SingleRelationshipPathStep(
              varFor("anon_1"),
              BOTH,
              Some(varFor("f")),
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
        "SHORTEST 1 ((a)-[anon_0]-(b) ((c)-[r]->(d)){1, } (e)-[anon_1]-(f) WHERE NOT `anon_0` = `anon_1` AND NOT `anon_0` IN `r` AND NOT `anon_1` IN `r` AND unique(`r`))",
        None,
        Set(("c", "c")),
        Set(("r", "r")),
        Set("f" -> "f", "e" -> "e", "b" -> "b"),
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
      .allNodeScan("a")
      .build()
  }

  test("Should handle path assignment for shortest path with simple pattern") {
    val query = "MATCH p = ANY SHORTEST (a)-[r]->(b) RETURN p"
    val plan = planner.plan(query).stripProduceResults

    val path = PathExpression(
      NodePathStep(
        varFor("a"),
        SingleRelationshipPathStep(
          varFor("r"),
          OUTGOING,
          Some(varFor("b")),
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
      .allNodeScan("a")
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
        "SHORTEST 1 ((a) ((n)-[r]->(m)){1, } (b) WHERE b:B AND unique(`r`))",
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
        "SHORTEST 1 ((a) ((n)-[r]->(m)){1, } (b) WHERE a:A AND unique(`r`))",
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
      .build()
      .plan(query).stripProduceResults should equal(planRL)
  }

  test("Should handle sub-path assignment with pre-filter predicates for shortest path") {
    val query = "MATCH ANY SHORTEST (p = (a) ((b)-[r]->(c))+ (d) ((e)<-[s]-(f))+ (g) WHERE length(p) > 3) RETURN p"
    val plan = planner.plan(query).stripProduceResults

    val path = PathExpression(NodePathStep(
      varFor("a"),
      RepeatPathStep(
        List(NodeRelPair(varFor("b"), varFor("r"))),
        varFor("d"),
        RepeatPathStep(
          List(NodeRelPair(varFor("e"), varFor("s"))),
          varFor("g"),
          NilPathStep()(pos)
        )(pos)
      )(pos)
    )(pos))(pos)

    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("p" -> path))
      .statefulShortestPathExpr(
        "a",
        "g",
        s"SHORTEST 1 ((a) ((b)-[r]->(c)){1, } (d) ((e)<-[s]-(f)){1, } (g) WHERE disjoint(`r`, `s`) AND length((a) ((b)-[r]-())* (d) ((e)-[s]-())* (g)) > 3 AND unique(`r`) AND unique(`s`))",
        Some(greaterThan(length(path), literalInt(3))),
        Set(("b", "b"), ("e", "e")),
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
        ExpandAll,
        reverseGroupVariableProjections = false
      )
      .allNodeScan("a")
      .build()
  }

  // There was a subtlety in unwrapParenthesizedPath leading to issues for sub-paths with no predicates
  test("Should handle sub-path assignment with no predicates for shortest path") {
    val query = "MATCH ANY SHORTEST (p = (a) ((b)-[r]->(c))+ (d) ((e)<-[s]-(f))+ (g)) RETURN p"
    val plan = planner.plan(query).stripProduceResults

    val path = PathExpression(NodePathStep(
      varFor("a"),
      RepeatPathStep(
        List(NodeRelPair(varFor("b"), varFor("r"))),
        varFor("d"),
        RepeatPathStep(
          List(NodeRelPair(varFor("e"), varFor("s"))),
          varFor("g"),
          NilPathStep()(pos)
        )(pos)
      )(pos)
    )(pos))(pos)

    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("p" -> path))
      .statefulShortestPathExpr(
        "a",
        "g",
        s"SHORTEST 1 ((a) ((b)-[r]->(c)){1, } (d) ((e)<-[s]-(f)){1, } (g) WHERE disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
        None,
        Set(("b", "b"), ("e", "e")),
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
        ExpandAll,
        reverseGroupVariableProjections = false
      )
      .allNodeScan("a")
      .build()
  }

  test("Should handle both path and sub-path assignment") {
    val query =
      "MATCH p = ANY SHORTEST (q = (a) ((b)-[r]->(c))+ (d) ((e)<-[s]-(f))+ (g) WHERE length(q) > 3) RETURN p, q"
    val plan = planner.plan(query).stripProduceResults

    val path = PathExpression(NodePathStep(
      varFor("a"),
      RepeatPathStep(
        List(NodeRelPair(varFor("b"), varFor("r"))),
        varFor("d"),
        RepeatPathStep(
          List(NodeRelPair(varFor("e"), varFor("s"))),
          varFor("g"),
          NilPathStep()(pos)
        )(pos)
      )(pos)
    )(pos))(pos)

    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("p" -> path, "q" -> path))
      .statefulShortestPathExpr(
        "a",
        "g",
        s"SHORTEST 1 ((a) ((b)-[r]->(c)){1, } (d) ((e)<-[s]-(f)){1, } (g) WHERE disjoint(`r`, `s`) AND length((a) ((b)-[r]-())* (d) ((e)-[s]-())* (g)) > 3 AND unique(`r`) AND unique(`s`))",
        Some(greaterThan(length(path), literalInt(3))),
        Set(("b", "b"), ("e", "e")),
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
        ExpandAll,
        reverseGroupVariableProjections = false
      )
      .allNodeScan("a")
      .build()
  }

  test("Should inline relationship local (start and end node) predicate into NFA: outside QPP") {
    val query = "MATCH ANY SHORTEST ((u:User)((n)-[r]->(m))+(v)-[r2]->(w) WHERE v.prop > w.prop) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .addTransition(3, 4, "(v)-[r2 WHERE startNode(r2).prop > endNode(r2).prop]->(w)")
      .setFinalState(4)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v)-[r2]->(w) WHERE NOT r2 IN `r` AND unique(`r`) AND v.prop > w.prop)",
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
          "SHORTEST 1 ((u) ((n)-[r]->(m) WHERE `n`.prop > `m`.prop){1, } (v)-[r2]->(w) WHERE NOT r2 IN `r` AND unique(`r`))",
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
    val query = "MATCH ANY SHORTEST ((u:User)((n)-[r]->(m))+(v)<-[r2]-(w) WHERE v.prop > w.prop) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .addTransition(3, 4, "(v)<-[r2 WHERE endNode(r2).prop > startNode(r2).prop]-(w)")
      .setFinalState(4)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v)<-[r2]-(w) WHERE NOT r2 IN `r` AND unique(`r`) AND v.prop > w.prop)",
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

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "w",
          "u",
          "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v)<-[r2]-(w) WHERE NOT r2 IN `r` AND unique(`r`) AND v.prop > r2.prop + w.prop)",
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

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "w",
          "u",
          "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v)-[r2]->(w) WHERE NOT r2 IN `r` AND unique(`r`) AND v.prop > r2.prop + w.prop)",
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
    val query = "MATCH ANY SHORTEST ((u:User)((n)-[r]->(m))+(v)-[r2]-(w) WHERE v.prop > w.prop) RETURN *"

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
          "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v)-[r2]-(w) WHERE NOT r2 IN `r` AND unique(`r`) AND v.prop > w.prop)",
          Some("v.prop > w.prop"),
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

  // Our AST does not allow rewriting DesugaredMapProjections to use an expression instead of a variable.
  test("Should not inline relationship local (start and end node) predicate into NFA: DesugaredMapProjection") {
    val query = "MATCH ANY SHORTEST ((u:User)((n)-[r]->(m))+(v)-[r2]->(w) WHERE v{.foo,.bar} = w{.foo,.bar}) RETURN *"

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
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v)-[r2]->(w) WHERE NOT r2 IN `r` AND unique(`r`) AND v{foo: v.foo, bar: v.bar} = w{foo: w.foo, bar: w.bar})",
          Some("v{foo: v.foo, bar: v.bar} = w{foo: w.foo, bar: w.bar}"),
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
        "SHORTEST 1 ((u) ((n)-[r]->(c)-[anon_0]->(m) WHERE `c`:B){1, } (v) WHERE NOT `anon_0` = `r` AND `n`.prop <= `m`.prop AND unique(`r` + `anon_7`))",
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
    val query = "MATCH (n) MATCH ANY SHORTEST ()-[r WHERE r.prop = n.prop]->() RETURN *"

    val nfa = new TestNFABuilder(0, "anon_0")
      .addTransition(0, 1, "(anon_0)-[r WHERE r.prop = cacheN[n.prop]]->(anon_2)")
      .setFinalState(1)
      .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "anon_0",
          "anon_1",
          "SHORTEST 1 ((anon_0)-[r]->(anon_1) WHERE r.prop = n.prop)",
          None,
          Set(),
          Set(),
          Set(("anon_2", "anon_1")),
          Set(("r", "r")),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll
        )
        .apply()
        .|.allNodeScan("anon_0", "n")
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
          "SHORTEST 1 ((u) ((n)-[r]->(m) WHERE a.prop = `n`.prop){1, } (v) WHERE unique(`r`))",
          None,
          Set(("n", "n"), ("m", "m")),
          Set(("r", "r")),
          Set(("v", "v")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          false
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

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v) WHERE $param AND unique(`r`))",
          Some("all(anon_0 IN range(0, size(m) - 1) WHERE $param)"),
          Set(("n", "n"), ("m", "m")),
          Set(("r", "r")),
          Set(("v", "v")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          false
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

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "v",
          "SHORTEST 1 ((u) ((n)-[r]->(m) WHERE `m`.prop IN [$param]){1, } (v) WHERE unique(`r`) AND v.prop IN [$param])",
          None,
          Set(("n", "n"), ("m", "m")),
          Set(("r", "r")),
          Set(("v", "v")),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          false
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }
}
