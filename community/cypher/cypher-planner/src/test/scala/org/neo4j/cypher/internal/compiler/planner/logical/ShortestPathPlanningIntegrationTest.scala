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
import org.neo4j.exceptions.InternalException

import scala.collection.immutable.ListSet

class ShortestPathPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  // We compare "solvedExpressionString" nested inside LogicalPlans.
  // This saves us from windows line break mismatches in those strings.
  implicit val windowsSafe: WindowsSafeAnyRef[LogicalPlan] = new WindowsSafeAnyRef[LogicalPlan]

  private val planner = plannerBuilder()
    .setAllNodesCardinality(100)
    .setAllRelationshipsCardinality(40)
    .setLabelCardinality("User", 4)
    .setLabelCardinality("N", 6)
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
    .build()

  test("should plan SHORTEST with 1 QPP, + quantifier, no predicates, left-to-right") {
    val query = "MATCH ANY SHORTEST (u:User)((n)-[r]->(m))+(v) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .addFinalState(3)
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
          ExpandAll,
          false
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
          NodeRelPair(varFor("anon_17"), varFor("anon_10")),
          NodeRelPair(varFor("anon_18"), varFor("anon_6")),
          NodeRelPair(varFor("anon_20"), varFor("anon_7")),
          NodeRelPair(varFor("anon_16"), varFor("anon_8")),
          NodeRelPair(varFor("anon_19"), varFor("anon_9"))
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
          "SHORTEST 1 ((s) ((anon_11)-[anon_1:R]->(anon_12)-[anon_2:T]-(anon_13)-[anon_3:T]-(anon_14)-[anon_4:T]-(anon_15)-[anon_5:R]->(anon_0) " +
            "WHERE NOT `anon_5` = `anon_1` AND NOT `anon_4` = `anon_3` AND NOT `anon_4` = `anon_2` AND NOT `anon_3` = `anon_2`){1, } (t) " +
            "WHERE unique((((`anon_10` + `anon_6`) + `anon_7`) + `anon_8`) + `anon_9`))",
          None,
          groupNodes = Set(
            ("anon_11", "anon_17"),
            ("anon_12", "anon_18"),
            ("anon_13", "anon_20"),
            ("anon_14", "anon_16"),
            ("anon_15", "anon_19")
          ),
          groupRelationships = Set(
            ("anon_1", "anon_10"),
            ("anon_2", "anon_6"),
            ("anon_3", "anon_7"),
            ("anon_4", "anon_8"),
            ("anon_5", "anon_9")
          ),
          singletonNodeVariables = Set("t" -> "t"),
          singletonRelationshipVariables = Set(),
          selector = StatefulShortestPath.Selector.Shortest(1),
          nfa = new TestNFABuilder(0, "s")
            .addTransition(0, 1, "(s) (anon_11)")
            .addTransition(1, 2, "(anon_11)-[anon_1:R]->(anon_12)")
            .addTransition(2, 3, "(anon_12)-[anon_2:T]-(anon_13)")
            .addTransition(3, 4, "(anon_13)-[anon_3:T]-(anon_14)")
            .addTransition(4, 5, "(anon_14)-[anon_4:T]-(anon_15)")
            .addTransition(5, 6, "(anon_15)-[anon_5:R]->(anon_0)")
            .addTransition(6, 1, "(anon_0) (anon_11)")
            .addTransition(6, 7, "(anon_0) (t)")
            .addFinalState(7)
            .build(),
          ExpandAll,
          reverseGroupVariableProjections = false
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
      .addFinalState(3)
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
          ExpandAll,
          false
        )
        .nodeByLabelScan("u", "User")
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
        .addFinalState(8)
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
          ExpandAll,
          reverseGroupVariableProjections = false
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
        .addTransition(0, 3, "(a) (anon_1 WHERE d = anon_1)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (anon_1 WHERE d = anon_1)")
        .addTransition(3, 4, "(anon_1) (e)")
        .addTransition(3, 6, "(anon_1) (g)")
        .addTransition(4, 5, "(e)-[s]->(f)")
        .addTransition(5, 4, "(f) (e)")
        .addTransition(5, 6, "(f) (g)")
        .addFinalState(6)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "g",
          "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (anon_0) ((e)-[s]->(f)){0, } (g) WHERE d = `anon_0` AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
          None,
          groupNodes = Set(("b", "b"), ("c", "c"), ("e", "e"), ("f", "f")),
          groupRelationships = Set(("r", "r"), ("s", "s")),
          singletonNodeVariables = Set("anon_1" -> "anon_0", "g" -> "g"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = false
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
        .addTransition(0, 3, "(a) (anon_1 WHERE d = anon_1)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (anon_1 WHERE d = anon_1)")
        .addFinalState(3)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "anon_0",
          "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (anon_0) WHERE d = `anon_0` AND unique(`r`))",
          None,
          Set(("b", "b"), ("c", "c")),
          Set(("r", "r")),
          Set("anon_1" -> "anon_0"),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = false
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
        .addTransition(0, 3, "(a) (anon_1 WHERE d = anon_1)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (anon_1 WHERE d = anon_1)")
        .addTransition(3, 4, "(anon_1) (e)")
        .addTransition(3, 6, "(anon_1) (g)")
        .addTransition(4, 5, "(e)-[s]->(f)")
        .addTransition(5, 4, "(f) (e)")
        .addTransition(5, 6, "(f) (g)")
        .addFinalState(6)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "g",
          "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (anon_0) ((e)-[s]->(f)){0, } (g) WHERE d = `anon_0` AND d.prop IN [5] AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
          // TODO: This could be moved to the earlier nodeByLabelScan making it a indexSeek. We could also rewrite the variable name here to inline the predicate but that would make it impossible to optimise it later.
          Some("cacheN[d.prop] = 5"),
          Set(("b", "b"), ("c", "c"), ("e", "e"), ("f", "f")),
          Set(("r", "r"), ("s", "s")),
          Set("anon_1" -> "anon_0", "g" -> "g"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = false
        )
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
        .addTransition(0, 1, "(a) (b)")
        .addTransition(0, 3, "(a) (d WHERE d.prop = 5)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (d WHERE d.prop = 5)")
        .addTransition(3, 4, "(d) (e)")
        .addTransition(3, 6, "(d) (anon_1)")
        .addTransition(4, 5, "(e)-[s]->(f)")
        .addTransition(5, 4, "(f) (e)")
        .addTransition(5, 6, "(f) (anon_1)")
        .addFinalState(6)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "anon_0",
          "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (d) ((e)-[s]->(f)){0, } (anon_0) WHERE d = `anon_0` AND d.prop IN [5] AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
          Some("d = anon_0"),
          Set(("b", "b"), ("c", "c"), ("e", "e"), ("f", "f")),
          Set(("r", "r"), ("s", "s")),
          Set("anon_1" -> "anon_0", "d" -> "d"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = false
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
        .addTransition(0, 1, "(d) (f)")
        .addTransition(0, 3, "(d) (anon_1 WHERE d = anon_1)")
        .addTransition(1, 2, "(f)<-[s]-(e)")
        .addTransition(2, 1, "(e) (f)")
        .addTransition(2, 3, "(e) (anon_1 WHERE d = anon_1)")
        .addTransition(3, 4, "(anon_1) (c)")
        .addTransition(3, 6, "(anon_1) (a)")
        .addTransition(4, 5, "(c)<-[r]-(b)")
        .addTransition(5, 4, "(b) (c)")
        .addTransition(5, 6, "(b) (a)")
        .addFinalState(6)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "d",
          "a",
          "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (anon_0) ((e)-[s]->(f)){0, } (d) WHERE d = `anon_0` AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
          None,
          Set(("b", "b"), ("c", "c"), ("e", "e"), ("f", "f")),
          Set(("r", "r"), ("s", "s")),
          Set("a" -> "a", "anon_1" -> "anon_0"),
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

  test("Shortest with Bound interior Node with predicate - reversed") {
    val query =
      "MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (a WHERE a.prop = 5) ((e)-[s]->(f))* (d:User) RETURN *"

    val nfa =
      new TestNFABuilder(0, "d")
        .addTransition(0, 1, "(d) (f)")
        .addTransition(0, 3, "(d) (a WHERE a.prop = 5)")
        .addTransition(1, 2, "(f)<-[s]-(e)")
        .addTransition(2, 1, "(e) (f)")
        .addTransition(2, 3, "(e) (a WHERE a.prop = 5)")
        .addTransition(3, 4, "(a) (c)")
        .addTransition(3, 6, "(a) (anon_1)")
        .addTransition(4, 5, "(c)<-[r]-(b)")
        .addTransition(5, 4, "(b) (c)")
        .addTransition(5, 6, "(b) (anon_1)")
        .addFinalState(6)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "d",
          "anon_0",
          "SHORTEST 1 ((anon_0) ((b)-[r]->(c)){0, } (a) ((e)-[s]->(f)){0, } (d) WHERE a = `anon_0` AND a.prop IN [5] AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
          Some("a = anon_0"),
          Set(("b", "b"), ("c", "c"), ("e", "e"), ("f", "f")),
          Set(("r", "r"), ("s", "s")),
          Set("anon_1" -> "anon_0", "a" -> "a"),
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
      "MATCH (a), (d) WITH *, 1 AS dummy MATCH ANY SHORTEST (a) ((b)-[r]->(c))* (d WHERE d.prop = 5) ((e)-[s]->(f))* (d) RETURN *"

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (b)")
        .addTransition(0, 3, "(a) (anon_2 WHERE d = anon_2)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (anon_2 WHERE d = anon_2)")
        .addTransition(3, 4, "(anon_2) (e)")
        .addTransition(3, 6, "(anon_2) (anon_3 WHERE d = anon_3)")
        .addTransition(4, 5, "(e)-[s]->(f)")
        .addTransition(5, 4, "(f) (e)")
        .addTransition(5, 6, "(f) (anon_3 WHERE d = anon_3)")
        .addFinalState(6)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "anon_1",
          "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (anon_0) ((e)-[s]->(f)){0, } (anon_1) WHERE d = `anon_0` AND d = `anon_1` AND disjoint(`r`, `s`) AND unique(`r`) AND unique(`s`))",
          None,
          Set(("b", "b"), ("c", "c"), ("e", "e"), ("f", "f")),
          Set(("r", "r"), ("s", "s")),
          Set("anon_2" -> "anon_0", "anon_3" -> "anon_1"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = false
        )
        .filter("cacheN[d.prop] = 5")
        .projection("1 AS dummy")
        .cartesianProduct()
        .|.cacheProperties("cacheNFromStore[d.prop]")
        .|.allNodeScan("d")
        .allNodeScan("a")
        .build()
    )
  }

  test("should allow planning of shortest with multiple repeated interior node") {
    val query =
      "MATCH ANY SHORTEST (a:User) ((b)-[r]->(c))* (d) ((e)-[s]->(f))* (d) ((g)-[t]->(h))* (d) RETURN *"

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (b)")
        .addTransition(0, 3, "(a) (d)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (d)")
        .addTransition(3, 4, "(d) (e)")
        .addTransition(3, 6, "(d) (anon_2)")
        .addTransition(4, 5, "(e)-[s]->(f)")
        .addTransition(5, 4, "(f) (e)")
        .addTransition(5, 6, "(f) (anon_2)")
        .addTransition(6, 7, "(anon_2) (g)")
        .addTransition(6, 9, "(anon_2) (anon_3)")
        .addTransition(7, 8, "(g)-[t]->(h)")
        .addTransition(8, 7, "(h) (g)")
        .addTransition(8, 9, "(h) (anon_3)")
        .addFinalState(9)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "anon_1",
          "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (d) ((e)-[s]->(f)){0, } (anon_0) ((g)-[t]->(h)){0, } (anon_1) WHERE d = `anon_0` AND d = `anon_1` AND disjoint(`r`, `s`) AND disjoint(`r`, `t`) AND disjoint(`s`, `t`) AND unique(`r`) AND unique(`s`) AND unique(`t`))",
          Some("d = anon_0 AND d = anon_1"),
          Set(("g", "g"), ("c", "c"), ("f", "f"), ("b", "b"), ("e", "e"), ("h", "h")),
          Set(("r", "r"), ("s", "s"), ("t", "t")),
          Set("d" -> "d", "anon_2" -> "anon_0", "anon_3" -> "anon_1"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = false
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
        .addTransition(0, 1, "(a) (b)")
        .addTransition(0, 3, "(a) (anon_2 WHERE a = anon_2)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (anon_2 WHERE a = anon_2)")
        .addTransition(3, 4, "(anon_2) (e)")
        .addTransition(3, 6, "(anon_2) (anon_3 WHERE a = anon_3)")
        .addTransition(4, 5, "(e)-[s]->(f)")
        .addTransition(5, 4, "(f) (e)")
        .addTransition(5, 6, "(f) (anon_3 WHERE a = anon_3)")
        .addTransition(6, 7, "(anon_3) (g)")
        .addTransition(6, 9, "(anon_3) (d)")
        .addTransition(7, 8, "(g)-[t]->(h)")
        .addTransition(8, 7, "(h) (g)")
        .addTransition(8, 9, "(h) (d)")
        .addFinalState(9)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "d",
          "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (anon_0) ((e)-[s]->(f)){0, } (anon_1) ((g)-[t]->(h)){0, } (d) WHERE a = `anon_0` AND a = `anon_1` AND disjoint(`r`, `s`) AND disjoint(`r`, `t`) AND disjoint(`s`, `t`) AND unique(`r`) AND unique(`s`) AND unique(`t`))",
          None,
          Set(("g", "g"), ("c", "c"), ("f", "f"), ("b", "b"), ("e", "e"), ("h", "h")),
          Set(("r", "r"), ("s", "s"), ("t", "t")),
          Set("anon_2" -> "anon_0", "anon_3" -> "anon_1", "d" -> "d"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = false
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
        .addTransition(0, 1, "(a) (b)")
        .addTransition(0, 3, "(a) (d)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (d)")
        .addTransition(3, 4, "(d) (e)")
        .addTransition(3, 6, "(d) (anon_1)")
        .addTransition(4, 5, "(e)-[s]->(f)")
        .addTransition(5, 4, "(f) (e)")
        .addTransition(5, 6, "(f) (anon_1)")
        .addTransition(6, 7, "(anon_1) (g)")
        .addTransition(6, 9, "(anon_1) (i)")
        .addTransition(7, 8, "(g)-[t]->(h)")
        .addTransition(8, 7, "(h) (g)")
        .addTransition(8, 9, "(h) (i)")
        .addFinalState(9)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "i",
          "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (d) ((e)-[s]->(f)){0, } (anon_0) ((g)-[t]->(h)){0, } (i) WHERE d = `anon_0` AND disjoint(`r`, `s`) AND disjoint(`r`, `t`) AND disjoint(`s`, `t`) AND unique(`r`) AND unique(`s`) AND unique(`t`))",
          Some("d = anon_0"),
          Set(("g", "g"), ("c", "c"), ("f", "f"), ("b", "b"), ("e", "e"), ("h", "h")),
          Set(("r", "r"), ("s", "s"), ("t", "t")),
          Set("d" -> "d", "anon_1" -> "anon_0", "i" -> "i"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = false
        )
        .nodeByLabelScan("a", "User")
        .build()
    )
  }

  test("should allow planning of shortest with exterior node") {
    val query =
      "MATCH ANY SHORTEST (a:User) ((b)-[r]->(c))* (a) RETURN *"

    val nfa =
      new TestNFABuilder(0, "a")
        .addTransition(0, 1, "(a) (b)")
        .addTransition(0, 3, "(a) (anon_1 WHERE a = anon_1)")
        .addTransition(1, 2, "(b)-[r]->(c)")
        .addTransition(2, 1, "(c) (b)")
        .addTransition(2, 3, "(c) (anon_1 WHERE a = anon_1)")
        .addFinalState(3)
        .build()

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "a",
          "anon_0",
          "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (anon_0) WHERE a = `anon_0` AND unique(`r`))",
          None,
          Set(("b", "b"), ("c", "c")),
          Set(("r", "r")),
          Set("anon_1" -> "anon_0"),
          Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = false
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
      .addFinalState(3)
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
          true
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
      .addFinalState(6)
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
          ExpandAll,
          false
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
      .addFinalState(4)
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
          ExpandAll,
          false
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
      .addFinalState(4)
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
          ExpandAll,
          false
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
      .addFinalState(4)
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
          ExpandAll,
          false
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
      .addFinalState(7)
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
          ExpandAll,
          false
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should not support a strict interior relationship of a shortest path pattern from a previous MATCH clause") {
    val query =
      """MATCH ()-[r]->()
        |MATCH ANY SHORTEST (a)-[r]->(b)-->*(c)
        |RETURN *""".stripMargin

    an[InternalException] should be thrownBy planner.plan(query)
  }

  test("Should not support a strict interior node of a shortest path pattern to be repeated, inside QPP") {
    val query =
      """MATCH ANY SHORTEST (a) ((b)--(b))* (c)
        |RETURN *""".stripMargin

    an[InternalException] should be thrownBy planner.plan(query)
  }

  test("Should not support a shortest path pattern with a predicate on several entities inside a QPP") {
    val query =
      """MATCH ANY SHORTEST (a) ((b)--(c) WHERE b.prop < c.prop)* (d)
        |RETURN *""".stripMargin

    an[InternalException] should be thrownBy planner.plan(query)
  }

  test(
    "should plan SHORTEST with predicate depending on no path variables as a filter before the statefulShortestPath"
  ) {
    val query = "MATCH ANY SHORTEST ((u:User)((n)-[r]->(m))+(v) WHERE $param) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .addFinalState(3)
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
          ExpandAll,
          false
        )
        .filter("CoerceToPredicate($param)")
        .nodeByLabelScan("u", "User")
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
      .addFinalState(3)
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
          ExpandAll,
          false
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
      .addTransition(2, 3, "(m_inner) (anon_1 WHERE m = anon_1)")
      .addFinalState(3)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "n",
          "anon_0",
          "SHORTEST 1 ((n) ((n_inner)-[r_inner]->(m_inner)){1, } (anon_0) WHERE m = `anon_0` AND unique(`r_inner`))",
          None,
          groupNodes = Set(("n_inner", "n_inner"), ("m_inner", "m_inner")),
          groupRelationships = Set(("r_inner", "r_inner")),
          singletonNodeVariables = Set("anon_1" -> "anon_0"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = false
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
      .addTransition(3, 4, "(m)-[r2]->(anon_1 WHERE o = anon_1)")
      .addFinalState(4)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "n",
          "anon_0",
          "SHORTEST 1 ((n) ((n_inner)-[r_inner]->(m_inner)){1, } (m)-[r2]->(anon_0) WHERE NOT r2 IN `r_inner` AND m.prop = o.prop AND o = `anon_0` AND unique(`r_inner`))",
          None,
          groupNodes = Set(("n_inner", "n_inner"), ("m_inner", "m_inner")),
          groupRelationships = Set(("r_inner", "r_inner")),
          singletonNodeVariables = Set("m" -> "m", "anon_1" -> "anon_0"),
          singletonRelationshipVariables = Set("r2" -> "r2"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          ExpandAll,
          reverseGroupVariableProjections = false
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
      .addFinalState(3)
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
          ExpandAll,
          false
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
      .addFinalState(4)
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
          false
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
      .addFinalState(5)
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
          false
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
          .addFinalState(3)
          .build(),
        ExpandAll,
        false
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
          .addFinalState(5)
          .build(),
        ExpandAll,
        false
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
          .addFinalState(1)
          .build(),
        ExpandAll,
        false
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
      .addFinalState(3)
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
        ExpandAll,
        reverseGroupVariableProjections = false
      )
      .nodeByLabelScan("a", "A")
      .build()

    val nfaRL = new TestNFABuilder(0, "b")
      .addTransition(0, 1, "(b) (m)")
      .addTransition(1, 2, "(m)<-[r]-(n)")
      .addTransition(2, 1, "(n) (m)")
      .addTransition(2, 3, "(n) (a:A)")
      .addFinalState(3)
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
          .addFinalState(6)
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
          .addFinalState(6)
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
          .addFinalState(6)
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
      .addFinalState(4)
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
      .addFinalState(4)
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
      .addFinalState(4)
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
      .addFinalState(4)
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
      .addFinalState(4)
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
      .addFinalState(4)
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
      .addFinalState(4)
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

  test("Should plan shortest with dependencies to previous match in inlined predicate") {
    val query = "MATCH (n) MATCH ANY SHORTEST ()-[r WHERE r.prop = n.prop]->() RETURN *"

    val nfa = new TestNFABuilder(0, "anon_0")
      .addTransition(0, 1, "(anon_0)-[r WHERE r.prop = cacheN[n.prop]]->(anon_2)")
      .addFinalState(1)
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
}
