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
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NFA.NodeJuxtapositionPredicate
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
          singletonNodeVariables = Set("v"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
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
          NodeRelPair(varFor("anon_27"), varFor("anon_20")),
          NodeRelPair(varFor("anon_28"), varFor("anon_16")),
          NodeRelPair(varFor("anon_30"), varFor("anon_17")),
          NodeRelPair(varFor("anon_26"), varFor("anon_18")),
          NodeRelPair(varFor("anon_29"), varFor("anon_19"))
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
          "SHORTEST 1 ((s) ((anon_21)-[anon_11:R]->(anon_22)-[anon_12:T]-(anon_23)-[anon_13:T]-(anon_24)-[anon_14:T]-(anon_25)-[anon_15:R]->(anon_10) WHERE NOT `anon_15` = `anon_11` AND NOT `anon_14` = `anon_13` AND NOT `anon_14` = `anon_12` AND NOT `anon_13` = `anon_12`){1, } (t) WHERE unique((((`anon_20` + `anon_16`) + `anon_17`) + `anon_18`) + `anon_19`))",
          None,
          Set(
            ("anon_21", "anon_27"),
            ("anon_22", "anon_28"),
            ("anon_25", "anon_29"),
            ("anon_23", "anon_30"),
            ("anon_24", "anon_26")
          ),
          Set(
            ("anon_11", "anon_20"),
            ("anon_13", "anon_17"),
            ("anon_15", "anon_19"),
            ("anon_12", "anon_16"),
            ("anon_14", "anon_18")
          ),
          Set("t"),
          Set(),
          StatefulShortestPath.Selector.Shortest(1),
          new TestNFABuilder(0, "s")
            .addTransition(0, 1, "(s) (anon_21)")
            .addTransition(1, 2, "(anon_21)-[anon_11:R]->(anon_22)")
            .addTransition(2, 3, "(anon_22)-[anon_12:T]-(anon_23)")
            .addTransition(3, 4, "(anon_23)-[anon_13:T]-(anon_24)")
            .addTransition(4, 5, "(anon_24)-[anon_14:T]-(anon_25)")
            .addTransition(5, 6, "(anon_25)-[anon_15:R]->(anon_10)")
            .addTransition(6, 1, "(anon_10) (anon_21)")
            .addTransition(6, 7, "(anon_10) (t)")
            .addFinalState(7)
            .build(),
          reverseGroupVariableProjections = false
        )
        .nodeByLabelScan("s", "User")
        .build()
    )
  }

  test("should plan SHORTEST with var-length relationship") {
    val query = "MATCH ANY SHORTEST (u:User)-[r:R*]->(v) RETURN *"

    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (anon_1)")
      .addTransition(1, 2, "(anon_1)-[r:R]->(anon_2)")
      .addTransition(2, 2, "(anon_2)-[r:R]->(anon_2)")
      .addTransition(2, 3, "(anon_2) (v)")
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
          singletonNodeVariables = Set("v"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
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
        .addTransition(0, 1, "(u) (anon_3)")
        .addTransition(1, 2, "(anon_3)-[r:R]->(anon_4)")
        .addTransition(2, 2, "(anon_4)-[r:R]->(anon_4)")
        .addTransition(2, 3, "(anon_4) (v WHERE v.prop = 3)")
        .addTransition(3, 4, "(v)-[s]->(w WHERE w.prop = 4)")
        .addTransition(4, 5, "(w) (anon_6)")
        .addTransition(5, 6, "(anon_6)-[t:R|T]->(anon_7)")
        .addTransition(6, 7, "(anon_7)-[t:R|T]->(anon_8)")
        .addTransition(6, 8, "(anon_7) (x)")
        .addTransition(7, 8, "(anon_8) (x)")
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
          singletonNodeVariables = Set("v", "w", "x"),
          singletonRelationshipVariables = Set("s"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          false
        )
        .nodeByLabelScan("u", "User")
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
          singletonNodeVariables = Set("u"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
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
          nonInlinablePreFilters = None,
          groupNodes = Set(("a", "a"), ("b", "b"), ("c", "c"), ("d", "d")),
          groupRelationships = Set(("r", "r"), ("s", "s")),
          singletonNodeVariables = Set("v", "w"),
          singletonRelationshipVariables = Set.empty,
          selector = StatefulShortestPath.Selector.Shortest(1),
          nfa = expectedNfa,
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
          singletonNodeVariables = Set("v", "w"),
          singletonRelationshipVariables = Set("s"),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
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
          singletonNodeVariables = Set("v", "w"),
          singletonRelationshipVariables = Set("s"),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
          false
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("should plan non inlined predicates") {
    val query =
      """MATCH ANY SHORTEST ((u:User) ((a)-[r]->(b))+ (v)-[s]->(w) WHERE v.prop = w.prop AND size(a) <> 5)
        |RETURN *""".stripMargin
    val plan =
      planner.plan(query).stripProduceResults

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
        .statefulShortestPath(
          "u",
          "w",
          "SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v)-[s]->(w) WHERE NOT s IN `r` AND NOT size(`a`) IN [5] AND unique(`r`) AND v.prop = w.prop)",
          Some("v.prop = w.prop AND NOT size(a) = 5"),
          Set(("a", "a"), ("b", "b")),
          Set(("r", "r")),
          singletonNodeVariables = Set("v", "w"),
          singletonRelationshipVariables = Set("s"),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
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
          nonInlinablePreFilters = None,
          groupNodes = Set(("a", "a"), ("b", "b"), ("c", "c"), ("d", "d")),
          groupRelationships = Set(("r", "r"), ("s", "s")),
          singletonNodeVariables = Set("v", "w", "x"),
          singletonRelationshipVariables = Set("t"),
          selector = StatefulShortestPath.Selector.Shortest(1),
          nfa = expectedNfa,
          false
        )
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should not support a strict interior node of a shortest path pattern from a previous MATCH clause") {
    val query =
      """MATCH (b:User)
        |MATCH ANY SHORTEST (a)-->*(b)-->*(c)
        |RETURN *""".stripMargin

    an[InternalException] should be thrownBy planner.plan(query)
  }

  test("Should not support a strict interior relationship of a shortest path pattern from a previous MATCH clause") {
    val query =
      """MATCH ()-[r]->()
        |MATCH ANY SHORTEST (a)-[r]->(b)-->*(c)
        |RETURN *""".stripMargin

    an[InternalException] should be thrownBy planner.plan(query)
  }

  test("Should not support a strict interior node of a shortest path pattern to be equal to the start boundary node") {
    val query =
      """MATCH ANY SHORTEST (a)-->*(a)-->*(b)
        |RETURN *""".stripMargin

    an[InternalException] should be thrownBy planner.plan(query)
  }

  test("Should not support a strict interior node of a shortest path pattern to be equal to the end boundary node") {
    val query =
      """MATCH ANY SHORTEST (a)-->*(b)-->*(b)
        |RETURN *""".stripMargin

    an[InternalException] should be thrownBy planner.plan(query)
  }

  // Note: We don't have this test for repeated relationship variables, because they are handled by the
  // UnfulfillableQueryRewriter
  test("Should not support a strict interior node of a shortest path pattern to be repeated") {
    val query =
      """MATCH ANY SHORTEST (a)-->*(b)-->*(b)-->*(c)
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
          singletonNodeVariables = Set("v"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
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
          singletonNodeVariables = Set("v"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
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
      .addTransition(2, 3, "(m_inner) (anon_6)")
      .addFinalState(3)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .filter("anon_6 = m")
        .statefulShortestPath(
          "n",
          "anon_6",
          "SHORTEST 1 ((n) ((n_inner)-[r_inner]->(m_inner)){1, } (anon_6) WHERE unique(`r_inner`))",
          None,
          groupNodes = Set(("n_inner", "n_inner"), ("m_inner", "m_inner")),
          groupRelationships = Set(("r_inner", "r_inner")),
          singletonNodeVariables = Set("anon_6"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          false
        )
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test(
    "Should plan SHORTEST if both start and end are already bound with rewritten selection on boundary nodes"
  ) {
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
      .addTransition(2, 3, "(m_inner) (m WHERE m.prop = anon_6.prop)")
      .addTransition(3, 4, "(m)-[r2]->(anon_6)")
      .addFinalState(4)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .filter("anon_6 = o")
        .statefulShortestPath(
          "n",
          "anon_6",
          "SHORTEST 1 ((n) ((n_inner)-[r_inner]->(m_inner)){1, } (m)-[r2]->(anon_6) WHERE NOT r2 IN `r_inner` AND m.prop = `anon_6`.prop AND unique(`r_inner`))",
          None,
          groupNodes = Set(("n_inner", "n_inner"), ("m_inner", "m_inner")),
          groupRelationships = Set(("r_inner", "r_inner")),
          singletonNodeVariables = Set("m", "anon_6"),
          singletonRelationshipVariables = Set("r2"),
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
          false
        )
        .skip(1)
        .cartesianProduct()
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
          singletonNodeVariables = Set("v"),
          singletonRelationshipVariables = Set.empty,
          StatefulShortestPath.Selector.Shortest(1),
          nfa,
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
    val patternExpressionPredicate = NodeJuxtapositionPredicate(
      Some(Expand.VariablePredicate(
        varFor("v"),
        NestedPlanExistsExpression(
          plan = nestedPlan,
          solvedExpressionAsString =
            solvedNestedExpressionAsString
        )(pos)
      ))
    )
    val expectedNfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (a)")
      .addTransition(1, 2, "(a)-[r]->(b)")
      .addTransition(2, 1, "(b) (a)")
      .addTransition((2, "b"), (3, "v"), patternExpressionPredicate)
      .addTransition(3, 4, "(v)-[s]->(w)")
      .addFinalState(4)
      .build()

    plan should equal(
      planner.subPlanBuilder()
        .statefulShortestPath(
          "u",
          "w",
          s"SHORTEST 1 ((u) ((a)-[r]->(b)){1, } (v)-[s]->(w) WHERE $solvedNestedExpressionAsString AND NOT s IN `r` AND unique(`r`))",
          None,
          Set(("a", "a"), ("b", "b")),
          Set(("r", "r")),
          singletonNodeVariables = Set("v", "w"),
          singletonRelationshipVariables = Set("s"),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
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
          singletonNodeVariables = Set("w", "v", "x"),
          singletonRelationshipVariables = Set("t", "s"),
          StatefulShortestPath.Selector.Shortest(1),
          expectedNfa,
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
        Set("d"),
        Set.empty,
        StatefulShortestPath.Selector.Shortest(1),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a) (b)")
          .addTransition(1, 2, "(b)-[r]->(c)")
          .addTransition(2, 1, "(c) (b)")
          .addTransition(2, 3, "(c) (d)")
          .addFinalState(3)
          .build(),
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
        Set("f", "e", "b"),
        Set("anon_0", "anon_1"),
        StatefulShortestPath.Selector.Shortest(1),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a)-[anon_0]-(b)")
          .addTransition(1, 2, "(b) (c)")
          .addTransition(2, 3, "(c)-[r]->(d)")
          .addTransition(3, 2, "(d) (c)")
          .addTransition(3, 4, "(d) (e)")
          .addTransition(4, 5, "(e)-[anon_1]-(f)")
          .addFinalState(5)
          .build(),
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
        Set("b"),
        Set("r"),
        StatefulShortestPath.Selector.Shortest(1),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a)-[r]->(b)")
          .addFinalState(1)
          .build(),
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
        singletonNodeVariables = Set("b"),
        singletonRelationshipVariables = Set(),
        StatefulShortestPath.Selector.Shortest(1),
        nfaLR,
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
        singletonNodeVariables = Set("a"),
        singletonRelationshipVariables = Set(),
        StatefulShortestPath.Selector.Shortest(1),
        nfaRL,
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

}
