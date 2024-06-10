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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality

import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ShortestPathCardinalityIntegrationTest extends CypherFunSuite with CardinalityIntegrationTestSupport {

  private val allNodes: Double = 10
  private val stopNodes: Double = 7
  private val nextRelationships: Double = 11
  private val allRelationships: Double = nextRelationships
  private val stopNameExistsSelectivity: Double = 0.9
  private val nextServiceIdSelectivity: Double = 0.8

  private val configuration: StatisticsBackedLogicalPlanningConfiguration =
    plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Stop", stopNodes)
      .setAllRelationshipsCardinality(allRelationships)
      .setRelationshipCardinality("()-[:NEXT]->()", nextRelationships)
      .setRelationshipCardinality("(:Stop)-[:NEXT]->()", nextRelationships)
      .setRelationshipCardinality("()-[:NEXT]->(:Stop)", nextRelationships)
      .setRelationshipCardinality("(:Stop)-[:NEXT]->(:Stop)", nextRelationships)
      .addNodeIndex("Stop", List("name"), existsSelectivity = stopNameExistsSelectivity, uniqueSelectivity = 1.0)
      .addRelationshipIndex("NEXT", List("serviceId"), 1.0, nextServiceIdSelectivity)
      .build()

  private val relationshipUniqueness: Double = PlannerDefaults.DEFAULT_REL_UNIQUENESS_SELECTIVITY.factor
  private val distinctRelationships: Double = PlannerDefaults.DEFAULT_PREDICATE_SELECTIVITY.factor
  private val defaultPropertySelectivity: Double = PlannerDefaults.DEFAULT_PROPERTY_SELECTIVITY.factor
  private val defaultEqualitySelectivity: Double = PlannerDefaults.DEFAULT_EQUALITY_SELECTIVITY.factor

  // Cardinalities for the various iterations of the following pattern: (:Stop)((:Stop)-[:NEXT {serviceId: $value}]->(:Stop)){n}(:Stop)
  private val iteration_0: Double =
    stopNodes * stopNodes / allNodes // Bit of a quirk in cardinality estimation for QPPs, label :Stop is counted twice

  private val iteration_1: Double =
    nextRelationships * nextServiceIdSelectivity

  private val iteration_2: Double =
    iteration_1 * nextRelationships / stopNodes * nextServiceIdSelectivity * relationshipUniqueness

  private val iteration_3: Double =
    iteration_2 * nextRelationships / stopNodes * nextServiceIdSelectivity * Math.pow(relationshipUniqueness, 2)

  private val iteration_4: Double =
    iteration_3 * nextRelationships / stopNodes * nextServiceIdSelectivity * Math.pow(relationshipUniqueness, 3)

  private val iteration_5: Double =
    iteration_4 * nextRelationships / stopNodes * nextServiceIdSelectivity * Math.pow(relationshipUniqueness, 4)

  private val iterations_1_2: Double = iteration_1 + iteration_2
  private val iterations_1_3: Double = iterations_1_2 + iteration_3
  private val iterations_1_4: Double = iterations_1_3 + iteration_4
  private val iterations_1_5: Double = iterations_1_4 + iteration_5

  private val iterations_0_2: Double = iteration_0 + iterations_1_2
  private val iterations_0_3: Double = iteration_0 + iterations_1_3
  private val iterations_0_4: Double = iteration_0 + iterations_1_4

  test("ALL is standard cardinality") {
    queryShouldHaveCardinality(
      configuration,
      "MATCH ALL (from:Stop)((a:Stop)-[n:NEXT {serviceId: 1}]->(b:Stop)){1,5}(to:Stop) WHERE to.name IS NOT NULL",
      iterations_1_5 * stopNameExistsSelectivity
    )
  }

  // The pattern would produce, on average, more than one row per pair of nodes, but because of `ANY 1` we will return only one row per pair.
  test("ANY 1 is capped at 1 path per partition") {
    queryShouldHaveCardinality(
      configuration,
      "MATCH ANY 1 (from:Stop)((a:Stop)-[n:NEXT {serviceId: 1}]->(b:Stop)){1,5}(to:Stop) WHERE to.name IS NOT NULL",
      stopNodes * stopNodes * stopNameExistsSelectivity
    )
  }

  test("ANY 1 with a post-filter on a relationship") {
    queryShouldHaveCardinality(
      configuration,
      """MATCH ANY 1 (from:Stop)((a:Stop)-[n:NEXT]->(b:Stop)){1,5}(via:Stop)-[o:NEXT]->(to:Stop) WHERE o.serviceId = 1""",
      stopNodes * stopNodes * nextServiceIdSelectivity
    )
  }

  // However the same pattern would produce, on average, fewer than two rows per pair, and so we are going to return all the rows.
  // It's almost as if this pattern and statistics were not completely random…
  test("ANY 2 is slightly lower than 2 paths per partition") {
    queryShouldHaveCardinality(
      configuration,
      "MATCH ANY 2 (from:Stop)((a:Stop)-[n:NEXT {serviceId: 1}]->(b:Stop)){1,5}(to:Stop) WHERE to.name IS NOT NULL",
      iterations_1_5 * stopNameExistsSelectivity
    )
  }

  test("ANY 2 with two QPPs and a post filter") {
    queryShouldHaveCardinality(
      configuration,
      """MATCH ANY 2 (from:Stop)((a:Stop)-[n:NEXT {serviceId: 1}]->(b:Stop)){1,3}(via:Stop)((c:Stop)-[s:NEXT {serviceId: 2}]->(d:Stop)){0,2}(to:Stop)
        |WHERE via.name IS NOT NULL""".stripMargin,
      iterations_1_3 * iterations_0_2 / stopNodes * distinctRelationships * stopNameExistsSelectivity
    )
  }

  test("ANY 3 is lower than 3 paths per partition") {
    queryShouldHaveCardinality(
      configuration,
      "MATCH ANY 3 (from:Stop)((a:Stop)-[n:NEXT WHERE n.serviceId = 1]->(b:Stop)){1,5}(to:Stop) WHERE to.name IS NOT NULL",
      iterations_1_5 * stopNameExistsSelectivity
    )
  }

  test("SHORTEST 1 is equal to ANY 1") {
    queryShouldHaveCardinality(
      configuration,
      "MATCH SHORTEST 1 (from:Stop)((a:Stop)-[n:NEXT WHERE n.serviceId = 1]->(b:Stop)){1,5}(to:Stop) WHERE to.name IS NOT NULL",
      stopNodes * stopNodes * stopNameExistsSelectivity
    )
  }

  test("SHORTEST 2 is equal to ANY 2") {
    queryShouldHaveCardinality(
      configuration,
      "MATCH SHORTEST 2 (from:Stop)((a:Stop)-[n:NEXT WHERE n.serviceId = 1]->(b:Stop)){1,5}(to:Stop) WHERE to.name IS NOT NULL",
      iterations_1_5 * stopNameExistsSelectivity
    )
  }

  test("SHORTEST 3 is equal to ANY 3") {
    queryShouldHaveCardinality(
      configuration,
      "MATCH SHORTEST 3 (from:Stop)((a:Stop)-[n:NEXT WHERE n.serviceId = 1]->(b:Stop)){1,5}(to:Stop) WHERE to.name IS NOT NULL",
      iterations_1_5 * stopNameExistsSelectivity
    )
  }

  test("SHORTEST 1 GROUP stops expanding at {1, 4} as it produces more than 1 row per partition") {
    queryShouldHaveCardinality(
      configuration,
      "MATCH SHORTEST 1 GROUP (from:Stop)((a:Stop)-[n:NEXT WHERE n.serviceId = 1]->(b:Stop)){1,5}(to:Stop) WHERE to.name IS NOT NULL",
      iterations_1_4 * stopNameExistsSelectivity
    )
  }

  test("SHORTEST 2 GROUP expands all the way to {1, 5} as it produces fewer than 2 rows per partition") {
    queryShouldHaveCardinality(
      configuration,
      "MATCH SHORTEST 2 GROUP (from:Stop)((a:Stop)-[n:NEXT WHERE n.serviceId = 1]->(b:Stop)){1, 5}(to:Stop) WHERE to.name IS NOT NULL",
      iterations_1_5 * stopNameExistsSelectivity
    )
  }

  test("SHORTEST 1 GROUP with two QPPs and a post filter") {
    queryShouldHaveCardinality(
      configuration,
      """MATCH SHORTEST 1 GROUP (from:Stop)((a:Stop)-[n:NEXT {serviceId: 1}]->(b:Stop))*(via:Stop)((c:Stop)-[s:NEXT {serviceId: 2}]->(d:Stop)){0,2}(to:Stop)
        |WHERE via.name IS NOT NULL""".stripMargin,
      iterations_0_2 * iterations_0_2 / stopNodes * distinctRelationships * stopNameExistsSelectivity
    )
  }

  test("SHORTEST 2 GROUP with two QPPs and a post filter") {
    queryShouldHaveCardinality(
      configuration,
      """MATCH SHORTEST 2 GROUP (from:Stop)((a:Stop)-[n:NEXT {serviceId: 1}]->(b:Stop))*(via:Stop)((c:Stop)-[s:NEXT {serviceId: 2}]->(d:Stop)){0,2}(to:Stop)
        |WHERE via.name IS NOT NULL""".stripMargin,
      iterations_0_3 * iterations_0_2 / stopNodes * distinctRelationships * stopNameExistsSelectivity
    )
  }

  test("SHORTEST 2 GROUP with two unbounded QPPs and a post filter") {
    queryShouldHaveCardinality(
      configuration,
      """MATCH SHORTEST 2 GROUP (from:Stop)((a:Stop)-[n:NEXT {serviceId: 1}]->(b:Stop))*(via:Stop)((c:Stop)-[s:NEXT {serviceId: 2}]->(d:Stop))*(to:Stop)
        |WHERE via.name IS NOT NULL""".stripMargin,
      iterations_0_3 * iterations_0_3 / stopNodes * distinctRelationships * stopNameExistsSelectivity
    )
  }

  test("SHORTEST 2 GROUP with two QPPs and a pre filter") {
    queryShouldHaveCardinality(
      configuration,
      """MATCH SHORTEST 2 GROUP ((from:Stop)((a:Stop)-[n:NEXT {serviceId: 1}]->(b:Stop))*(via:Stop)((c:Stop)-[s:NEXT {serviceId: 2}]->(d:Stop)){0,2}(to:Stop)
        |WHERE via.name IS NOT NULL)""".stripMargin,
      iterations_0_4 * iterations_0_2 / stopNodes * distinctRelationships * stopNameExistsSelectivity
    )
  }

  test("SHORTEST with boundary node predicate should not count predicate selectivity twice") {
    val unindexedProp = DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_EQUALITY_SELECTIVITY

    val query = """MATCH (a) MATCH ANY SHORTEST ((u)((n {prop: 5})-[r]->(m)){1,2}(v))"""

    // (u)((n {prop: 5})-[r]->(m)){1}(v)
    val qpp1 = allRelationships * unindexedProp

    // (u)((n {prop: 5})-[r]->(m)){2}(v)
    val qpp2 = (qpp1 * qpp1 / allNodes * relationshipUniqueness)

    // (u)((n {prop: 5})-[r]->(m)){1,2}(v)
    val qpp = qpp1 + qpp2

    val expectedCardinality = allNodes * qpp

    queryShouldHaveCardinality(configuration, query, expectedCardinality)
  }

  // The resulting cardinality should be the same as in the following test: "SHORTEST with inlined property predicate referencing two nodes within the qpp"
  test("SHORTEST should inline predicates with source node references but currently does not do that") {

    val query = """MATCH (a) MATCH ANY SHORTEST ((u)((n WHERE a.prop = n.prop)-[r]->(m)){0,2}(v))"""

    // (a)
    val match1 = allNodes

    // (u)((n)-[r]->(m)){0}(v) -- for illustration purposes, not a legal pattern
    val qpp0 = allNodes // technically: (u) * (v) / ()

    // (u)((n)-[r]->(m)){1}(v)
    val qpp1 = allRelationships

    // (u)((n)-[r]->(m)){2}(v)
    val qpp2 = allRelationships * allRelationships / allNodes * relationshipUniqueness

    // (u)((n)-[r]->(m)){0,2}(v)
    val qpp = qpp0 + qpp1 + qpp2

    /**
     * This is the crux, the pattern:
     *   {{{ (u)((n WHERE a.prop = n.prop)-[r]->(m)){0,2}(v) }}}
     * gets rewritten to:
     *   {{{ (u)((n)-[r]->(m)){0,2}(v) WHERE all(i IN range(0, size(n) - 1) WHERE a.prop = n[i].prop) }}}
     *
     * We should turn it back into its original form for cardinality estimation, but we don't in this case because of the dependency on node `a`.
     * Note that `all(…)` here is a special expression, a [[org.neo4j.cypher.internal.ir.ast.ForAllRepetitions]], and has a selectivity of 1.
     *
     * This means that our pattern:
     *   {{{ (u)((n WHERE a.prop = n.prop)-[r]->(m)){0,2}(v) }}}
     * gets estimated, incorrectly, as:
     *   {{{ (u)((n)-[r]->(m)){0,2}(v) }}}
     */
    val spp = qpp

    // (u), (v)
    val sppEndpoints = allNodes * allNodes

    // ANY SHORTEST ((u)((n)-[r]->(m)){0,2}(v) WHERE all(i IN range(0, size(n) - 1) WHERE a.prop = n[i].prop))
    val match2 = math.min(spp, sppEndpoints)

    val expectedCardinality = match1 * match2

    queryShouldHaveCardinality(configuration, query, expectedCardinality)
  }

  test("SHORTEST with inlined property predicate referencing two nodes within the qpp") {

    val query = """MATCH (a) MATCH ANY SHORTEST (u)((n)-[r]->(m) WHERE n.prop = m.prop){0,2}(v)"""

    // (a)
    val match1 = allNodes

    // WHERE n.prop = m.prop
    val propertyEqualitySelectivity = defaultPropertySelectivity * defaultEqualitySelectivity

    // (n)-[r]->(m) WHERE n.prop = m.prop
    val relationshipPattern = allRelationships * propertyEqualitySelectivity

    // (u)((n)-[r]->(m) WHERE n.prop = m.prop){0}(v) -- for illustration purposes, not a legal pattern
    val qpp0 = allNodes // technically: (u) * (v) / ()

    // (u)((n)-[r]->(m) WHERE n.prop = m.prop){1}(v)
    val qpp1 = relationshipPattern

    // (u)((n)-[r]->(m) WHERE n.prop = m.prop){2}(v)
    val qpp2 = relationshipPattern * relationshipPattern / allNodes * relationshipUniqueness

    // (u)((n)-[r]->(m) WHERE n.prop = m.prop){0,2}(v)
    val qpp = qpp0 + qpp1 + qpp2

    /**
     * The pattern:
     *   {{{ (u)((n)-[r]->(m) WHERE n.prop = m.prop){0,2}(v) }}}
     * gets rewritten to:
     *   {{{ (u)((n)-[r]->(m)){0,2}(v) WHERE all(i IN range(0, size(n) - 1) WHERE n[i].prop = m[i].prop) }}}
     * however it gets turned back into its original form for cardinality estimation.
     */
    val spp = qpp

    // (u), (v)
    val sppEndpoints = allNodes * allNodes

    // ANY SHORTEST (u)((n)-[r]->(m) WHERE n.prop = m.prop){0,2}(v)
    val match2 = math.min(spp, sppEndpoints)

    val expectedCardinality = match1 * match2

    queryShouldHaveCardinality(configuration, query, expectedCardinality)
  }
}
