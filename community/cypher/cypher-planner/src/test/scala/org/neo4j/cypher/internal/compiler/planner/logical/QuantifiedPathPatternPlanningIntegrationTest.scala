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
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.AssertIsNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class QuantifiedPathPatternPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  private def disjoint(lhs: String, rhs: String, unnamedOffset: Int = 0): String =
    s"NONE(anon_$unnamedOffset IN $lhs WHERE anon_$unnamedOffset IN $rhs)"

  private val planner = plannerBuilder()
    .setAllNodesCardinality(100)
    .setAllRelationshipsCardinality(40)
    .setLabelCardinality("User", 5)
    .setLabelCardinality("N", 6)
    .setLabelCardinality("NN", 5)
    .setRelationshipCardinality("()-[:R]->()", 10)
    .setRelationshipCardinality("()-[:S]->()", 10)
    .setRelationshipCardinality("()-[:T]->()", 10)
    .setRelationshipCardinality("(:N)-[]->()", 10)
    .setRelationshipCardinality("(:N)-[]->(:N)", 10)
    .setRelationshipCardinality("(:N)-[]->(:NN)", 10)
    .setRelationshipCardinality("(:NN)-[]->()", 10)
    .setRelationshipCardinality("(:NN)-[]->(:N)", 10)
    .setRelationshipCardinality("(:NN)-[]->(:NN)", 10)
    .setRelationshipCardinality("(:User)-[]->()", 10)
    .setRelationshipCardinality("(:User)-[]->(:N)", 10)
    .setRelationshipCardinality("(:User)-[]->(:NN)", 10)
    .setRelationshipCardinality("()-[]->(:User)", 10)
    .setRelationshipCardinality("(:User)-[:R]->()", 10)
    .setRelationshipCardinality("(:N)-[:R]->()", 10)
    .addSemanticFeature(SemanticFeature.QuantifiedPathPatterns)
    .build()

  test("should use correctly Namespaced variables") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setAllRelationshipsCardinality(10)
      .enableDeduplicateNames(enable = false)
      .addSemanticFeature(SemanticFeature.QuantifiedPathPatterns)
      .build()

    val query = "MATCH (a)((n)-[r]->())*(b) RETURN n, r"
    val plan = planner.plan(query).stripProduceResults
    val `(a)((n)-[r]-())*(b)` = TrailParameters(
      min = 0,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "  n@1",
      innerEnd = "  UNNAMED0",
      groupNodes = Set(("  n@1", "  n@3")),
      groupRelationships = Set(("  r@2", "  r@4")),
      innerRelationships = Set("  r@2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`(a)((n)-[r]-())*(b)`)
        .|.expand("(`  n@1`)-[`  r@2`]->(`  UNNAMED0`)")
        .|.argument("  n@1")
        .allNodeScan("a")
        .build()
    )
  }

  test("Should plan quantifier * with start node") {
    val query = "MATCH (u:User)((n)-[]->(m))* RETURN n, m"
    val plan = planner.plan(query).stripProduceResults
    val `(u)((n)-[]-(m))*` = TrailParameters(
      min = 0,
      max = Unlimited,
      start = "u",
      end = "anon_1",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_3", "anon_7")),
      innerRelationships = Set("anon_3"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`(u)((n)-[]-(m))*`)
        .|.expand("(n)-[anon_3]->(m)")
        .|.argument("n")
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should plan quantifier * with end node") {
    val query = "MATCH ((n)-[]->(m))*(u:User) RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))*(u)` = TrailParameters(
      min = 0,
      max = Unlimited,
      start = "u",
      end = "anon_0",
      innerStart = "m",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_3", "anon_7")),
      innerRelationships = Set("anon_3"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m))*(u)`)
        .|.expand("(m)<-[anon_3]-(n)")
        .|.argument("m")
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should plan quantifier +") {
    val query = "MATCH ((n)-[]->(m))+ RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_0",
      end = "anon_2",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_4", "anon_8")),
      innerRelationships = Set("anon_4"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m))+`)
        .|.expand("(n)-[anon_4]->(m)")
        .|.argument("n")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test("Should plan quantifier {1,}") {
    val query = "MATCH ((n)-[]->(m)){1,} RETURN n, m"

    val plan = planner.plan(query).stripProduceResults

    val `((n)-[]->(m)){1,}` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_0",
      end = "anon_2",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_4", "anon_8")),
      innerRelationships = Set("anon_4"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )
    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m)){1,}`)
        .|.expand("(n)-[anon_4]->(m)")
        .|.argument("n")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test("Should plan quantifier {,5} with start node") {
    val query = "MATCH ()((n)-[]->(m)){,5} RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `()((n)-[]->(m)){,5}` = TrailParameters(
      min = 0,
      max = UpperBound.Limited(5),
      start = "anon_0",
      end = "anon_2",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_4", "anon_8")),
      innerRelationships = Set("anon_4"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`()((n)-[]->(m)){,5}`)
        .|.expand("(n)-[anon_4]->(m)")
        .|.argument("n")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test("Should plan quantifier {1,5} with end node") {
    val query = "MATCH ((n)-[]->(m)){1,5} RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m)){1,5}` = TrailParameters(
      min = 1,
      max = UpperBound.Limited(5),
      start = "anon_0",
      end = "anon_2",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_4", "anon_8")),
      innerRelationships = Set("anon_4"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m)){1,5}`)
        .|.expand("(n)-[anon_4]->(m)")
        .|.argument("n")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test(
    "Should plan mix of quantified and non-quantified path patterns, expanding the non-quantified relationship first"
  ) {
    val query = "MATCH ((n)-[]->(m))+ (a)--(b) RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))+(a)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "anon_0",
      innerStart = "m",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_4", "anon_8")),
      innerRelationships = Set("anon_4"),
      previouslyBoundRelationships = Set("anon_2"),
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m))+(a)`)
        .|.expandAll("(m)<-[anon_4]-(n)")
        .|.argument("m")
        .allRelationshipsScan("(a)-[anon_2]-(b)")
        .build()
    )
  }

  test("Should plan mix of quantified and non-quantified path patterns, expanding the quantified one first") {
    val query = "MATCH (:User) ((n)-[]->(m))+ (a)--(b) RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))+(a)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_0",
      end = "a",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_4", "anon_8")),
      innerRelationships = Set("anon_4"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .filter("not anon_2 in anon_8")
        .expandAll("(a)-[anon_2]-(b)")
        .trail(`((n)-[]->(m))+(a)`)
        .|.expandAll("(n)-[anon_4]->(m)")
        .|.argument("n")
        .nodeByLabelScan("anon_0", "User")
        .build()
    )
  }

  test("Should plan mix of quantified path pattern and two non-quantified relationships") {
    val query = "MATCH ((n)-[r1]->(m)-[r2]->(o))+ (a)-[r3]-(b)<-[r4]-(c:N) RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[r1]->(m)-[r2]->(o))+(a)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "anon_0",
      innerStart = "o",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m"), ("o", "o")),
      groupRelationships = Set(("r2", "r2"), ("r1", "r1")),
      innerRelationships = Set("r1", "r2"),
      previouslyBoundRelationships = Set("r3", "r4"),
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[r1]->(m)-[r2]->(o))+(a)`)
        .|.filter("not r2 = r1")
        .|.expandAll("(m)<-[r1]-(n)")
        .|.expandAll("(o)<-[r2]-(m)")
        .|.argument("o")
        .filter("not r4 = r3")
        .expandAll("(b)-[r3]-(a)")
        .expandAll("(c)-[r4]->(b)")
        .nodeByLabelScan("c", "N")
        .build()
    )
  }

  test(
    "Should plan mix of quantified path pattern and two non-quantified relationships of which some are provably disjoint"
  ) {
    val query = "MATCH ((n)-[r1:R]->(m)-[r2]->(o))+ (a)-[r3:T]-(b)<-[r4]-(c:N) RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[r1:R]->(m)-[r2]->(o))+(a)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "a",
      end = "anon_0",
      innerStart = "o",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m"), ("o", "o")),
      groupRelationships = Set(("r2", "r2"), ("r1", "r1")),
      innerRelationships = Set("r1", "r2"),
      previouslyBoundRelationships = Set("r3", "r4"),
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[r1:R]->(m)-[r2]->(o))+(a)`)
        .|.filter("not r2 = r1")
        .|.expandAll("(m)<-[r1:R]-(n)")
        .|.expandAll("(o)<-[r2]-(m)")
        .|.argument("o")
        .filter("not r4 = r3")
        .expandAll("(b)-[r3:T]-(a)")
        .expandAll("(c)-[r4]->(b)")
        .nodeByLabelScan("c", "N")
        .build()
    )
  }

  test("Should plan two consecutive quantified path patterns") {
    val query = "MATCH (u:User) ((n)-[]->(m))+ ((a)--(b))+ RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `(u) ((n)-[]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "u",
      end = "anon_1",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_5", "anon_9")),
      innerRelationships = Set("anon_5"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )
    val `((a)--(b))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_1",
      end = "anon_3",
      innerStart = "a",
      innerEnd = "b",
      groupNodes = Set(("a", "a"), ("b", "b")),
      groupRelationships = Set(("anon_11", "anon_15")),
      innerRelationships = Set("anon_11"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set("anon_9")
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((a)--(b))+`)
        .|.expandAll("(a)-[anon_11]-(b)")
        .|.argument("a")
        .trail(`(u) ((n)-[]->(m))+`)
        .|.expandAll("(n)-[anon_5]->(m)")
        .|.argument("n")
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should plan two consecutive quantified path patterns with a join") {
    val query = "MATCH (u:User&N) ((n)-[r]->(m))+ (x) ((a)-[r2]-(b))+ (v:User) USING JOIN ON x RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `(u) ((n)-[r]->(m))+ (x)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "u",
      end = "x",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )
    val `(x) ((a)-[r2]-(b))+ (v)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "v",
      end = "x",
      innerStart = "b",
      innerEnd = "a",
      groupNodes = Set(("a", "a"), ("b", "b")),
      groupRelationships = Set(("r2", "r2")),
      innerRelationships = Set("r2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .filter(disjoint("r", "r2", 18))
        .nodeHashJoin("x")
        .|.trail(`(x) ((a)-[r2]-(b))+ (v)`)
        .|.|.expandAll("(b)-[r2]-(a)")
        .|.|.argument("b")
        .|.nodeByLabelScan("v", "User")
        .trail(`(u) ((n)-[r]->(m))+ (x)`)
        .|.expandAll("(n)-[r]->(m)")
        .|.argument("n")
        .filter("u:N")
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should plan two consecutive quantified path patterns with different relationship types") {
    val query = "MATCH (u:User) ((n)-[:R]->(m))+ ((a)-[:T]-(b))+ RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `(u) ((n)-[]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "u",
      end = "anon_1",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_5", "anon_9")),
      innerRelationships = Set("anon_5"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )
    val `((a)-[]->(b))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_1",
      end = "anon_3",
      innerStart = "a",
      innerEnd = "b",
      groupNodes = Set(("a", "a"), ("b", "b")),
      groupRelationships = Set(("anon_11", "anon_15")),
      innerRelationships = Set("anon_11"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((a)-[]->(b))+`)
        .|.expandAll("(a)-[anon_11:T]-(b)")
        .|.argument("a")
        .trail(`(u) ((n)-[]->(m))+`)
        .|.expandAll("(n)-[anon_5:R]->(m)")
        .|.argument("n")
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should plan two quantified path pattern with some provably disjoint relationships") {
    val query = "MATCH ((n)-[r1:R]->(m)-[r2]->(o))+ ((a)-[r3:T]-(b)<-[r4]-(c))+ (:N) RETURN n, m"

    val `((n)-[r1:R]->(m)-[r2]->(o))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_1",
      end = "anon_0",
      innerStart = "o",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m"), ("o", "o")),
      groupRelationships = Set(("r2", "r2"), ("r1", "r1")),
      innerRelationships = Set("r1", "r2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set("r3", "r4")
    )
    val `((a)-[r3:T]-(b)<-[r4]-(c))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_2",
      end = "anon_1",
      innerStart = "c",
      innerEnd = "a",
      groupNodes = Set(("a", "a"), ("b", "b"), ("c", "c")),
      groupRelationships = Set(("r3", "r3"), ("r4", "r4")),
      innerRelationships = Set("r4", "r3"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[r1:R]->(m)-[r2]->(o))+`)
        .|.filter("not r2 = r1")
        .|.expandAll("(m)<-[r1:R]-(n)")
        .|.expandAll("(o)<-[r2]-(m)")
        .|.argument("o")
        .trail(`((a)-[r3:T]-(b)<-[r4]-(c))+`)
        .|.filter("not r4 = r3")
        .|.expandAll("(b)-[r3:T]-(a)")
        .|.expandAll("(c)-[r4]->(b)")
        .|.argument("c")
        .nodeByLabelScan("anon_2", "N")
        .build()
    )
  }

  test("Should plan two quantified path pattern with partial overlap in relationship types") {
    val query = "MATCH ((n)-[r1:R]->(m)-[r2:S]->(o))+ ((a)-[r3:T]-(b)<-[r4:R]-(c))+ (:N) RETURN n, m"

    val `((n)-[r1:R]->(m)-[r2]->(o))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_1",
      end = "anon_0",
      innerStart = "o",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m"), ("o", "o")),
      groupRelationships = Set(("r2", "r2"), ("r1", "r1")),
      innerRelationships = Set("r1", "r2"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set("r4")
    )
    val `((a)-[r3:T]-(b)<-[r4]-(c))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_2",
      end = "anon_1",
      innerStart = "c",
      innerEnd = "a",
      groupNodes = Set(("a", "a"), ("b", "b"), ("c", "c")),
      groupRelationships = Set(("r3", "r3"), ("r4", "r4")),
      innerRelationships = Set("r4", "r3"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[r1:R]->(m)-[r2]->(o))+`)
        .|.expandAll("(m)<-[r1:R]-(n)")
        .|.expandAll("(o)<-[r2:S]-(m)")
        .|.argument("o")
        .trail(`((a)-[r3:T]-(b)<-[r4]-(c))+`)
        .|.expandAll("(b)-[r3:T]-(a)")
        .|.expandAll("(c)-[r4:R]->(b)")
        .|.argument("c")
        .nodeByLabelScan("anon_2", "N")
        .build()
    )
  }

  test("Should plan quantified path pattern with a WHERE clause") {
    val query = "MATCH ((n)-[]->(m) WHERE n.prop > m.prop)+ RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_0",
      end = "anon_2",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_4", "anon_8")),
      innerRelationships = Set("anon_4"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m))+`)
        .|.filter("n.prop > m.prop")
        .|.expandAll("(n)-[anon_4]->(m)")
        .|.argument("n")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test("Should plan quantified path pattern with a WHERE clause with references to previous MATCH") {
    val query =
      "MATCH (a) WITH a.prop AS prop MATCH ((n)-[]->(m) WHERE n.prop > prop)+ RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_2",
      end = "anon_0",
      innerStart = "m",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_4", "anon_8")),
      innerRelationships = Set("anon_4"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.trail(`((n)-[]->(m))+`)
        .|.|.filter("n.prop > prop")
        .|.|.expandAll("(m)<-[anon_4]-(n)")
        .|.|.argument("m", "prop")
        .|.allNodeScan("anon_2", "prop")
        .projection("a.prop AS prop")
        .allNodeScan("a")
        .build()
    )
  }

  test("Should plan quantified path pattern with a WHERE clause with multiple references to previous MATCH") {
    val query = "MATCH (a), (b) MATCH (a) ((n)-[r]->(m) WHERE n.prop > a.prop AND n.prop > b.prop)+ (b) RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[r]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "b",
      end = "anon_9",
      innerStart = "m",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .filter("anon_9 = a")
        .trail(`((n)-[r]->(m))+`)
        .|.filter("cacheNFromStore[n.prop] > cacheN[a.prop]", "cacheNFromStore[n.prop] > cacheN[b.prop]")
        .|.expandAll("(m)<-[r]-(n)")
        .|.argument("m", "a", "b")
        .cartesianProduct()
        .|.cacheProperties("cacheNFromStore[b.prop]")
        .|.allNodeScan("b")
        .cacheProperties("cacheNFromStore[a.prop]")
        .allNodeScan("a")
        .build()
    )
  }

  test("Should plan quantified path pattern with inlined predicates") {
    val query = "MATCH (:User) ((n:N&NN {prop: 5} WHERE n.foo > 0)-[r:!REL WHERE r.prop > 0]->(m))+ RETURN n, m"

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[r]->(m))+` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_0",
      end = "anon_1",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[r]->(m))+`)
        .|.filter("not r:REL", "r.prop > 0")
        .|.expandAll("(n)-[r]->(m)")
        .|.filter("n:N", "n:NN", "n.prop = 5", "n.foo > 0")
        .|.argument("n")
        .nodeByLabelScan("anon_0", "User")
        .build()
    )
  }

  test("Should plan quantified path pattern if both start and end are already bound") {
    val query =
      s"""
         |MATCH (n), (m)
         |WITH * SKIP 1
         |MATCH (n)((n_inner)-[r_inner]->(m_inner))+ (m)
         |RETURN n, m
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val `(n)((n_inner)-[r_inner]->(m_inner))+ (m)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "n",
      end = "anon_8",
      innerStart = "n_inner",
      innerEnd = "m_inner",
      groupNodes = Set(("n_inner", "n_inner"), ("m_inner", "m_inner")),
      groupRelationships = Set(("r_inner", "r_inner")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .filter("anon_8 = m")
        .trail(`(n)((n_inner)-[r_inner]->(m_inner))+ (m)`)
        .|.expandAll("(n_inner)-[r_inner]->(m_inner)")
        .|.argument("n_inner", "m", "n")
        .skip(1)
        .cartesianProduct()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test("Should not plan assertNode with quantified path pattern in OPTIONAL MATCH") {
    val array = (1 to 10).mkString("[", ",", "]")

    val query =
      s"""
         |MATCH (n1)
         |UNWIND $array AS a0
         |OPTIONAL MATCH (n1) ((inner1)-[:R]->(inner2))+ (n2)
         |WITH a0
         |RETURN a0 ORDER BY a0
         |""".stripMargin

    // Plan for the OPTIONAL MATCH should not contain a .filter(assertIsNode("n1")),
    // since n1 is not a single node pattern, but a concatenated path.
    val plan = planner.plan(query).stripProduceResults
    plan.folder.treeFindByClass[AssertIsNode] should be(None)
  }

  test("should plan quantified relationship") {
    val query =
      s"""
         |MATCH (n)-[r]->+(m)
         |RETURN n, m
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val `(n)-[r]->+(m)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "n",
      end = "m",
      innerStart = "anon_0",
      innerEnd = "anon_1",
      groupNodes = Set(),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set(),
      previouslyBoundRelationshipGroups = Set()
    )

    plan shouldBe planner.subPlanBuilder()
      .trail(`(n)-[r]->+(m)`)
      .|.expandAll("(anon_0)-[r]->(anon_1)")
      .|.argument("anon_0")
      .allNodeScan("n")
      .build()
  }

  test("should plan quantified relationship with predicates") {
    val query =
      s"""
         |MATCH (n)-[r:R WHERE r.prop > 123]->{2,5}(m)
         |RETURN n, m
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val `(n)-[r:R WHERE r.prop > 123]->{2,5}(m)` = TrailParameters(
      min = 2,
      max = UpperBound.Limited(5),
      start = "n",
      end = "m",
      innerStart = "anon_0",
      innerEnd = "anon_1",
      groupNodes = Set(),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set(),
      previouslyBoundRelationshipGroups = Set()
    )

    plan shouldBe planner.subPlanBuilder()
      .trail(`(n)-[r:R WHERE r.prop > 123]->{2,5}(m)`)
      .|.filter("r.prop > 123")
      .|.expandAll("(anon_0)-[r:R]->(anon_1)")
      .|.argument("anon_0")
      .allNodeScan("n")
      .build()
  }

  test("should plan an index seek with multiple predicates on RHS of Trail") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(5000)
      .setLabelCardinality("A", 10)
      .setRelationshipCardinality("()-[:REL]->()", 5000)
      .setRelationshipCardinality("(:A)-[:REL]->()", 10)
      .addNodeIndex("A", Seq("prop"), 0.5, 0.5)
      .addSemanticFeature(SemanticFeature.QuantifiedPathPatterns)
      .build()

    val q = "MATCH (n) ((x)<-[r1:REL]-(a:A WHERE a.prop > 100 AND a.prop < 123)-[r2:REL]->(y))+ (m) RETURN *"

    val plan = planner.plan(q).stripProduceResults
    val params = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "n",
      end = "m",
      innerStart = "x",
      innerEnd = "y",
      groupNodes = Set(("y", "y"), ("x", "x"), ("a", "a")),
      groupRelationships = Set(("r2", "r2"), ("r1", "r1")),
      innerRelationships = Set("r1", "r2"),
      previouslyBoundRelationships = Set(),
      previouslyBoundRelationshipGroups = Set()
    )

    plan shouldBe planner.subPlanBuilder()
      .trail(params)
      .|.filter("not r2 = r1")
      .|.expandAll("(a)-[r2:REL]->(y)")
      .|.expandInto("(x)<-[r1:REL]-(a)")
      .|.nodeIndexOperator("a:A(100 < prop < 123)", argumentIds = Set("x"))
      .allNodeScan("n")
      .build()
  }

  test("Should start with higher cardinality label if first expansion is cheaper") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(160)
      .setAllRelationshipsCardinality(1000)
      .setLabelCardinality("B", 100)
      .setLabelCardinality("C", 10)
      .setRelationshipCardinality("(:B)-[:R]->(:C)", 2)
      .setRelationshipCardinality("(:B)-[:R]->()", 2)
      .setRelationshipCardinality("()-[:R]->(:C)", 500)
      .setRelationshipCardinality("()-[:R]->()", 500)
      .addSemanticFeature(SemanticFeature.QuantifiedPathPatterns)
      .build()

    val query = "MATCH (b:B) ( (x)-[r:R]->(y) ){3} (c:C) RETURN *"

    val plan = planner.plan(query).stripProduceResults

    val trailParameters = TrailParameters(
      min = 3,
      max = UpperBound.Limited(3),
      start = "b",
      end = "c",
      innerStart = "x",
      innerEnd = "y",
      groupNodes = Set(("x", "x"), ("y", "y")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set(),
      previouslyBoundRelationshipGroups = Set()
    )

    plan shouldBe planner.subPlanBuilder()
      .filter("c:C")
      .trail(trailParameters)
      .|.expandAll("(x)-[r:R]->(y)")
      .|.argument("x")
      .nodeByLabelScan("b", "B")
      .build()
  }

  test("should go into trail with lower cardinality first") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(10000)
      .setLabelCardinality("X", 40)
      .setLabelCardinality("A", 500)
      .setLabelCardinality("B", 800)
      .setLabelCardinality("N", 500)
      .setLabelCardinality("M", 500)
      .setLabelCardinality("P", 20)
      .setLabelCardinality("Q", 30)
      .setRelationshipCardinality("()-[:REL]->()", 10000)
      .setRelationshipCardinality("(:N)-[:REL]->()", 500)
      .setRelationshipCardinality("()-[:REL]->(:M)", 500)
      .setRelationshipCardinality("(:A)-[:REL]->(:M)", 500)
      .setRelationshipCardinality("(:A)-[:REL]->(:N)", 500)
      .setRelationshipCardinality("(:N)-[:REL]->(:M)", 500)
      .setRelationshipCardinality("(:N)-[:REL]->(:N)", 500)
      .setRelationshipCardinality("(:M)-[:REL]->(:M)", 500)
      .setRelationshipCardinality("(:M)-[:REL]->(:N)", 500)
      .setRelationshipCardinality("(:N)-[:REL]->(:X)", 500)
      .setRelationshipCardinality("(:M)-[:REL]->(:X)", 500)
      .setRelationshipCardinality("(:P)-[:REL]->()", 10)
      .setRelationshipCardinality("()-[:REL]->(:Q)", 10)
      .setRelationshipCardinality("(:P)-[:REL]->(:Q)", 10)
      .setRelationshipCardinality("(:P)-[:REL]->(:P)", 10)
      .setRelationshipCardinality("(:P)-[:REL]->(:B)", 10)
      .setRelationshipCardinality("(:Q)-[:REL]->(:Q)", 10)
      .setRelationshipCardinality("(:Q)-[:REL]->(:P)", 10)
      .setRelationshipCardinality("(:Q)-[:REL]->(:B)", 10)
      .setRelationshipCardinality("(:X)-[:REL]->(:Q)", 10)
      .setRelationshipCardinality("(:X)-[:REL]->(:P)", 10)
      .addSemanticFeature(SemanticFeature.QuantifiedPathPatterns)
      .build()

    val query =
      """
        |MATCH (a:A) ((n:N)-[r1:REL]->(m:M)){3} (x:X) ((p:P)-[r2:REL]->(q:Q)){3} (b:B) 
        |RETURN *""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val n_r1_m_trail = TrailParameters(
      min = 3,
      max = UpperBound.Limited(3),
      start = "x",
      end = "a",
      innerStart = "m",
      innerEnd = "n",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("r1", "r1")),
      innerRelationships = Set("r1"),
      previouslyBoundRelationships = Set(),
      previouslyBoundRelationshipGroups = Set("r2")
    )

    val p_r2_q_trail = TrailParameters(
      min = 3,
      max = UpperBound.Limited(3),
      start = "x",
      end = "b",
      innerStart = "p",
      innerEnd = "q",
      groupNodes = Set(("p", "p"), ("q", "q")),
      groupRelationships = Set(("r2", "r2")),
      innerRelationships = Set("r2"),
      previouslyBoundRelationships = Set(),
      previouslyBoundRelationshipGroups = Set()
    )

    plan shouldBe planner.subPlanBuilder()
      .filter("a:A")
      .trail(n_r1_m_trail)
      .|.filter("n:N")
      .|.expandAll("(m)<-[r1:REL]-(n)")
      .|.filter("m:M")
      .|.argument("m")
      .filter("b:B")
      .trail(p_r2_q_trail)
      .|.filter("q:Q")
      .|.expandAll("(p)-[r2:REL]->(q)")
      .|.filter("p:P")
      .|.argument("p")
      .nodeByLabelScan("x", "X")
      .build()
  }
}
