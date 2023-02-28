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
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.ListSet

class QuantifiedPathPatternPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  private def disjoint(lhs: String, rhs: String, unnamedOffset: Int = 0): String =
    s"NONE(anon_$unnamedOffset IN $lhs WHERE anon_$unnamedOffset IN $rhs)"

  private val planner = plannerBuilder()
    .enablePlanningIntersectionScans()
    .setAllNodesCardinality(100)
    .setAllRelationshipsCardinality(40)
    .setLabelCardinality("User", 4)
    .setLabelCardinality("N", 6)
    .setLabelCardinality("NN", 5)
    .setRelationshipCardinality("()-[:R]->()", 10)
    .setRelationshipCardinality("()-[:S]->()", 10)
    .setRelationshipCardinality("()-[:T]->()", 10)
    .setRelationshipCardinality("(:N)-[]->()", 10)
    .setRelationshipCardinality("(:N)-[]->(:N)", 10)
    .setRelationshipCardinality("(:N)-[]->(:NN)", 10)
    .setRelationshipCardinality("(:NN)-[]->()", 10)
    .setRelationshipCardinality("()-[]->(:NN)", 10)
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`(a)((n)-[r]-())*(b)`)
        .|.filterExpression(isRepeatTrailUnique("  r@2"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`(u)((n)-[]-(m))*`)
        .|.filterExpression(isRepeatTrailUnique("anon_3"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m))*(u)`)
        .|.filterExpression(isRepeatTrailUnique("anon_3"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m))+`)
        .|.filterExpression(isRepeatTrailUnique("anon_4"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )
    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m)){1,}`)
        .|.filterExpression(isRepeatTrailUnique("anon_4"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`()((n)-[]->(m)){,5}`)
        .|.filterExpression(isRepeatTrailUnique("anon_4"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m)){1,5}`)
        .|.filterExpression(isRepeatTrailUnique("anon_4"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m))+(a)`)
        .|.filterExpression(isRepeatTrailUnique("anon_4"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    plan should equal(
      planner.subPlanBuilder()
        .filter("not anon_2 in anon_8")
        .expandAll("(a)-[anon_2]-(b)")
        .trail(`((n)-[]->(m))+(a)`)
        .|.filterExpression(isRepeatTrailUnique("anon_4"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[r1]->(m)-[r2]->(o))+(a)`)
        .|.filterExpressionOrString("not r2 = r1", isRepeatTrailUnique("r1"))
        .|.expandAll("(m)<-[r1]-(n)")
        .|.filterExpression(isRepeatTrailUnique("r2"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[r1:R]->(m)-[r2]->(o))+(a)`)
        .|.filterExpressionOrString("not r2 = r1", isRepeatTrailUnique("r1"))
        .|.expandAll("(m)<-[r1:R]-(n)")
        .|.filterExpression(isRepeatTrailUnique("r2"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
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
      previouslyBoundRelationshipGroups = Set("anon_9"),
      reverseGroupVariableProjections = false
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((a)--(b))+`)
        .|.filterExpression(isRepeatTrailUnique("anon_11"))
        .|.expandAll("(a)-[anon_11]-(b)")
        .|.argument("a")
        .trail(`(u) ((n)-[]->(m))+`)
        .|.filterExpression(isRepeatTrailUnique("anon_5"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true
    )

    plan should equal(
      planner.subPlanBuilder()
        .filter(disjoint("r", "r2", 20))
        .nodeHashJoin("x")
        .|.trail(`(x) ((a)-[r2]-(b))+ (v)`)
        .|.|.filterExpression(isRepeatTrailUnique("r2"))
        .|.|.expandAll("(b)-[r2]-(a)")
        .|.|.argument("b")
        .|.nodeByLabelScan("v", "User")
        .trail(`(u) ((n)-[r]->(m))+ (x)`)
        .|.filterExpression(isRepeatTrailUnique("r"))
        .|.expandAll("(n)-[r]->(m)")
        .|.argument("n")
        .intersectionNodeByLabelsScan("u", Seq("User", "N"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((a)-[]->(b))+`)
        .|.filterExpression(isRepeatTrailUnique("anon_11"))
        .|.expandAll("(a)-[anon_11:T]-(b)")
        .|.argument("a")
        .trail(`(u) ((n)-[]->(m))+`)
        .|.filterExpression(isRepeatTrailUnique("anon_5"))
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
      previouslyBoundRelationshipGroups = Set("r3", "r4"),
      reverseGroupVariableProjections = true
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true
    )

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[r1:R]->(m)-[r2]->(o))+`)
        .|.filterExpressionOrString("not r2 = r1", isRepeatTrailUnique("r1"))
        .|.expandAll("(m)<-[r1:R]-(n)")
        .|.filterExpression(isRepeatTrailUnique("r2"))
        .|.expandAll("(o)<-[r2]-(m)")
        .|.argument("o")
        .trail(`((a)-[r3:T]-(b)<-[r4]-(c))+`)
        .|.filterExpressionOrString("not r4 = r3", isRepeatTrailUnique("r3"))
        .|.expandAll("(b)-[r3:T]-(a)")
        .|.filterExpression(isRepeatTrailUnique("r4"))
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
      previouslyBoundRelationshipGroups = Set("r4"),
      reverseGroupVariableProjections = true
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true
    )

    val plan = planner.plan(query).stripProduceResults
    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[r1:R]->(m)-[r2]->(o))+`)
        .|.filterExpression(isRepeatTrailUnique("r1"))
        .|.expandAll("(m)<-[r1:R]-(n)")
        .|.filterExpression(isRepeatTrailUnique("r2"))
        .|.expandAll("(o)<-[r2:S]-(m)")
        .|.argument("o")
        .trail(`((a)-[r3:T]-(b)<-[r4]-(c))+`)
        .|.filterExpression(isRepeatTrailUnique("r3"))
        .|.expandAll("(b)-[r3:T]-(a)")
        .|.filterExpression(isRepeatTrailUnique("r4"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m))+`)
        .|.filterExpressionOrString("n.prop > m.prop", isRepeatTrailUnique("anon_4"))
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
      start = "anon_0",
      end = "anon_2",
      innerStart = "n",
      innerEnd = "m",
      groupNodes = Set(("n", "n"), ("m", "m")),
      groupRelationships = Set(("anon_4", "anon_8")),
      innerRelationships = Set("anon_4"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    plan should equal(
      planner.subPlanBuilder()
        .apply()
        .|.trail(`((n)-[]->(m))+`)
        .|.|.filterExpression(isRepeatTrailUnique("anon_4"))
        .|.|.expandAll("(n)-[anon_4]->(m)")
        .|.|.filter("n.prop > prop")
        .|.|.argument("n", "prop")
        .|.filter("anon_0.prop > prop")
        .|.allNodeScan("anon_0", "prop")
        .projection(project = Seq("a.prop AS prop"), discard = Set("a"))
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = true
    )

    plan should equal(
      planner.subPlanBuilder()
        .filter("anon_9 = a")
        .trail(`((n)-[r]->(m))+`)
        .|.filterExpressionOrString(
          "cacheNFromStore[n.prop] > cacheN[a.prop]",
          "cacheNFromStore[n.prop] > cacheN[b.prop]",
          isRepeatTrailUnique("r")
        )
        .|.expandAll("(m)<-[r]-(n)")
        .|.argument("m", "a", "b")
        .filter("cacheN[a.prop] > cacheN[a.prop]", "cacheN[a.prop] > cacheN[b.prop]")
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[r]->(m))+`)
        .|.filterExpressionOrString("not r:REL", "r.prop > 0", isRepeatTrailUnique("r"))
        .|.expandAll("(n)-[r]->(m)")
        .|.filter("n:N", "n:NN", "n.prop = 5", "n.foo > 0")
        .|.argument("n")
        .filter("anon_0.prop = 5", "anon_0.foo > 0", "anon_0:NN", "anon_0:N")
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
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    plan should equal(
      planner.subPlanBuilder()
        .filter("anon_8 = m")
        .trail(`(n)((n_inner)-[r_inner]->(m_inner))+ (m)`)
        .|.filterExpression(isRepeatTrailUnique("r_inner"))
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

    plan shouldBe planner.subPlanBuilder()
      .expandAll("(n)-[r*1..]->(m)")
      .allNodeScan("n")
      .build()
  }

  test("should plan quantified relationship as VarExpand when starting on endNode") {
    val query =
      s"""
         |MATCH (n:N)-[r]->+(nn:NN)
         |RETURN *
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .filter("n:N")
      .expandAll("(nn)<-[r*1..]-(n)")
      .nodeByLabelScan("nn", "NN")
      .build()
  }

  test("should plan quantified relationship in other direction") {
    val query =
      s"""
         |MATCH (n)<-[r]-+(m)
         |RETURN n, m
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .expand("(n)<-[r*1..]-(m)", expandMode = ExpandAll, projectedDir = SemanticDirection.INCOMING)
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
      previouslyBoundRelationshipGroups = Set(),
      reverseGroupVariableProjections = false
    )

    plan shouldBe planner.subPlanBuilder()
      .trail(`(n)-[r:R WHERE r.prop > 123]->{2,5}(m)`)
      .|.filterExpressionOrString("r.prop > 123", isRepeatTrailUnique("r"))
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
      previouslyBoundRelationshipGroups = Set(),
      reverseGroupVariableProjections = false
    )

    plan shouldBe planner.subPlanBuilder()
      .trail(params)
      .|.filterExpressionOrString("not r2 = r1", isRepeatTrailUnique("r2"))
      .|.expandAll("(a)-[r2:REL]->(y)")
      .|.filterExpression(isRepeatTrailUnique("r1"))
      .|.expandInto("(x)<-[r1:REL]-(a)")
      .|.nodeIndexOperator("a:A(100 < prop < 123)", argumentIds = Set("x"))
      .allNodeScan("n")
      .build()
  }

  test("should plan an index seek for predicates on QPP start node") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(5000)
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("()-[]->(:A)", 500)
      .setRelationshipCardinality("(:A)-[]->(:A)", 500)
      .addNodeIndex("A", Seq("prop"), 0.5, 0.5)
      .addSemanticFeature(SemanticFeature.QuantifiedPathPatterns)
      .build()

    val q = "MATCH ((a:A WHERE a.prop > 0)<--())+ RETURN *"

    val plan = planner.plan(q).stripProduceResults
    val params = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_0",
      end = "anon_3",
      innerStart = "a",
      innerEnd = "anon_2",
      groupNodes = Set(("a", "a")),
      groupRelationships = Set(("anon_5", "anon_7")),
      innerRelationships = Set("anon_5"),
      previouslyBoundRelationships = Set(),
      previouslyBoundRelationshipGroups = Set(),
      reverseGroupVariableProjections = false
    )

    plan shouldBe planner.subPlanBuilder()
      .trail(params)
      .|.filterExpression(isRepeatTrailUnique("anon_5"))
      .|.expandAll("(a)<-[anon_5]-(anon_2)")
      .|.filter("a:A", "a.prop > 0")
      .|.argument("a")
      .nodeIndexOperator("anon_0:A(prop > 0)")
      .build()
  }

  test("should plan an index seek for predicates on QPP end node") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(5000)
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("(:A)-[]->()", 500)
      .setRelationshipCardinality("(:A)-[]->(:A)", 500)
      .addNodeIndex("A", Seq("prop"), 0.5, 0.5)
      .addSemanticFeature(SemanticFeature.QuantifiedPathPatterns)
      .build()

    val q = "MATCH (()<--(a:A WHERE a.prop > 0))+ RETURN *"

    val plan = planner.plan(q).stripProduceResults
    val params = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "anon_3",
      end = "anon_0",
      innerStart = "a",
      innerEnd = "anon_1",
      groupNodes = Set(("a", "a")),
      groupRelationships = Set(("anon_4", "anon_7")),
      innerRelationships = Set("anon_4"),
      previouslyBoundRelationships = Set(),
      previouslyBoundRelationshipGroups = Set(),
      reverseGroupVariableProjections = true
    )

    plan shouldBe planner.subPlanBuilder()
      .trail(params)
      .|.filterExpression(isRepeatTrailUnique("anon_4"))
      .|.expandAll("(a)-[anon_4]->(anon_1)")
      .|.filter("a:A", "a.prop > 0")
      .|.argument("a")
      .nodeIndexOperator("anon_3:A(prop > 0)")
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
      previouslyBoundRelationshipGroups = Set(),
      reverseGroupVariableProjections = false
    )

    plan shouldBe planner.subPlanBuilder()
      .filter("c:C")
      .trail(trailParameters)
      .|.filterExpression(isRepeatTrailUnique("r"))
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
      .setRelationshipCardinality("(:N)-[:REL]->(:P)", 500)
      .setRelationshipCardinality("(:M)-[:REL]->(:P)", 500)
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
      .setRelationshipCardinality("(:M)-[:REL]->(:Q)", 10)
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
      previouslyBoundRelationshipGroups = Set("r2"),
      reverseGroupVariableProjections = true
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
      previouslyBoundRelationshipGroups = Set(),
      reverseGroupVariableProjections = false
    )

    plan shouldBe planner.subPlanBuilder()
      .filter("a:A", "a:N")
      .trail(n_r1_m_trail)
      .|.filterExpressionOrString("n:N", isRepeatTrailUnique("r1"))
      .|.expandAll("(m)<-[r1:REL]-(n)")
      .|.filter("m:M")
      .|.argument("m")
      .filter("b:B", "b:Q")
      .trail(p_r2_q_trail)
      .|.filterExpressionOrString("q:Q", isRepeatTrailUnique("r2"))
      .|.expandAll("(p)-[r2:REL]->(q)")
      .|.filter("p:P")
      .|.argument("p")
      .intersectionNodeByLabelsScan("x", Seq("X", "M", "P"))
      .build()
  }

  test("should insert eager between quantified relationship and the creation of an overlapping relationship") {
    val query = "MATCH (a)(()-[r]->()){1,5}(b) CREATE (a)-[r2:T]->(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .create(Nil, List(createRelationship("r2", "a", "T", "b", SemanticDirection.OUTGOING)))
      .eager(ListSet(EagernessReason.Unknown))
      .expandAll("(a)-[r*1..5]->(b)")
      .allNodeScan("a")
      .build()
  }

  test("should insert eager between quantified relationship and the creation of an overlapping node") {
    val query = "MATCH (a:N)(()-[r]->())*(b {prop: 42}) MERGE (c:N {prop: 123})"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(List(createNodeWithProperties("c", List("N"), "{prop: 123}")), Nil, Nil, Nil, Set.empty)
      .|.filter("c.prop = 123")
      .|.nodeByLabelScan("c", "N")
      .eager(ListSet(EagernessReason.Unknown))
      .filter("b.prop = 42")
      .expandAll("(a)-[r*0..]->(b)")
      .nodeByLabelScan("a", "N")
      .build()
  }

  test(
    "shouldn't but does insert an unnecessary eager based solely on the predicates contained within the quantified path pattern"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(40)
      .setLabelCardinality("A", 5)
      .setLabelCardinality("B", 50)
      .setRelationshipCardinality("()-[]->(:B)", 10)
      .setRelationshipCardinality("(:A)-[]->(:B)", 10)
      .setRelationshipCardinality("(:B)-[]->(:B)", 10)
      .addSemanticFeature(SemanticFeature.QuantifiedPathPatterns)
      .build()

    // This first MATCH expands to: (:A) | (:A)-->(:B) | (:A)-->(:B)-->(:B) | etc – all nodes have at least one label
    val query = "MATCH (start:A)((a)-[r]->(b:B))*(end) CREATE (x)"
    val plan = planner.plan(query).stripProduceResults

    val `(start)((a)-[r]->(b))*(end)` =
      TrailParameters(
        min = 0,
        max = UpperBound.Unlimited,
        start = "start",
        end = "end",
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set(("a", "a"), ("b", "b")),
        groupRelationships = Set(("r", "r")),
        innerRelationships = Set("r"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false
      )

    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .create(createNode("x"))
      // At the time of writing, predicates do not percolate properly to quantified path patterns.
      // Here we find an overlap between MATCH () and  CREATE () even though we will not match on ().
      .eager(ListSet(EagernessReason.Unknown))
      .filter("end:B")
      .trail(`(start)((a)-[r]->(b))*(end)`)
      .|.filterExpressionOrString("b:B", isRepeatTrailUnique("r"))
      .|.expandAll("(a)-[r]->(b)")
      .|.argument("a")
      .nodeByLabelScan("start", "A")
      .build()
  }

  test(
    "shouldn't insert eager when a quantified path pattern and a created node don't overlap"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(40)
      .setLabelCardinality("A", 5)
      .setLabelCardinality("B", 5)
      .setRelationshipCardinality("(:A)-[]->()", 10)
      .setRelationshipCardinality("()-[]->(:B)", 10)
      .setRelationshipCardinality("(:A)-[]->(:A)", 10)
      .setRelationshipCardinality("(:A)-[]->(:B)", 10)
      .setRelationshipCardinality("(:B)-[]->(:A)", 10)
      .setRelationshipCardinality("(:B)-[]->(:B)", 10)
      .addSemanticFeature(SemanticFeature.QuantifiedPathPatterns)
      .build()

    val query = "MATCH (start:A)((a:A)-[r]->(b:B))*(end:B) CREATE (c:C) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    val `(start)((a)-[r]->(b))*(end)` =
      TrailParameters(
        min = 0,
        max = UpperBound.Unlimited,
        start = "start",
        end = "end",
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set(("a", "a"), ("b", "b")),
        groupRelationships = Set(("r", "r")),
        innerRelationships = Set("r"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false
      )

    plan shouldEqual planner.subPlanBuilder()
      .create(createNode("c", "C"))
      .filter("end:B")
      .trail(`(start)((a)-[r]->(b))*(end)`)
      .|.filterExpressionOrString("b:B", isRepeatTrailUnique("r"))
      .|.expandAll("(a)-[r]->(b)")
      .|.filter("a:A")
      .|.argument("a")
      .nodeByLabelScan("start", "A")
      .build()
  }

  test(
    "should insert eager when a quantified path pattern and a deleted node overlap"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(40)
      .setLabelCardinality("A", 5)
      .setLabelCardinality("B", 5)
      .setLabelCardinality("C", 5)
      .setRelationshipCardinality("(:A)-[]->()", 10)
      .setRelationshipCardinality("()-[]->(:B)", 10)
      .setRelationshipCardinality("(:A)-[]->(:A)", 10)
      .setRelationshipCardinality("(:A)-[]->(:B)", 10)
      .setRelationshipCardinality("(:B)-[]->(:A)", 10)
      .setRelationshipCardinality("(:B)-[]->(:B)", 10)
      .addSemanticFeature(SemanticFeature.QuantifiedPathPatterns)
      .build()

    val query = "MATCH (x:C) OPTIONAL MATCH (start:A)((a:A)-[r]->(b:B))*(end:B) DELETE x"
    val plan = planner.plan(query).stripProduceResults

    val `(start)((a)-[r]->(b))*(end)` =
      TrailParameters(
        min = 0,
        max = UpperBound.Unlimited,
        start = "start",
        end = "end",
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set(("a", "a"), ("b", "b")),
        groupRelationships = Set(("r", "r")),
        innerRelationships = Set("r"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false
      )

    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .deleteNode("x")
      .eager(ListSet(
        EagernessReason.ReadDeleteConflict("start", None),
        EagernessReason.ReadDeleteConflict("end", None),
        EagernessReason.ReadDeleteConflict("a", None),
        EagernessReason.ReadDeleteConflict("b", None)
      ))
      .apply()
      .|.optional("x")
      .|.filter("end:B")
      .|.trail(`(start)((a)-[r]->(b))*(end)`)
      .|.|.filterExpressionOrString("b:B", isRepeatTrailUnique("r"))
      .|.|.expandAll("(a)-[r]->(b)")
      .|.|.filter("a:A")
      .|.|.argument("a")
      .|.nodeByLabelScan("start", "A")
      .nodeByLabelScan("x", "C")
      .build()
  }

  test(
    "Should plan ExpandAll instead of Trail on quantified path pattern with single relationship and no inner node variables - kleene star"
  ) {
    val query = "MATCH (a)(()-[r]->())*(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[r*0..]->(b)")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan ExpandAll instead of Trail on quantified path pattern with relationship and no inner node variables - kleene plus"
  ) {
    val query = "MATCH (a)(()-[r]->())+(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[r*1..]->(b)")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan ExpandAll instead of Trail on quantified path pattern with relationship and no inner node variables - lower bound"
  ) {
    val query = "MATCH (a)(()-[r]->()){2,}(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[r*2..]->(b)")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan ExpandAll instead of Trail on quantified path pattern with relationship and no inner node variables - upper bound"
  ) {
    val query = "MATCH (a)(()-[r]->()){,2}(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[r*0..2]->(b)")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan ExpandAll instead of Trail on quantified path pattern with relationship and no inner node variables - lower and upper bound"
  ) {
    val query = "MATCH (a)(()-[r]->()){2,3}(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[r*2..3]->(b)")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should not plan ExpandAll instead of Trail on quantified path patterns with relationship and a inner node variable - left inner variable"
  ) {
    val query = "MATCH (a)((b)-[r]->()){2,3}(c) RETURN *"
    val plan = planner.plan(query).stripProduceResults
    val `(a)((b)-[r]->()){2,3}(c)` =
      TrailParameters(
        min = 2,
        max = UpperBound.Limited(3),
        start = "a",
        end = "c",
        innerStart = "b",
        innerEnd = "anon_0",
        groupNodes = Set(("b", "b")),
        groupRelationships = Set(("r", "r")),
        innerRelationships = Set("r"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false
      )

    plan shouldEqual planner.subPlanBuilder()
      .trail(`(a)((b)-[r]->()){2,3}(c)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expandAll("(b)-[r]->(anon_0)")
      .|.argument("b")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should not plan ExpandAll instead of Trail on quantified path patterns with relationship and a inner node variable - right inner variable"
  ) {
    val query = "MATCH (a)(()-[r]->(b)){2,3}(c) RETURN *"
    val plan = planner.plan(query).stripProduceResults
    val `(a)(()-[r]->(b)){2,3}(c)` =
      TrailParameters(
        min = 2,
        max = UpperBound.Limited(3),
        start = "a",
        end = "c",
        innerStart = "anon_0",
        innerEnd = "b",
        groupNodes = Set(("b", "b")),
        groupRelationships = Set(("r", "r")),
        innerRelationships = Set("r"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false
      )

    plan shouldEqual planner.subPlanBuilder()
      .trail(`(a)(()-[r]->(b)){2,3}(c)`)
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expandAll("(anon_0)-[r]->(b)")
      .|.argument("anon_0")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should not plan ExpandAll on quantified path pattern with multiple relationships"
  ) {
    val query = "MATCH (a)(()-[r]->()-[]->(b)){2,3}(c) RETURN *"
    val plan = planner.plan(query).stripProduceResults
    val `(a)(()-[r]->(b)){2,3}(c)` =
      TrailParameters(
        min = 2,
        max = UpperBound.Limited(3),
        start = "a",
        end = "c",
        innerStart = "anon_0",
        innerEnd = "b",
        groupNodes = Set(("b", "b")),
        groupRelationships = Set(("r", "r"), ("anon_4", "anon_8")),
        innerRelationships = Set("r", "anon_4"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false
      )

    plan shouldEqual planner.subPlanBuilder()
      .trail(`(a)(()-[r]->(b)){2,3}(c)`)
      .|.filterExpressionOrString("not anon_4 = r", isRepeatTrailUnique("anon_4"))
      .|.expandAll("(anon_1)-[anon_4]->(b)")
      .|.filterExpression(isRepeatTrailUnique("r"))
      .|.expandAll("(anon_0)-[r]->(anon_1)")
      .|.argument("anon_0")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan ExpandAll instead of Trail on quantified path pattern with relationship and no inner node variable - juxtaposed relationship"
  ) {
    val query = "MATCH ()--(a)(()-[r]->()){2,3} RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .filter("not anon_1 IN r")
      .expandAll("(a)-[r*2..3]->(anon_4)")
      .allRelationshipsScan("(anon_0)-[anon_1]-(a)")
      .build()
  }

  test(
    "Should not plan ExpandAll instead of Trail on quantified path patterns with predicate"
  ) {
    val query = "MATCH (a)(()-[r WHERE r.prop > 5]->()){2,3}(c) RETURN *"
    val plan = planner.plan(query).stripProduceResults
    val `(a)(()-[r WHERE r.prop > 5]->()){2,3}(c)` =
      TrailParameters(
        min = 2,
        max = UpperBound.Limited(3),
        start = "a",
        end = "c",
        innerStart = "anon_0",
        innerEnd = "anon_1",
        groupNodes = Set.empty,
        groupRelationships = Set(("r", "r")),
        innerRelationships = Set("r"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false
      )

    plan shouldEqual planner.subPlanBuilder()
      .trail(`(a)(()-[r WHERE r.prop > 5]->()){2,3}(c)`)
      .|.filterExpressionOrString("r.prop > 5", isRepeatTrailUnique("r"))
      .|.expandAll("(anon_0)-[r]->(anon_1)")
      .|.argument("anon_0")
      .allNodeScan("a")
      .build()
  }

  test(
    "Should plan ExpandAll instead of Trail on Quantified path patterns with Relationship and no inner node variables - Relationship form Kleene plus"
  ) {
    val query = "MATCH (a)-[r]->+(b) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[r*1..]->(b)")
      .allNodeScan("a")
      .build()
  }

  test("Should plan VarExpand for multiple qpp's") {
    val query = "MATCH (a) (()-[r1]->())+ (b) (()<-[r2]-())+ (c) RETURN *"
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .filter("none(anon_12 IN r1 WHERE anon_12 IN r2)")
      .expand(
        "(b)<-[r1*1..]-(a)",
        expandMode = ExpandAll,
        projectedDir = OUTGOING,
        nodePredicates = Seq(),
        relationshipPredicates = Seq()
      )
      .expand(
        "(b)<-[r2*1..]-(c)",
        expandMode = ExpandAll,
        projectedDir = INCOMING,
        nodePredicates = Seq(),
        relationshipPredicates = Seq()
      )
      .allNodeScan("b")
      .build()
  }
}
