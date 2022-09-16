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

  private val planner = plannerBuilder()
    .setAllNodesCardinality(10)
    .setAllRelationshipsCardinality(10)
    .setRelationshipCardinality("()-[:R]->()", 10)
    .setLabelCardinality("User", 5)
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
      groupRelationships = Set(),
      innerRelationships = Set("anon_0"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`(u)((n)-[]-(m))*`)
        .|.expand("(n)-[anon_0]->(m)")
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
      groupRelationships = Set(),
      innerRelationships = Set("anon_1"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m))*(u)`)
        .|.expand("(m)<-[anon_1]-(n)")
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
      groupRelationships = Set(),
      innerRelationships = Set("anon_1"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m))+`)
        .|.expand("(n)-[anon_1]->(m)")
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
      groupRelationships = Set(),
      innerRelationships = Set("anon_1"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )
    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m)){1,}`)
        .|.expand("(n)-[anon_1]->(m)")
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
      groupRelationships = Set(),
      innerRelationships = Set("anon_1"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`()((n)-[]->(m)){,5}`)
        .|.expand("(n)-[anon_1]->(m)")
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
      groupRelationships = Set(),
      innerRelationships = Set("anon_1"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m)){1,5}`)
        .|.expand("(n)-[anon_1]->(m)")
        .|.argument("n")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test("Should plan mix of quantified and non-quantified path patterns") {
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
      groupRelationships = Set(),
      innerRelationships = Set("anon_1"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m))+(a)`)
        .|.expandAll("(m)<-[anon_1]-(n)")
        .|.argument("m")
        .allRelationshipsScan("(a)-[anon_2]-(b)")
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
        .|.argument("n_inner")
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
}
