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

  test("Should plan quantifier * with start node") {
    val query =
      s"""
         |MATCH (u:User)((n)-[]->(m))* RETURN n, m
         |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    val `(u)((n)-[]-(m))*` = TrailParameters(
      0,
      Unlimited,
      "u",
      Some("anon_1"),
      "anon_2",
      "anon_4",
      Set(("anon_2", "n"), ("anon_4", "m")),
      Set(("anon_3", "anon_0")),
      Set("anon_3"),
      Set()
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`(u)((n)-[]-(m))*`)
        .|.expand("(anon_2)-[anon_3]->(anon_4)")
        .|.argument("anon_2")
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should plan quantifier * with end node") {
    val query =
      s"""
         |MATCH ((n)-[]->(m))*(u:User) RETURN n, m
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))*(u)` = TrailParameters(
      0,
      Unlimited,
      "u",
      Some("anon_0"),
      "anon_4",
      "anon_2",
      Set(("anon_2", "n"), ("anon_4", "m")),
      Set(("anon_3", "anon_1")),
      Set("anon_3"),
      Set()
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m))*(u)`)
        .|.expand("(anon_4)<-[anon_3]-(anon_2)")
        .|.argument("anon_4")
        .nodeByLabelScan("u", "User")
        .build()
    )
  }

  test("Should plan quantifier +") {
    val query =
      s"""
         |MATCH ((n)-[]->(m))+ RETURN n, m
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))+` = TrailParameters(
      1,
      Unlimited,
      "anon_0",
      Some("anon_2"),
      "anon_3",
      "anon_5",
      Set(("anon_3", "n"), ("anon_5", "m")),
      Set(("anon_4", "anon_1")),
      Set("anon_4"),
      Set()
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m))+`)
        .|.expand("(anon_3)-[anon_4]->(anon_5)")
        .|.argument("anon_3")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test("Should plan quantifier {1,}") {
    val query =
      s"""
         |MATCH ((n)-[]->(m)){1,} RETURN n, m
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults

    val `((n)-[]->(m)){1,}` = TrailParameters(
      1,
      Unlimited,
      "anon_0",
      Some("anon_2"),
      "anon_3",
      "anon_5",
      Set(("anon_3", "n"), ("anon_5", "m")),
      Set(("anon_4", "anon_1")),
      Set("anon_4"),
      Set()
    )
    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m)){1,}`)
        .|.expand("(anon_3)-[anon_4]->(anon_5)")
        .|.argument("anon_3")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test("Should plan quantifier {,5} with start node") {
    val query =
      s"""
         |MATCH ()((n)-[]->(m)){,5} RETURN n, m
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val `()((n)-[]->(m)){,5}` = TrailParameters(
      0,
      UpperBound.Limited(5),
      "anon_0",
      Some("anon_2"),
      "anon_3",
      "anon_5",
      Set(("anon_3", "n"), ("anon_5", "m")),
      Set(("anon_4", "anon_1")),
      Set("anon_4"),
      Set()
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`()((n)-[]->(m)){,5}`)
        .|.expand("(anon_3)-[anon_4]->(anon_5)")
        .|.argument("anon_3")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test("Should plan quantifier {1,5} with end node") {
    val query =
      s"""
         |MATCH ((n)-[]->(m)){1,5} RETURN n, m
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m)){1,5}` = TrailParameters(
      1,
      UpperBound.Limited(5),
      "anon_0",
      Some("anon_2"),
      "anon_3",
      "anon_5",
      Set(("anon_3", "n"), ("anon_5", "m")),
      Set(("anon_4", "anon_1")),
      Set("anon_4"),
      Set()
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m)){1,5}`)
        .|.expand("(anon_3)-[anon_4]->(anon_5)")
        .|.argument("anon_3")
        .allNodeScan("anon_0")
        .build()
    )
  }

  test("Should plan mix of quantified and non-quantified path patterns") {
    val query =
      s"""
         |MATCH ((n)-[]->(m))+ (a)--(b) RETURN n, m
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    val `((n)-[]->(m))+(a)` = TrailParameters(
      1,
      Unlimited,
      "a",
      Some("anon_0"),
      "anon_5",
      "anon_3",
      Set(("anon_3", "n"), ("anon_5", "m")),
      Set(("anon_4", "anon_1")),
      Set("anon_4"),
      Set()
    )

    plan should equal(
      planner.subPlanBuilder()
        .trail(`((n)-[]->(m))+(a)`)
        .|.expandAll("(anon_5)<-[anon_4]-(anon_3)")
        .|.argument("anon_5")
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
      1,
      Unlimited,
      "n",
      Some("anon_5"),
      "anon_0",
      "anon_2",
      Set(("anon_0", "n_inner"), ("anon_2", "m_inner")),
      Set(("anon_1", "r_inner")),
      Set("anon_1"),
      Set()
    )

    plan should equal(
      planner.subPlanBuilder()
        .filter("anon_5 = m")
        .trail(`(n)((n_inner)-[r_inner]->(m_inner))+ (m)`)
        .|.expandAll("(anon_0)-[anon_1]->(anon_2)")
        .|.argument("anon_0")
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
