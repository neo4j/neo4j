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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeFull
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

class RemoteIndexSeekRewriterTest extends CypherPlannerTestSuite {

  private def rewrite(plan: LogicalPlan): LogicalPlan =
    plan.endoRewrite(RemoteIndexSeekRewriter)

  test("should rewrite NodeIndexSeek on RHS of Apply to RemoteNodeIndexSeek") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.expand("(m)-[:KNOWS]->(n)")
      .|.nodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val result = rewrite(input)

    val expected = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.expand("(m)-[:KNOWS]->(n)")
      .|.remoteNodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    result shouldEqual expected
  }

  test("should not rewrite NodeIndexSeek on RHS of a hash join where the argumentIds are empty") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .nodeHashJoin("m")
      .|.nodeIndexOperator("n:Person(id = 42)")
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    rewrite(input) shouldEqual input
  }

  test("should not rewrite a top-level NodeIndexSeek") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .nodeIndexOperator("n:Person(id = 42)")
      .build()

    rewrite(input) shouldEqual input
  }

  test("should not rewrite NodeIndexSeek on the LHS of an apply plan") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.allNodeScan("m")
      .nodeIndexOperator("n:Person(id = 42)")
      .build()

    rewrite(input) shouldEqual input
  }

  test("should rewrite NodeUniqueIndexSeek on RHS of Apply to RemoteNodeUniqueIndexSeek") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.expand("(m)-[:KNOWS]->(n)")
      .|.nodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"), unique = true)
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val expected = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.expand("(m)-[:KNOWS]->(n)")
      .|.remoteNodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"), unique = true)
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    rewrite(input) shouldEqual expected
  }

  test("should not rewrite a top-level NodeUniqueIndexSeek") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .nodeIndexOperator("n:Person(id = 42)", unique = true)
      .build()

    rewrite(input) shouldEqual input
  }

  test("should not rewrite NodeUniqueIndexSeek on the LHS of an apply plan") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.allNodeScan("m")
      .nodeIndexOperator("n:Person(id = 42)", unique = true)
      .build()

    rewrite(input) shouldEqual input
  }

  test("should not rewrite NodeIndexSeek on RHS of a Merge-Apply") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
      .|.nodeIndexOperator("n:Person(id = m.id)", argumentIds = Set("m"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val result = rewrite(input)

    result shouldEqual input
  }

  test("should not rewrite NodeIndexSeek on LHS of a Merge-Apply") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
      .|.argument("n")
      .nodeIndexOperator("n:Person(id = 42)")
      .build()

    val result = rewrite(input)

    result shouldEqual input
  }

  test("should not rewrite NodeIndexSeek AFTER a Merge-Apply") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .apply()
      .|.nodeIndexOperator("m:Person(id = n.id)", argumentIds = Set("n"))
      .apply()
      .|.merge(Seq(createNodeFull("p", labels = Seq("Person"), properties = Some("{name: 'Andy'}"))))
      .|.argument("n")
      .nodeIndexOperator("n:Person(id = 42)")
      .build()

    val result = rewrite(input)

    result shouldEqual input
  }

  test("should not rewrite NodeIndexSeek in the RHS of node hash join if there is a write on the other side") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .nodeHashJoin("m")
      .|.nodeIndexOperator("n:Person(id = 42)")
      .create(createNode("p", "Person"))
      .nodeByLabelScan("m", "Person", IndexOrderNone)
      .build()

    val result = rewrite(input)

    result shouldEqual input
  }

  test("should not rewrite NodeIndexSeek in the LHS of node hash join if there is a write on the other side") {
    val input = new LogicalPlanBuilder(wholePlan = false)
      .nodeHashJoin("m")
      .|.create(createNode("p", "Person"))
      .|.nodeByLabelScan("m", "Person", IndexOrderNone)
      .nodeIndexOperator("n:Person(id = 42)")
      .build()

    val result = rewrite(input)

    result shouldEqual input
  }
}
