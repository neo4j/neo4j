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
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

class MergeRewriterTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport {

  test("should rewrite merge + expandInto") {
    val before = new LogicalPlanBuilder()
      .produceResults("r")
      .apply()
      .|.merge(Seq.empty, Seq(createRelationship("r", "x", "R", "y")), lockNodes = Set("x", "y"))
      .|.expandInto("(x)-[r:R]->(y)")
      .|.argument("x", "y")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("r")
      .apply()
      .|.mergeInto("(x)-[r:R]->(y)")
      .|.argument("x", "y")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    rewrite(before) should equal(after)
  }

  test("should rewrite merge + expandInto with ON MATCH and ON CREATE") {
    val before = new LogicalPlanBuilder()
      .produceResults("r")
      .apply()
      .|.merge(
        Seq.empty,
        Seq(createRelationship("r", "x", "R", "y")),
        onMatch = Seq(setRelationshipProperty("r", "prop", "true")),
        onCreate = Seq(setRelationshipProperty("r", "prop", "false")),
        lockNodes = Set("x", "y")
      )
      .|.expandInto("(x)-[r:R]->(y)")
      .|.argument("x", "y")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("r")
      .apply()
      .|.mergeInto("(x)-[r:R]->(y)", onMatch = Seq(("prop", "true")), onCreate = Seq(("prop", "false")))
      .|.argument("x", "y")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    rewrite(before) should equal(after)
  }

  test("should rewrite merge + expandInto with (random) ON MATCH and ON CREATE") {
    val before = new LogicalPlanBuilder()
      .produceResults("r")
      .apply()
      .|.merge(
        Seq.empty,
        Seq(createRelationship("r", "x", "R", "y")),
        onMatch = Seq(setRelationshipProperty("r", "prop", "rand()")),
        onCreate = Seq(setRelationshipProperty("r", "prop", "rand()")),
        lockNodes = Set("x", "y")
      )
      .|.expandInto("(x)-[r:R]->(y)")
      .|.argument("x", "y")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("r")
      .apply()
      .|.mergeInto("(x)-[r:R]->(y)", onMatch = Seq(("prop", "rand()")), onCreate = Seq(("prop", "rand()")))
      .|.argument("x", "y")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    rewrite(before) should equal(after)
  }

  test("should not rewrite if only one node is bound") {
    val before = new LogicalPlanBuilder()
      .produceResults("r")
      .apply()
      .|.merge(Seq(createNode("y")), Seq(createRelationship("r", "x", "R", "y")), lockNodes = Set("x"))
      .|.expand("(x)-[r:R]->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    assertNotRewritten(before)
  }

  test("should not rewrite if ON MATCH reference something that is not related to relationships") {
    val before = new LogicalPlanBuilder()
      .produceResults("r")
      .apply()
      .|.merge(
        Seq.empty,
        Seq(createRelationship("r", "x", "R", "y")),
        onMatch = Seq(setLabel("y", "FOO")),
        lockNodes = Set("x", "y")
      )
      .|.expandInto("(x)-[r:R]->(y)")
      .|.argument("x", "y")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    assertNotRewritten(before)
  }

  test("should not rewrite if ON CREATE reference something that is not related to relationships") {
    val before = new LogicalPlanBuilder()
      .produceResults("r")
      .apply()
      .|.merge(
        Seq.empty,
        Seq(createRelationship("r", "x", "R", "y")),
        onMatch = Seq(setNodeProperty("x", "prop", "true")),
        lockNodes = Set("x", "y")
      )
      .|.expandInto("(x)-[r:R]->(y)")
      .|.argument("x", "y")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    assertNotRewritten(before)
  }

  test("should not rewrite merge + expandInto if we don't support fastExpandInto") {
    val before = new LogicalPlanBuilder()
      .produceResults("r")
      .apply()
      .|.merge(Seq.empty, Seq(createRelationship("r", "x", "R", "y")), lockNodes = Set("x", "y"))
      .|.expandInto("(x)-[r:R]->(y)")
      .|.argument("x", "y")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    assertNotRewritten(before, supportsFastExpandInto = false)
  }

  test("should rewrite merge + unique node index seek") {
    val before = new LogicalPlanBuilder()
      .produceResults("x")
      .merge(Seq(createNodeWithProperties("x", Seq("X"), "{prop: 42}")))
      .nodeIndexOperator("x:X(prop=42)", unique = true)
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("x")
      .mergeUniqueNode("x", "X", Seq("prop" -> "42"))
      .build()

    rewrite(before) should equal(after)
  }

  test("should rewrite merge + unique node index seek with a random onMatch and onCreate") {
    val before = new LogicalPlanBuilder()
      .produceResults("x")
      .merge(
        Seq(createNodeWithProperties("x", Seq("X"), "{prop: 42}")),
        onMatch = Seq(setNodeProperty("x", "onMatch", "rand()")),
        onCreate = Seq(setNodeProperty("x", "onCreate", "rand()"))
      )
      .nodeIndexOperator("x:X(prop=42)", unique = true)
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("x")
      .mergeUniqueNode(
        "x",
        "X",
        Seq("prop" -> "42"),
        onMatch = Seq(("onMatch", "rand()")),
        onCreate = Seq(("onCreate", "rand()"))
      )
      .build()

    rewrite(before) should equal(after)
  }

  test("should not rewrite merge + unique node index when onMatch depends on merge variable") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .merge(
        Seq(createNodeWithProperties("x", Seq("X"), "{prop: 42}")),
        onMatch = Seq(setNodeProperty("x", "onMatch", "x.onCreate + 1")),
        onCreate = Seq(setNodeProperty("x", "onCreate", "rand()"))
      )
      .nodeIndexOperator("x:X(prop=42)", unique = true)
      .build()

    assertNotRewritten(plan)
  }

  private def assertNotRewritten(p: LogicalPlan, supportsFastExpandInto: Boolean = true): Unit = {
    rewrite(p, supportsFastExpandInto) should equal(p)
  }

  private def rewrite(p: LogicalPlan, supportsFastExpandInto: Boolean = true): LogicalPlan =
    p.endoRewrite(mergeRewriter(supportsFastExpandInto))
}
