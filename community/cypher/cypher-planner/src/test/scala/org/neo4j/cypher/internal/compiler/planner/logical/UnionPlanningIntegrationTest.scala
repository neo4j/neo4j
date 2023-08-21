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

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UnionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {

  test("one UNION all") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 100)
      .setLabelCardinality("B", 200)
      .build()

    val query =
      """
        |MATCH (a:A) RETURN a AS a, 1 AS b
        |UNION ALL
        |MATCH (a:B) RETURN 1 AS b, a AS a
        |""".stripMargin

    cfg.plan(query) shouldEqual cfg.planBuilder()
      .produceResults("a", "b")
      .union()
      .|.projection("a AS a", "b AS b")
      .|.projection("1 AS b")
      .|.nodeByLabelScan("a", "B", IndexOrderNone)
      .projection("a AS a", "b AS b")
      .projection("1 AS b")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()
  }

  test("one UNION distinct") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 100)
      .setLabelCardinality("B", 200)
      .build()

    val query =
      """
        |MATCH (a:A) RETURN a AS a, 1 AS b
        |UNION
        |MATCH (a:B) RETURN 1 AS b, a AS a"""
        .stripMargin

    cfg.plan(query) shouldEqual cfg.planBuilder()
      .produceResults("a", "b")
      .distinct("a AS a", "b AS b")
      .union()
      .|.projection("a AS a", "b AS b")
      .|.projection("1 AS b")
      .|.nodeByLabelScan("a", "B", IndexOrderNone)
      .projection("a AS a", "b AS b")
      .projection("1 AS b")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()
  }

  test("two UNION all") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 100)
      .setLabelCardinality("B", 200)
      .setLabelCardinality("C", 300)
      .build()

    val query =
      """MATCH (a:A) RETURN a AS a, 1 AS b
        |UNION ALL
        |MATCH (a:B) RETURN 1 AS b, a AS a
        |UNION ALL
        |MATCH (a:C) RETURN a AS a, 1 AS b
        |""".stripMargin

    cfg.plan(query) shouldEqual cfg.planBuilder()
      .produceResults("a", "b")
      .union()
      .|.projection("a AS a", "b AS b")
      .|.projection("1 AS b")
      .|.nodeByLabelScan("a", "C", IndexOrderNone)
      .projection("a AS a", "b AS b")
      .union()
      .|.projection("a AS a", "b AS b")
      .|.projection("1 AS b")
      .|.nodeByLabelScan("a", "B", IndexOrderNone)
      .projection("a AS a", "b AS b")
      .projection("1 AS b")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()
  }

  test("two UNION distinct") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 100)
      .setLabelCardinality("B", 200)
      .setLabelCardinality("C", 300)
      .build()

    val query =
      """MATCH (a:A) RETURN a AS a, 1 AS b
        |UNION
        |MATCH (a:B) RETURN 1 AS b, a AS a
        |UNION
        |MATCH (a:C) RETURN a AS a, 1 AS b
        |""".stripMargin

    cfg.plan(query) shouldEqual cfg.planBuilder()
      .produceResults("a", "b")
      .distinct("a AS a", "b AS b")
      .union()
      .|.projection("a AS a", "b AS b")
      .|.projection("1 AS b")
      .|.nodeByLabelScan("a", "C", IndexOrderNone)
      .projection("a AS a", "b AS b")
      .union()
      .|.projection("a AS a", "b AS b")
      .|.projection("1 AS b")
      .|.nodeByLabelScan("a", "B", IndexOrderNone)
      .projection("a AS a", "b AS b")
      .projection("1 AS b")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()
  }

  test("one UNION distinct without columns") {
    val cfg = plannerBuilder().setAllNodesCardinality(1000).build()

    val query =
      """
        |CREATE (a)
        |UNION
        |CREATE (b)
        |""".stripMargin

    cfg.plan(query) shouldEqual cfg.planBuilder()
      .produceResults()
      .union()
      .|.projection()
      .|.emptyResult()
      .|.create(createNode("b"))
      .|.argument()
      .projection()
      .emptyResult()
      .create(createNode("a"))
      .argument()
      .build()
  }
}
