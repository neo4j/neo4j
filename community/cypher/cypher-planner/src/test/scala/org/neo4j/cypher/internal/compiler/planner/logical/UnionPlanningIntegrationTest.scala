/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class UnionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("one UNION all") {

    val (_, logicalPlan, _, _, _) = new given {
      knownLabels = Set("A", "B")
    }.getLogicalPlanFor(
      """
        |MATCH (a:A) RETURN a AS a, 1 AS b
        |UNION ALL
        |MATCH (a:B) RETURN 1 AS b, a AS a""".stripMargin, stripProduceResults = false)

    val Seq(a1, a2, a3) = namespaced("a", 8, 35, 52)
    val Seq(b1, b2, b3) = namespaced("b", 33, 35, 69)

    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults(a2, b2)
        .union()
        .|.projection(s"$a3 AS $a2", s"$b3 AS $b2")
        .|.projection(s"1 AS $b3")
        .|.nodeByLabelScan(a3, "B")
        .projection(s"$a1 AS $a2", s"$b1 AS $b2")
        .projection(s"1 AS $b1")
        .nodeByLabelScan(a1, "A")
        .build()
    )
  }

  test("one UNION distinct") {

    val (_, logicalPlan, _, _, _) = new given {
      knownLabels = Set("A", "B")
    }.getLogicalPlanFor(
      """
        |MATCH (a:A) RETURN a AS a, 1 AS b
        |UNION
        |MATCH (a:B) RETURN 1 AS b, a AS a""".stripMargin, stripProduceResults = false)

    val Seq(a1, a2, a3) = namespaced("a", 8, 35, 48)
    val Seq(b1, b2, b3) = namespaced("b", 33, 35, 65)

    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults(a2, b2)
        .distinct(s"$a2 AS $a2", s"$b2 AS $b2")
        .union()
        .|.projection(s"$a3 AS $a2", s"$b3 AS $b2")
        .|.projection(s"1 AS $b3")
        .|.nodeByLabelScan(a3, "B")
        .projection(s"$a1 AS $a2", s"$b1 AS $b2")
        .projection(s"1 AS $b1")
        .nodeByLabelScan(a1, "A")
        .build()
    )
  }
  test("two UNION all") {

    val (_, logicalPlan, _, _, _) = new given {
      knownLabels = Set("A", "B", "C")
    }.getLogicalPlanFor(
      """MATCH (a:A) RETURN a AS a, 1 AS b
        |UNION ALL
        |MATCH (a:B) RETURN 1 AS b, a AS a
        |UNION ALL
        |MATCH (a:C) RETURN a AS a, 1 AS b
        |""".stripMargin, stripProduceResults = false)

    val Seq(a1, a2, a3, a4, a5) = namespaced("a", 7, 34, 51, 78, 95)
    val Seq(b1, b2, b3, b4, b5) = namespaced("b", 32, 34, 68, 78, 120)

    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults(a4, b4)
        .union()
        .|.projection(s"$a5 AS $a4", s"$b5 AS $b4")
        .|.projection(s"1 AS $b5")
        .|.nodeByLabelScan(a5, "C")
        .projection(s"$a2 AS $a4", s"$b2 AS $b4")
        .union()
        .|.projection(s"$a3 AS $a2", s"$b3 AS $b2")
        .|.projection(s"1 AS $b3")
        .|.nodeByLabelScan(a3, "B")
        .projection(s"$a1 AS $a2", s"$b1 AS $b2")
        .projection(s"1 AS $b1")
        .nodeByLabelScan(a1, "A")
        .build()
    )
  }

  test("two UNION distinct") {

    val (_, logicalPlan, _, _, _) = new given {
      knownLabels = Set("A", "B", "C")
    }.getLogicalPlanFor(
      """MATCH (a:A) RETURN a AS a, 1 AS b
        |UNION
        |MATCH (a:B) RETURN 1 AS b, a AS a
        |UNION
        |MATCH (a:C) RETURN a AS a, 1 AS b
        |""".stripMargin, stripProduceResults = false)

    val Seq(a1, a2, a3, a4, a5) = namespaced("a", 7, 34, 47, 74, 87)
    val Seq(b1, b2, b3, b4, b5) = namespaced("b", 32, 34, 64, 74, 112)

    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults(a4, b4)
        .distinct(s"$a4 AS $a4", s"$b4 AS $b4")
        .union()
        .|.projection(s"$a5 AS $a4", s"$b5 AS $b4")
        .|.projection(s"1 AS $b5")
        .|.nodeByLabelScan(a5, "C")
        .projection(s"$a2 AS $a4", s"$b2 AS $b4")
        .union()
        .|.projection(s"$a3 AS $a2", s"$b3 AS $b2")
        .|.projection(s"1 AS $b3")
        .|.nodeByLabelScan(a3, "B")
        .projection(s"$a1 AS $a2", s"$b1 AS $b2")
        .projection(s"1 AS $b1")
        .nodeByLabelScan(a1, "A")
        .build()
    )
  }

  test("one UNION distinct without columns") {

    val (_, logicalPlan, _, _, _) = new given {
    }.getLogicalPlanFor(
      """
        |CREATE (a)
        |UNION
        |CREATE (b)
        |""".stripMargin, stripProduceResults = false)

    logicalPlan should equal(
      new LogicalPlanBuilder()
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
    )
  }
}
