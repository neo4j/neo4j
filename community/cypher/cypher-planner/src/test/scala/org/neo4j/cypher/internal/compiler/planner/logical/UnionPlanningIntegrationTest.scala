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

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UnionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("one UNION all") {

    val (_, logicalPlan, _, _) = new given {
      knownLabels = Set("A", "B")
    }.getLogicalPlanFor(normalizeNewLines(
      """
        |MATCH (a:A) RETURN a AS a, 1 AS b
        |UNION ALL
        |MATCH (a:B) RETURN 1 AS b, a AS a""".stripMargin), stripProduceResults = false)

    logicalPlan should equal(
      new LogicalPlanBuilder()
        .produceResults("a", "b")
        .union()
        .|.projection("a AS a", "b AS b")
        .|.projection("1 AS b")
        .|.nodeByLabelScan("a", "B", IndexOrderNone)
        .projection("a AS a", "b AS b")
        .projection("1 AS b")
        .nodeByLabelScan("a", "A", IndexOrderNone)
        .build()
    )
  }

  test("one UNION distinct") {

    val (_, logicalPlan, _, _) = new given {
      knownLabels = Set("A", "B")
    }.getLogicalPlanFor(normalizeNewLines(
      """
        |MATCH (a:A) RETURN a AS a, 1 AS b
        |UNION
        |MATCH (a:B) RETURN 1 AS b, a AS a""".stripMargin), stripProduceResults = false)


    logicalPlan should equal(
      new LogicalPlanBuilder()
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
    )
  }
  test("two UNION all") {

    val (_, logicalPlan, _, _) = new given {
      knownLabels = Set("A", "B", "C")
    }.getLogicalPlanFor(normalizeNewLines(
      """MATCH (a:A) RETURN a AS a, 1 AS b
        |UNION ALL
        |MATCH (a:B) RETURN 1 AS b, a AS a
        |UNION ALL
        |MATCH (a:C) RETURN a AS a, 1 AS b
        |""".stripMargin), stripProduceResults = false)


    logicalPlan should equal(
      new LogicalPlanBuilder()
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
    )
  }

  test("two UNION distinct") {

    val (_, logicalPlan, _, _) = new given {
      knownLabels = Set("A", "B", "C")
    }.getLogicalPlanFor(normalizeNewLines(
      """MATCH (a:A) RETURN a AS a, 1 AS b
        |UNION
        |MATCH (a:B) RETURN 1 AS b, a AS a
        |UNION
        |MATCH (a:C) RETURN a AS a, 1 AS b
        |""".stripMargin), stripProduceResults = false)

    logicalPlan should equal(
      new LogicalPlanBuilder()
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
    )
  }

  test("one UNION distinct without columns") {

    val (_, logicalPlan, _, _) = new given {
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
