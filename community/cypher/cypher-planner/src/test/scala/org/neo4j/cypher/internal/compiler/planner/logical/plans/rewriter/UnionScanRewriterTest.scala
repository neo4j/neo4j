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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UnionScanRewriterTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should rewrite simple ascending plan") {
    val input = new LogicalPlanBuilder()
      .produceResults("n")
      .orderedDistinct(Seq("n"), "n AS n")
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "B", IndexOrderAscending)
      .nodeByLabelScan("n", "A", IndexOrderAscending)
      .build()

    rewrite(input) should equal(new LogicalPlanBuilder()
      .produceResults("n")
      .unionNodeByLabelsScan("n", Seq("A", "B"), IndexOrderAscending)
      .build())
  }

  test("should rewrite simple descending plan") {
    val input = new LogicalPlanBuilder()
      .produceResults("n")
      .orderedDistinct(Seq("n"), "n AS n")
      .orderedUnion(Seq(Descending("n")))
      .|.nodeByLabelScan("n", "B", IndexOrderDescending)
      .nodeByLabelScan("n", "A", IndexOrderDescending)
      .build()

    rewrite(input) should equal(new LogicalPlanBuilder()
      .produceResults("n")
      .unionNodeByLabelsScan("n", Seq("A", "B"), IndexOrderDescending)
      .build())
  }

  test("should rewrite bigger plan") {
    val input = new LogicalPlanBuilder()
      .produceResults("n")
      .orderedDistinct(Seq("n"), "n AS n")
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "L", IndexOrderAscending)
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "K", IndexOrderAscending)
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "J", IndexOrderAscending)
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "I", IndexOrderAscending)
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "H", IndexOrderAscending)
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "G", IndexOrderAscending)
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "F", IndexOrderAscending)
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "E", IndexOrderAscending)
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "D", IndexOrderAscending)
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "C", IndexOrderAscending)
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "B", IndexOrderAscending)
      .nodeByLabelScan("n", "A", IndexOrderAscending)
      .build()

    rewrite(input) should equal(new LogicalPlanBuilder()
      .produceResults("n")
      .unionNodeByLabelsScan("n", Seq("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L"), IndexOrderAscending)
      .build())
  }

  test("should not rewrite if different index order") {
    val input = new LogicalPlanBuilder()
      .produceResults("n")
      .orderedDistinct(Seq("n"), "n AS n")
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "C", IndexOrderAscending)
      .orderedUnion(Seq(Descending("n")))
      .|.nodeByLabelScan("n", "B", IndexOrderDescending)
      .nodeByLabelScan("n", "A", IndexOrderAscending)
      .build()

    rewrite(input) should equal(input)
  }

  test("should not rewrite if different names") {
    val input = new LogicalPlanBuilder()
      .produceResults("n")
      .orderedDistinct(Seq("n"), "n AS n")
      .orderedUnion(Seq(Ascending("n")))
      .|.nodeByLabelScan("n", "C", IndexOrderAscending)
      .orderedUnion(Seq(Ascending("m")))
      .|.nodeByLabelScan("m", "B", IndexOrderAscending)
      .nodeByLabelScan("n", "A", IndexOrderAscending)
      .build()

    rewrite(input) should equal(input)
  }

  test("should preserve arguments") {
    val input = new LogicalPlanBuilder()
      .produceResults("m")
      .apply()
      .|.orderedDistinct(Seq("n"), "n AS n")
      .|.orderedUnion(Seq(Ascending("n")))
      .|.|.nodeByLabelScan("n", "C", IndexOrderAscending, "m")
      .|.orderedUnion(Seq(Ascending("n")))
      .|.|.nodeByLabelScan("n", "B", IndexOrderAscending, "m")
      .|.nodeByLabelScan("n", "A", IndexOrderAscending, "m")
      .allNodeScan("m")
      .build()

    rewrite(input) should equal(new LogicalPlanBuilder()
      .produceResults("m")
      .apply()
      .|.unionNodeByLabelsScan("n", Seq("A", "B", "C"), IndexOrderAscending, "m")
      .allNodeScan("m")
      .build())
  }

  private def rewrite(p: LogicalPlan): LogicalPlan = p.endoRewrite(unionScanRewriter)
}
