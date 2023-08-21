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

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class BfsAggregationRemoverTest extends CypherFunSuite with LogicalPlanningTestSupport {

  /**
   * Distinct positive cases
   */

  test("should remove distinct on to node") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove distinct on aliased to node") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("tutu AS tutu")
      .projection("to AS tutu")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("tutu AS tutu")
      .projection("to AS tutu")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove distinct on from and to nodes") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("from AS from", "to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("from AS from", "to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove distinct on aliased from and to nodes") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("from AS morf", "tutu AS tutu")
      .projection("from AS morf", "to AS tutu")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("from AS morf", "tutu AS tutu")
      .projection("from AS morf", "to AS tutu")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove distinct on from and to nodes when they are referenced by more operators") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("from AS from", "to AS to")
      .projection("to AS tutu")
      .filter("from <> to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("from AS from", "to AS to")
      .projection("to AS tutu")
      .filter("from <> to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should only remove first distinct when there are multiple alternating bfs & distinct plans") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .bfsPruningVarExpand("(middle)-[*1..3]-(to)")
      .distinct("middle AS middle")
      .bfsPruningVarExpand("(from)-[*1..3]-(middle)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .bfsPruningVarExpand("(middle)-[*1..3]-(to)")
      .projection("middle AS middle")
      .bfsPruningVarExpand("(from)-[*1..3]-(middle)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  /**
   * Distinct negative cases
   */

  test("should not remove distinct on from node") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("from AS from")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove distinct on to node when not unique leaf") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test("should remove distinct on from and to nodes when not unique leaf") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("from AS from", "to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove distinct on to node when there are other distinct columns") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to", "1 AS one")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove distinct on from and to nodes when there are other distinct columns") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("from AS from", "to AS to", "1 AS one")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove distinct on to node when other cardinality increasing plan before bfs") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .unwind("[1] AS one")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove distinct on to node when other cardinality increasing plan after bfs") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .unwind("[1] AS one")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove distinct on to node when there are multiple bfs plans") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .bfsPruningVarExpand("(middle)-[*1..3]-(to)")
      .bfsPruningVarExpand("(from)-[*1..3]-(middle)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  /**
   * OrderedDistinct positive cases
   */

  test("should remove ordered distinct on to node") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("to"), "to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove ordered distinct on from and to nodes") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("to"), "from AS from", "to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("from AS from", "to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove ordered distinct on from and to nodes when they are referenced by more operators") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("to"), "from AS from", "to AS to")
      .projection("to AS tutu")
      .filter("from <> to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("from AS from", "to AS to")
      .projection("to AS tutu")
      .filter("from <> to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should only remove first ordered distinct when there are multiple alternating bfs & distinct plans") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("to"), "to AS to")
      .bfsPruningVarExpand("(middle)-[*1..3]-(to)")
      .orderedDistinct(Seq("middle"), "middle AS middle")
      .bfsPruningVarExpand("(from)-[*1..3]-(middle)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("to"), "to AS to")
      .bfsPruningVarExpand("(middle)-[*1..3]-(to)")
      .projection("middle AS middle")
      .bfsPruningVarExpand("(from)-[*1..3]-(middle)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  /**
   * OrderedDistinct negative cases
   */

  test("should not remove ordered distinct on from node") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("from"), "from AS from")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove ordered distinct on to node when not unique leaf") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("to"), "to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test("should remove ordered distinct on from and to nodes when not unique leaf") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("from"), "from AS from", "to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove ordered distinct on to node when there are other distinct columns") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("to"), "to AS to", "1 AS one")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove ordered distinct on from and to nodes when there are other distinct columns") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("from"), "from AS from", "to AS to", "1 AS one")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove ordered distinct on to node when other cardinality increasing plan before bfs") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("to"), "to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .unwind("[1] AS one")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove ordered distinct on to node when other cardinality increasing plan after bfs") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("to"), "to AS to")
      .unwind("[1] AS one")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove ordered distinct on to node when there are multiple bfs plans") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("to"), "to AS to")
      .bfsPruningVarExpand("(middle)-[*1..3]-(to)")
      .bfsPruningVarExpand("(from)-[*1..3]-(middle)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  /**
   * Aggregation positive cases
   */

  test("should relax collect(DISTINCT to) on aggregation") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("count(DISTINCT to) AS x"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("count(to) AS x"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should relax collect(DISTINCT to) on aggregation when there are other aggregation functions") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("count(DISTINCT to) AS x", "collect(DISTINCT from) AS y"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("count(to) AS x", "collect(DISTINCT from) AS y"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  /**
   * Aggregation negative cases
   */

  test("should not remove aggregation collect(DISTINCT from)") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("count(DISTINCT from) AS x"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove aggregation collect(DISTINCT to) when non unique leaf") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("count(DISTINCT to) AS x"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove aggregation min(size(r))") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("min(depth) AS x"))
      .bfsPruningVarExpand("(a)-[r*1..]-(b)", depthName = Some("depth"))
      .argument("a")
      .build()

    assertNotRewritten(before)
  }

  /**
   * Grouping Aggregation positive cases
   */

  test("should relax from, collect(DISTINCT to) on aggregation") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("from AS from"), Seq("count(DISTINCT to) AS x"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("from AS from"), Seq("count(to) AS x"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should relax aliased from, collect(DISTINCT to) on aggregation") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("morf AS morf"), Seq("count(DISTINCT to) AS x"))
      .projection("from AS morf")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("morf AS morf"), Seq("count(to) AS x"))
      .projection("from AS morf")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should relax from,other collect(DISTINCT to) on aggregation") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("from AS from", "1 AS one"), Seq("count(DISTINCT to) AS x"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("from AS from", "1 AS one"), Seq("count(to) AS x"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove grouping aggregation when group=to and aggregate=min(depth)") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("to AS to"), Seq("min(depth) AS min"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("to AS to", "depth AS min")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove grouping aggregation when group=aliased to and aggregate=min(depth)") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("tutu AS tutu"), Seq("min(depth) AS min"))
      .projection("to AS tutu")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("tutu AS tutu", "depth AS min")
      .projection("to AS tutu")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove grouping aggregation when group=from,to and aggregate=min(depth)") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("from AS from", "to AS to"), Seq("min(depth) AS min"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("from AS from", "to AS to", "depth AS min")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove grouping aggregation when group=aliased from, aliased to and aggregate=min(depth)") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("morf AS morf", "tutu AS tutu"), Seq("min(depth) AS min"))
      .projection("from AS morf", "to AS tutu")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("morf AS morf", "tutu AS tutu", "depth AS min")
      .projection("from AS morf", "to AS tutu")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove grouping aggregation when group=to and multiple aggregate=min(depth)") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("to AS to"), Seq("min(depth) AS min1", "min(depth) AS min2"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("to AS to", "depth AS min1", "depth AS min2")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove grouping aggregation when group=from,to and multiple aggregate=min(depth)") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("from AS from", "to AS to"), Seq("min(depth) AS min1", "min(depth) AS min2"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("from AS from", "to AS to", "depth AS min1", "depth AS min2")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should only remove first aggregation when there are multiple alternating bfs & aggregation plans") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("middle AS middle", "to AS to"), Seq("min(depth2) AS min3", "min(depth2) AS min4"))
      .bfsPruningVarExpand("(middle)-[*1..3]-(to)", depthName = Some("depth2"))
      .aggregation(Seq("middle AS middle"), Seq("min(depth1) AS min1", "min(depth1) AS min2"))
      .bfsPruningVarExpand("(from)-[*1..3]-(middle)", depthName = Some("depth1"))
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("middle AS middle", "to AS to"), Seq("min(depth2) AS min3", "min(depth2) AS min4"))
      .bfsPruningVarExpand("(middle)-[*1..3]-(to)", depthName = Some("depth2"))
      .projection("middle AS middle", "depth1 AS min1", "depth1 AS min2")
      .bfsPruningVarExpand("(from)-[*1..3]-(middle)", depthName = Some("depth1"))
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  /**
   * Grouping Aggregation negative cases
   */

  test("should not relax misc grouping key, collect(DISTINCT to) on aggregation") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("1 AS one"), Seq("count(DISTINCT to) AS x"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not relax from, collect(DISTINCT misc) on aggregation") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("from AS from"), Seq("count(DISTINCT one) AS x"))
      .projection("1 AS one")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove grouping aggregation when group=from and aggregate=collect(to)") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("from AS from"), Seq("collect(to) AS x"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove grouping aggregation when grouping on from, to, and another column") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("from AS from", "to AS to", "1 AS one"), Seq("min(depth) AS x"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove grouping aggregation when group=from and aggregate=min(depth)") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("from AS from"), Seq("min(depth) AS min"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  /**
   * Ordered Grouping Aggregation positive cases
   */

  test("should relax from, collect(DISTINCT to) on ordered aggregation") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("from AS from"), Seq("count(DISTINCT to) AS x"), Seq("from"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("from AS from"), Seq("count(to) AS x"), Seq("from"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should relax from,other collect(DISTINCT to) on ordered aggregation") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("from AS from", "1 AS one"), Seq("count(DISTINCT to) AS x"), Seq("from"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("from AS from", "1 AS one"), Seq("count(to) AS x"), Seq("from"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "should remove ordered grouping aggregation when group=to and aggregate=min(depth) with to is referenced by other operators"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("to AS to"), Seq("min(depth) AS min"), Seq("to"))
      .projection("to AS tutu")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("to AS to", "depth AS min")
      .projection("to AS tutu")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove ordered grouping aggregation when group=from,to and aggregate=min(depth)") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("from AS from", "to AS to"), Seq("min(depth) AS min"), Seq("from"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("from AS from", "to AS to", "depth AS min")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove ordered grouping aggregation when group=to and multiple aggregate=min(depth)") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("to AS to"), Seq("min(depth) AS min1", "min(depth) AS min2"), Seq("to"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("to AS to", "depth AS min1", "depth AS min2")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should remove ordered grouping aggregation when group=from,to and multiple aggregate=min(depth)") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("from AS from", "to AS to"), Seq("min(depth) AS min1", "min(depth) AS min2"), Seq("from"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection("from AS from", "to AS to", "depth AS min1", "depth AS min2")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "should only remove first ordered aggregation when there are multiple alternating bfs & ordered aggregation plans"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(
        Seq("middle AS middle", "to AS to"),
        Seq("min(depth2) AS min3", "min(depth2) AS min4"),
        Seq("middle")
      )
      .bfsPruningVarExpand("(middle)-[*1..3]-(to)", depthName = Some("depth2"))
      .orderedAggregation(Seq("middle AS middle"), Seq("min(depth1) AS min1", "min(depth1) AS min2"), Seq("middle"))
      .bfsPruningVarExpand("(from)-[*1..3]-(middle)", depthName = Some("depth1"))
      .argument("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(
        Seq("middle AS middle", "to AS to"),
        Seq("min(depth2) AS min3", "min(depth2) AS min4"),
        Seq("middle")
      )
      .bfsPruningVarExpand("(middle)-[*1..3]-(to)", depthName = Some("depth2"))
      .projection("middle AS middle", "depth1 AS min1", "depth1 AS min2")
      .bfsPruningVarExpand("(from)-[*1..3]-(middle)", depthName = Some("depth1"))
      .argument("from")
      .build()

    rewrite(before) should equal(after)
  }

  /**
   * Ordered Grouping Aggregation negative cases
   */

  test("should not relax misc grouping key, collect(DISTINCT to) on ordered aggregation") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("one AS one"), Seq("count(DISTINCT to) AS x"), Seq("one"))
      .projection("1 AS one")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not relax from, collect(DISTINCT misc) on ordered aggregation") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("from AS from"), Seq("count(DISTINCT one) AS x"), Seq("from"))
      .projection("1 AS one")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove ordered grouping aggregation when group=from and aggregate=collect(to)") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("from AS from"), Seq("collect(to) AS x"), Seq("from"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not ordered remove grouping aggregation when grouping on from, to, and another column") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("from AS from", "to AS to", "1 AS one"), Seq("min(depth) AS x"), Seq("from"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not remove ordered grouping aggregation when group=from and aggregate=min(depth)") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("from AS from"), Seq("min(depth) AS min"), Seq("from"))
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", depthName = Some("depth"))
      .argument("from")
      .build()

    assertNotRewritten(before)
  }

  private def assertNotRewritten(p: LogicalPlan): Unit = {
    rewrite(p) should equal(p)
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    p.endoRewrite(bfsAggregationRemover)
}
