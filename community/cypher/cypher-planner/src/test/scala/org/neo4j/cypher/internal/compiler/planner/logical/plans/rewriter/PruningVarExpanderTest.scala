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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PruningVarExpanderTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("simplest possible query that can use PruningVarExpand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .expand("(from)-[*2..3]-(to)")
      .allNodeScan("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .pruningVarExpand("(from)-[*2..3]-(to)")
      .allNodeScan("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("simplest possible query that can use BFSPruningVarExpand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .expand("(from)-[*1..3]->(to)")
      .allNodeScan("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]->(to)")
      .allNodeScan("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should not rewrite simplest possible VarExpandInto plan with DFS policy") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .expandInto("(from)-[*1..3]->(to)")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before, policy = VarExpandRewritePolicy.PreferDFS)
  }

  test("should rewrite simplest possible VarExpandInto plan with BFS policy") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .expandInto("(from)-[*1..3]->(to)")
      .allNodeScan("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]->(to)", mode = ExpandInto)
      .allNodeScan("from")
      .build()

    rewrite(before, policy = VarExpandRewritePolicy.PreferBFS) should equal(after)
  }

  test("do use BFSPruningVarExpand for undirected search when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .expand("(from)-[*0..3]-(to)")
      .allNodeScan("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .bfsPruningVarExpand("(from)-[*0..3]-(to)")
      .allNodeScan("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("use BFSPruningVarExpand for undirected search when min depth is 1") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .expand("(from)-[*1..3]-(to)")
      .allNodeScan("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)")
      .allNodeScan("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("ordered distinct with pruningVarExpand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("a"), "a AS a")
      .expandAll("(a)-[*2..3]->(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("a"), "a AS a")
      .pruningVarExpand("(a)-[*2..3]->(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("ordered distinct with BFSPruningVarExpand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("a"), "a AS a")
      .expandAll("(a)-[*1..3]->(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("a"), "a AS a")
      .bfsPruningVarExpand("(a)-[*1..3]->(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("query with distinct aggregation") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("count(DISTINCT to) AS x"))
      .expand("(from)-[*2..3]-(to)")
      .allNodeScan("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("count(DISTINCT to) AS x"))
      .pruningVarExpand("(from)-[*2..3]-(to)")
      .allNodeScan("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("query with distinct aggregation and BFSPruningVarExpand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("count(DISTINCT to) AS x"))
      .expand("(from)<-[*1..3]-(to)")
      .allNodeScan("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("count(DISTINCT to) AS x"))
      .bfsPruningVarExpand("(from)<-[*1..3]-(to)")
      .allNodeScan("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("should not rewrite expand into query with distinct aggregation with DFS policy") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("count(DISTINCT to) AS x"))
      .expandInto("(from)<-[*1..3]-(to)")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before, policy = VarExpandRewritePolicy.PreferDFS)
  }

  test("should rewrite expand into query with distinct aggregation with BFS policy") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("count(DISTINCT to) AS x"))
      .expandInto("(from)<-[*1..3]-(to)")
      .allNodeScan("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("count(DISTINCT to) AS x"))
      .bfsPruningVarExpand("(from)<-[*1..3]-(to)", mode = ExpandInto)
      .allNodeScan("from")
      .build()

    rewrite(before, policy = VarExpandRewritePolicy.PreferBFS) should equal(after)
  }

  test("ordered grouping aggregation") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("a AS a"), Seq("count(distinct b) AS c"), Seq("a"))
      .expand("(a)-[*2..3]->(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("a AS a"), Seq("count(distinct b) AS c"), Seq("a"))
      .pruningVarExpand("(a)-[*2..3]->(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("Simple query that filters between expand and distinct") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .filter("to:X")
      .expand("(from)-[*2..3]-(to)")
      .allNodeScan("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .filter("to:X")
      .pruningVarExpand("(from)-[*2..3]-(to)")
      .allNodeScan("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("Simple query that filters between expand and distinct and BFSPruningVarExpand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .filter("to:X")
      .expand("(from)-[*1..3]->(to)")
      .allNodeScan("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .filter("to:X")
      .bfsPruningVarExpand("(from)-[*1..3]->(to)")
      .allNodeScan("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("Query that aggregates before making the result DISTINCT") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .expand("(from)-[*1..3]-(to)")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test("Double var expand with distinct result to both pruning") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .expand("(b)-[:T*2..3]-(c)")
      .expand("(a)-[:R*2..3]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .pruningVarExpand("(b)-[:T*2..3]-(c)")
      .pruningVarExpand("(a)-[:R*2..3]-(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("Double var expand with distinct result to both BFSPruningVarExpand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .expand("(b)-[:T*1..3]->(c)")
      .expand("(a)-[:R*1..3]->(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .bfsPruningVarExpand("(b)-[:T*1..3]->(c)")
      .bfsPruningVarExpand("(a)-[:R*1..3]->(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("var expand followed by normal expand to first pruning") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .expand("(b)-[:T]-(c)")
      .expand("(a)-[:R*2..3]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .expand("(b)-[:T]-(c)")
      .pruningVarExpand("(a)-[:R*2..3]-(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("var expand followed by normal expand to first BFSPruningVarExpand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .expand("(b)<-[:T]-(c)")
      .expand("(a)<-[:R*1..3]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .expand("(b)<-[:T]-(c)")
      .bfsPruningVarExpand("(a)<-[:R*1..3]-(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("double var expand with grouping aggregation to one pruning") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("c AS c"), Seq("min(size(r)) AS distance"))
      .expand("(b)-[*2..3]-(c)")
      .expand("(a)-[r*2..]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("c AS c"), Seq("min(size(r)) AS distance"))
      .pruningVarExpand("(b)-[*2..3]-(c)")
      .expand("(a)-[r*2..]-(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("double var expand with grouping aggregation to one bfs when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("size(r2) AS group"), Seq("min(size(r1)) AS distance"))
      .expand("(b)-[r2*0..3]-(c)")
      .expand("(a)-[r1*0..]-(b)")
      .allNodeScan("a")
      .build()

    val depthNameStr = "  depth0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("size(r2) AS group"), Seq(s"min(`${depthNameStr}`) AS distance"))
      .expand("(b)-[r2*0..3]-(c)")
      .bfsPruningVarExpand("(a)-[r1*0..]-(b)", depthName = Some(depthNameStr))
      .allNodeScan("a")
      .build()

    rewrite(before, names = Seq(depthNameStr)) should equal(after)
  }

  test("double var expand with grouping aggregation to one bfs when min depth is 1") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("size(r2) AS group"), Seq("min(size(r1)) AS distance"))
      .expand("(b)-[r2*1..3]-(c)")
      .expand("(a)-[r1*1..]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("size(r2) AS group"), Seq(s"min(`$depthStr`) AS distance"))
      .expand("(b)-[r2*1..3]-(c)")
      .bfsPruningVarExpand("(a)-[r1*1..]-(b)", depthName = Some(depthStr))
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("double var expand with grouping aggregation to bfs and pruning when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("c AS c"), Seq("min(size(r)) AS distance"))
      .expand("(b)-[*2..3]-(c)")
      .expand("(a)-[r*0..]-(b)")
      .allNodeScan("a")
      .build()

    val depthNameStr = "  depth0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("c AS c"), Seq(s"min(`${depthNameStr}`) AS distance"))
      .pruningVarExpand("(b)-[*2..3]-(c)")
      .bfsPruningVarExpand("(a)-[r*0..]-(b)", depthName = Some(depthNameStr))
      .allNodeScan("a")
      .build()

    rewrite(before, names = Seq(depthNameStr)) should equal(after)
  }

  val depthStr = "  UNNAMED0"

  test("double var expand with grouping aggregation to bfs and pruning when min depth is 1") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("c AS c"), Seq("min(size(r)) AS distance"))
      .expand("(b)-[*2..3]-(c)")
      .expand("(a)-[r*1..]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("c AS c"), Seq(s"min(`${depthStr}`) AS distance"))
      .pruningVarExpand("(b)-[*2..3]-(c)")
      .bfsPruningVarExpand("(a)-[r*1..]-(b)", depthName = Some(depthStr))
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("double var expand with grouping aggregation to both pruning") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("a AS a"), Seq("collect(distinct b) AS aggB", "collect(distinct c) AS aggC"))
      .expand("(b)-[*2..3]-(c)")
      .expand("(a)-[*2..2]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("a AS a"), Seq("collect(distinct b) AS aggB", "collect(distinct c) AS aggC"))
      .pruningVarExpand("(b)-[*2..3]-(c)")
      .pruningVarExpand("(a)-[*2..2]-(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("double var expand with grouping aggregation to both bfs when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Map("a" -> v"a"),
        Map(
          "agg1" -> min(size(v"r2")),
          "agg2" -> min(length(varLengthPathExpression(v"a", v"r1", v"b"))),
          "agg3" -> min(length(varLengthPathExpression(v"a", v"r1", v"b")))
        ),
        None
      )
      .expand("(b)-[r2*0..3]-(c)")
      .expand("(a)-[r1*0..]-(b)")
      .allNodeScan("a")
      .build()

    val r1DepthNameStr = "  depth0"
    val r2DepthNameStr = "  depth1"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Seq("a AS a"),
        Seq(s"min(`$r1DepthNameStr`) AS agg1", s"min(`$r2DepthNameStr`) AS agg2", s"min(`$r2DepthNameStr`) AS agg3")
      )
      .bfsPruningVarExpand("(b)-[*0..3]-(c)", depthName = Some(r1DepthNameStr))
      .bfsPruningVarExpand("(a)-[*0..]-(b)", depthName = Some(r2DepthNameStr))
      .allNodeScan("a")
      .build()

    rewrite(before, names = Seq(r1DepthNameStr, r2DepthNameStr)) should equal(after)
  }

  test("double var expand with grouping aggregation to both bfs when min depth is 1") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Map("a" -> v"a"),
        Map(
          "agg1" -> min(size(v"r2")),
          "agg2" -> min(length(varLengthPathExpression(v"a", v"r1", v"b"))),
          "agg3" -> min(length(varLengthPathExpression(v"a", v"r1", v"b")))
        ),
        None
      )
      .expand("(b)-[r2*1..3]-(c)")
      .expand("(a)-[r1*1..]-(b)")
      .allNodeScan("a")
      .build()

    val r1DepthNameStr = "  depth0"
    val r2DepthNameStr = "  depth1"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Seq("a AS a"),
        Seq(s"min(`$r1DepthNameStr`) AS agg1", s"min(`$r2DepthNameStr`) AS agg2", s"min(`$r2DepthNameStr`) AS agg3")
      )
      .bfsPruningVarExpand("(b)-[*1..3]-(c)", depthName = Some(r1DepthNameStr))
      .bfsPruningVarExpand("(a)-[*1..]-(b)", depthName = Some(r2DepthNameStr))
      .allNodeScan("a")
      .build()

    rewrite(before, names = Seq(r1DepthNameStr, r2DepthNameStr)) should equal(after)
  }

  test("optional match can be solved with PruningVarExpand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .apply()
      .|.optional("a")
      .|.expand("(a)-[:R*2..3]-(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .apply()
      .|.optional("a")
      .|.pruningVarExpand("(a)-[:R*2..3]-(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("optional match can be solved with BFSPruningVarExpand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .apply()
      .|.optional("a")
      .|.expand("(a)-[:R*1..3]->(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .apply()
      .|.optional("a")
      .|.bfsPruningVarExpand("(a)-[:R*1..3]->(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("can plan PruningVarExpand when VarExpand is on RHS of NodeHashJoin and Distinct is above Join") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .nodeHashJoin("a")
      .|.expand("(b)-[:R*2..3]-(a)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .nodeHashJoin("a")
      .|.pruningVarExpand("(b)-[:R*2..3]-(a)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "can plan BFSPruningVarExpand when VarExpand is on RHS of NodeHashJoin and Distinct is above Join when min depth is 0"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .nodeHashJoin("a")
      .|.expand("(b)-[:R*0..3]-(a)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .nodeHashJoin("a")
      .|.bfsPruningVarExpand("(b)-[:R*0..3]-(a)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "can plan BFSPruningVarExpand when VarExpand is on RHS of NodeHashJoin and Distinct is above Join when min depth is 1"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .nodeHashJoin("a")
      .|.expand("(b)-[:R*1..3]-(a)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .nodeHashJoin("a")
      .|.bfsPruningVarExpand("(b)-[:R*1..3]-(a)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("can plan PruningVarExpand when VarExpand is on RHS of ValueHashJoin and Distinct is above Join") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .valueHashJoin("a=b")
      .|.expand("(b)-[:R*2..3]-(a)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .valueHashJoin("a=b")
      .|.pruningVarExpand("(b)-[:R*2..3]-(a)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "can plan BFSPruningVarExpand when VarExpand is on RHS of ValueHashJoin and Distinct is above Join when min depth is 0"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .valueHashJoin("a=b")
      .|.expand("(b)-[:R*0..3]-(a)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .valueHashJoin("a=b")
      .|.bfsPruningVarExpand("(b)-[:R*0..3]-(a)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "can plan BFSPruningVarExpand when VarExpand is on RHS of ValueHashJoin and Distinct is above Join when min depth is 1"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .valueHashJoin("a=b")
      .|.expand("(b)-[:R*1..3]-(a)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .valueHashJoin("a=b")
      .|.bfsPruningVarExpand("(b)-[:R*1..3]-(a)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("should not plan PruningVarExpand when VarExpand is on RHS of ValueHashJoin when path is join predicate") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .valueHashJoin("pathA=pathB")
      .|.projection(Map("pathB" -> varLengthPathExpression(
        v"a",
        v"r",
        v"b",
        SemanticDirection.BOTH
      )))
      .|.expand("(b)-[r:R*2..3]-(a)")
      .|.allNodeScan("b")
      .projection(Map("pathA" -> varLengthPathExpression(
        v"a",
        v"r",
        v"b",
        SemanticDirection.BOTH
      )))
      .expand("(a)-[r:R*2..3]-(b)")
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("should not plan BFSPruningVarExpand when VarExpand is on RHS of ValueHashJoin when path is join predicate") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .valueHashJoin("pathA=pathB")
      .|.projection(Map("pathB" -> varLengthPathExpression(
        v"a",
        v"r",
        v"b",
        SemanticDirection.BOTH
      )))
      .|.expand("(b)-[r:R*1..3]-(a)")
      .|.allNodeScan("b")
      .projection(Map("pathA" -> varLengthPathExpression(
        v"a",
        v"r",
        v"b",
        SemanticDirection.BOTH
      )))
      .expand("(a)-[r:R*1..3]-(b)")
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("can plan PruningVarExpand when VarExpand is on LHS of NodeHashJoin and Distinct is above Join") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .nodeHashJoin("a")
      .|.allNodeScan("b")
      .expand("(b)-[:R*2..3]-(a)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .nodeHashJoin("a")
      .|.allNodeScan("b")
      .pruningVarExpand("(b)-[:R*2..3]-(a)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "can plan BFSPruningVarExpand when VarExpand is on LHS of NodeHashJoin and Distinct is above Join when min depth is 0"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .nodeHashJoin("a")
      .|.allNodeScan("b")
      .expand("(b)-[:R*0..3]-(a)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .nodeHashJoin("a")
      .|.allNodeScan("b")
      .bfsPruningVarExpand("(b)-[:R*0..3]-(a)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "can plan BFSPruningVarExpand when VarExpand is on LHS of NodeHashJoin and Distinct is above Join when min depth is 1"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .nodeHashJoin("a")
      .|.allNodeScan("b")
      .expand("(b)-[:R*1..3]-(a)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .nodeHashJoin("a")
      .|.allNodeScan("b")
      .bfsPruningVarExpand("(b)-[:R*1..3]-(a)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("can plan PruningVarExpand when VarExpand is on LHS of ValueHashJoin and Distinct is above Join") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .valueHashJoin("b=c")
      .|.allNodeScan("c")
      .expand("(a)-[:R*2..3]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .valueHashJoin("b=c")
      .|.allNodeScan("c")
      .pruningVarExpand("(a)-[:R*2..3]-(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "can plan BFSPruningVarExpand when VarExpand is on LHS of ValueHashJoin and Distinct is above Join when min depth is 0"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .valueHashJoin("b=c")
      .|.allNodeScan("c")
      .expand("(a)-[:R*0..3]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .valueHashJoin("b=c")
      .|.allNodeScan("c")
      .bfsPruningVarExpand("(a)-[:R*0..3]-(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "can plan BFSPruningVarExpand when VarExpand is on LHS of ValueHashJoin and Distinct is above Join when min depth is 1"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .valueHashJoin("b=c")
      .|.allNodeScan("c")
      .expand("(a)-[:R*1..3]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .valueHashJoin("b=c")
      .|.allNodeScan("c")
      .bfsPruningVarExpand("(a)-[:R*1..3]-(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("should not solve with PruningVarExpand when VarExpand is on LHS of Apply and Distinct is above Apply") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .apply()
      .|.argument("a")
      .expand("(a)-[:R*2..3]-(b)")
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("should not solve with BFSPruningVarExpand when VarExpand is on LHS of Apply and Distinct is above Apply") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .apply()
      .|.argument("a")
      .expand("(a)-[:R*1..3]-(b)")
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("can solve with PruningVarExpand when VarExpand is on both LHS and RHS of Apply and Distinct is above Apply") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b", "c AS c")
      .apply()
      .|.expand("(a)-[:R*2..3]-(c)")
      .|.argument("a")
      .expand("(a)-[:R*2..3]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b", "c AS c")
      .apply()
      .|.pruningVarExpand("(a)-[:R*2..3]-(c)")
      .|.argument("a")
      .expand("(a)-[:R*2..3]-(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "can solve with BFSPruningVarExpand when VarExpand is on both LHS and RHS of Apply and Distinct is above Apply when min depth is 0"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b", "c AS c")
      .apply()
      .|.expand("(a)-[:R*0..3]-(c)")
      .|.argument("a")
      .expand("(a)-[:R*0..3]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b", "c AS c")
      .apply()
      .|.bfsPruningVarExpand("(a)-[:R*0..3]-(c)")
      .|.argument("a")
      .expand("(a)-[:R*0..3]-(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "can solve with BFSPruningVarExpand when VarExpand is on both LHS and RHS of Apply and Distinct is above Apply when min depth is 1"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b", "c AS c")
      .apply()
      .|.expand("(a)-[:R*1..3]-(c)")
      .|.argument("a")
      .expand("(a)-[:R*1..3]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b", "c AS c")
      .apply()
      .|.bfsPruningVarExpand("(a)-[:R*1..3]-(c)")
      .|.argument("a")
      .expand("(a)-[:R*1..3]-(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("can solve with PruningVarExpand when VarExpand is on RHS of SemiApply") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .semiApply()
      .|.expand("(a)-[:R*2..3]-(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .semiApply()
      .|.pruningVarExpand("(a)-[:R*2..3]-(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("can solve with BFSPruningVarExpand when VarExpand is on RHS of SemiApply when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .semiApply()
      .|.expand("(a)-[:R*0..3]-(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .semiApply()
      .|.bfsPruningVarExpand("(a)-[:R*0..3]-(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("can solve with BFSPruningVarExpand when VarExpand is on RHS of SemiApply when min depth is 1") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .semiApply()
      .|.expand("(a)-[:R*1..3]-(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .semiApply()
      .|.bfsPruningVarExpand("(a)-[:R*1..3]-(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("should not solve with PruningVarExpand when VarExpand is on LHS of SemiApply and Distinct is above SemiApply") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .semiApply()
      .|.argument("a")
      .expand("(a)-[:R*2..3]-(b)")
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test(
    "should not solve with BFSPruningVarExpand when VarExpand is on LHS of SemiApply and Distinct is above SemiApply"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .semiApply()
      .|.argument("a")
      .expand("(a)-[:R*1..3]-(b)")
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("should not rewrite when doing non-distinct aggregation") {
    // Should not be rewritten since it's asking for a count of all paths leading to a node
    // match (a)-[*1..3]-(b) return b, count(*)

    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("to AS to"), Seq("count(*) AS count"))
      .expand("(from)-[*1..3]-(to)")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test("on longer var-lengths, we also use PruningVarExpand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .expand("(from)-[*4..5]-(to)")
      .allNodeScan("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .pruningVarExpand("(from)-[*4..5]-(to)")
      .allNodeScan("from")
      .build()

    rewrite(before) should equal(after)
  }

  test("do not use pruning for length=1") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .expand("(from)-[*1..1]-(to)")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test("do not use pruning for pathExpressions when path is needed") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("d AS d")
      .projection("nodes(path) AS d")
      .projection(Map("path" -> varLengthPathExpression(
        v"from",
        v"r",
        v"to",
        SemanticDirection.BOTH
      )))
      .expand("(from)-[r*0..2]-(to)")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test(
    "do not use pruning-varexpand when both sides of the var-length-relationship are already known with DFS policy"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .expand("(from)-[*1..3]-(to)", expandMode = ExpandInto)
      .cartesianProduct()
      .|.allNodeScan("to")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before, policy = VarExpandRewritePolicy.PreferDFS)
  }

  test("use pruning-varexpand when both sides of the var-length-relationship are already known with BFS policy") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .expand("(from)-[*1..3]-(to)", expandMode = ExpandInto)
      .cartesianProduct()
      .|.allNodeScan("to")
      .allNodeScan("from")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("to AS to")
      .bfsPruningVarExpand("(from)-[*1..3]-(to)", mode = ExpandInto)
      .cartesianProduct()
      .|.allNodeScan("to")
      .allNodeScan("from")
      .build()

    rewrite(before, policy = VarExpandRewritePolicy.PreferBFS) should equal(after)
  }

  test("should handle insanely long logical plans without running out of stack") {
    val leafPlan: LogicalPlan = Argument(Set(v"x"))
    var plan = leafPlan
    (1 until 10000) foreach { _ =>
      plan = Selection(Seq(trueLiteral), plan)
    }

    rewrite(plan) // should not throw exception
  }

  test("cartesian product can be solved with PruningVarExpand when VarExpand is on LHS") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .cartesianProduct()
      .|.allNodeScan("a")
      .expand("(b)-[:R*2..3]-(c)")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .cartesianProduct()
      .|.allNodeScan("a")
      .pruningVarExpand("(b)-[:R*2..3]-(c)")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test("cartesian product can be solved with BFSPruningVarExpand when VarExpand is on LHS when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .cartesianProduct()
      .|.allNodeScan("a")
      .expand("(b)-[:R*0..3]-(c)")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .cartesianProduct()
      .|.allNodeScan("a")
      .bfsPruningVarExpand("(b)-[:R*0..3]-(c)")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test("cartesian product can not be solved with BFSPruningVarExpand when VarExpand is on LHS when min depth is 1") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .cartesianProduct()
      .|.allNodeScan("a")
      .expand("(b)-[:R*1..3]-(c)")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .cartesianProduct()
      .|.allNodeScan("a")
      .bfsPruningVarExpand("(b)-[:R*1..3]-(c)")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test("cartesian product can be solved with PruningVarExpand when VarExpand is on RHS") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .cartesianProduct()
      .|.expand("(b)-[:R*2..3]-(c)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .cartesianProduct()
      .|.pruningVarExpand("(b)-[:R*2..3]-(c)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("cartesian product can be solved with BFSPruningVarExpand when VarExpand is on RHS when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .cartesianProduct()
      .|.expand("(b)-[:R*0..3]-(c)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .cartesianProduct()
      .|.bfsPruningVarExpand("(b)-[:R*0..3]-(c)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("cartesian product can be solved with BFSPruningVarExpand when VarExpand is on RHS when min depth is 1") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .cartesianProduct()
      .|.expand("(b)-[:R*1..3]-(c)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .cartesianProduct()
      .|.bfsPruningVarExpand("(b)-[:R*1..3]-(c)")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("cartesian product can be solved with PruningVarExpand when VarExpand is on both sides") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .cartesianProduct()
      .|.expand("(a)-[:R*2..3]-(c2)")
      .|.allNodeScan("a")
      .expand("(b)-[:R*2..3]-(c1)")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .cartesianProduct()
      .|.pruningVarExpand("(a)-[:R*2..3]-(c2)")
      .|.allNodeScan("a")
      .pruningVarExpand("(b)-[:R*2..3]-(c1)")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test("cartesian product can be solved with BFSPruningVarExpand when VarExpand is on both sides when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .cartesianProduct()
      .|.expand("(a)-[:R*0..3]-(c2)")
      .|.allNodeScan("a")
      .expand("(b)-[:R*0..3]-(c1)")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .cartesianProduct()
      .|.bfsPruningVarExpand("(a)-[:R*0..3]-(c2)")
      .|.allNodeScan("a")
      .bfsPruningVarExpand("(b)-[:R*0..3]-(c1)")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "cartesian product can be solved with BFSPruningVarExpand when VarExpand is on both sides when min depth is 1"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .cartesianProduct()
      .|.expand("(a)-[:R*1..3]-(c2)")
      .|.allNodeScan("a")
      .expand("(b)-[:R*1..3]-(c1)")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .cartesianProduct()
      .|.bfsPruningVarExpand("(a)-[:R*1..3]-(c2)")
      .|.allNodeScan("a")
      .bfsPruningVarExpand("(b)-[:R*1..3]-(c1)")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test("cartesian product can not be solved with PruningVarExpand on either side when relationships are used on top") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .filter("size(r1)>1 OR size(r2)>1")
      .cartesianProduct()
      .|.expand("(a)-[r2:R*2..3]-(c2)")
      .|.allNodeScan("a")
      .expand("(b)-[r1:R*2..3]-(c1)")
      .allNodeScan("b")
      .build()

    assertNotRewritten(before)
  }

  test(
    "cartesian product can not be solved with BFSPruningVarExpand on either side when relationships are used on top"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .filter("size(r1)>1 OR size(r2)>1")
      .cartesianProduct()
      .|.expand("(a)-[r2:R*1..3]-(c2)")
      .|.allNodeScan("a")
      .expand("(b)-[r1:R*1..3]-(c1)")
      .allNodeScan("b")
      .build()

    assertNotRewritten(before)
  }

  test("Union can be solved with PruningVarExpand when VarExpand is on LHS") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .union()
      .|.allNodeScan("a")
      .expand("(b)-[:R*2..3]-(c)")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .union()
      .|.allNodeScan("a")
      .pruningVarExpand("(b)-[:R*2..3]-(c)")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test("Union can be solved with BFSPruningVarExpand when VarExpand is on LHS when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .union()
      .|.allNodeScan("a")
      .expand("(b)-[:R*0..3]-(c)")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .union()
      .|.allNodeScan("a")
      .bfsPruningVarExpand("(b)-[:R*0..3]-(c)")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test("Union can be solved with BFSPruningVarExpand when VarExpand is on LHS when min depth is 1") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .union()
      .|.allNodeScan("a")
      .expand("(b)-[:R*1..3]-(c)")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .union()
      .|.allNodeScan("a")
      .bfsPruningVarExpand("(b)-[:R*1..3]-(c)")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test("Union can be solved with PruningVarExpand when VarExpand is on RHS") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .union()
      .|.expand("(b)-[:R*2..3]-(c)")
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .union()
      .|.pruningVarExpand("(b)-[:R*2..3]-(c)")
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test("Union can be solved with BFSPruningVarExpand when VarExpand is on RHS when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .union()
      .|.expand("(b)-[:R*0..3]-(c)")
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .union()
      .|.bfsPruningVarExpand("(b)-[:R*0..3]-(c)")
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test("Union can be solved with BFSPruningVarExpand when VarExpand is on RHS when min depth is 1") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .union()
      .|.expand("(b)-[:R*1..3]-(c)")
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c AS c")
      .union()
      .|.bfsPruningVarExpand("(b)-[:R*1..3]-(c)")
      .|.allNodeScan("a")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test("Union can be solved with PruningVarExpand when VarExpand is on both sides") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .union()
      .|.expand("(a)-[:R*2..3]-(c2)")
      .|.allNodeScan("a")
      .expand("(b)-[:R*2..3]-(c1)")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .union()
      .|.pruningVarExpand("(a)-[:R*2..3]-(c2)")
      .|.allNodeScan("a")
      .pruningVarExpand("(b)-[:R*2..3]-(c1)")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test("Union can be solved with BFSPruningVarExpand when VarExpand is on both sides when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .union()
      .|.expand("(a)-[:R*0..3]-(c2)")
      .|.allNodeScan("a")
      .expand("(b)-[:R*0..3]-(c1)")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .union()
      .|.bfsPruningVarExpand("(a)-[:R*0..3]-(c2)")
      .|.allNodeScan("a")
      .bfsPruningVarExpand("(b)-[:R*0..3]-(c1)")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test("Union can be solved with BFSPruningVarExpand when VarExpand is on both sides when min depth is 1") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .union()
      .|.expand("(a)-[:R*1..3]-(c2)")
      .|.allNodeScan("a")
      .expand("(b)-[:R*1..3]-(c1)")
      .allNodeScan("b")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .union()
      .|.bfsPruningVarExpand("(a)-[:R*1..3]-(c2)")
      .|.allNodeScan("a")
      .bfsPruningVarExpand("(b)-[:R*1..3]-(c1)")
      .allNodeScan("b")
      .build()

    rewrite(before) should equal(after)
  }

  test("union can not be solved with PruningVarExpand on either side when relationships are used on top") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .filter("size(r1)>1 OR size(r2)>1")
      .union()
      .|.expand("(a)-[r2:R*2..3]-(c2)")
      .|.allNodeScan("a")
      .expand("(b)-[r1:R*2..3]-(c1)")
      .allNodeScan("b")
      .build()

    assertNotRewritten(before)
  }

  test("union can not be solved with BFSPruningVarExpand on either side when relationships are used on top") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("c1 AS c1", "c2 AS c2")
      .filter("size(r1)>1 OR size(r2)>1")
      .union()
      .|.expand("(a)-[r2:R*1..3]-(c2)")
      .|.allNodeScan("a")
      .expand("(b)-[r1:R*1..3]-(c1)")
      .allNodeScan("b")
      .build()

    assertNotRewritten(before)
  }

  test("do not use pruning-varexpand when upper bound < lower bound") {
    val plan = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .expandAll("(a)-[r*3..2]->(b)")
      .allNodeScan("a")
      .build()

    assertNotRewritten(plan)
  }

  test("do not use pruning when upper limit is not specified") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .expand("(a)-[*2..]-(b)")
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("use bfs pruning even when upper limit is not specified when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .expand("(a)-[*0..]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .bfsPruningVarExpand("(a)-[*0..]-(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("use bfs pruning even when upper limit is not specified when min depth is 1") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .expand("(a)-[*1..]-(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .bfsPruningVarExpand("(a)-[*1..]-(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("use bfs pruning with aggregation when aggregation function is min(length(path)) when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Map.empty[String, Expression],
        Map("distance" -> min(length(varLengthPathExpression(v"a", v"r", v"b")))),
        None
      )
      .expand("(a)-[r*0..]-(b)")
      .allNodeScan("a")
      .build()

    val depthNameStr = "  depth0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq(s"min(`$depthNameStr`) AS distance"))
      .bfsPruningVarExpand("(a)-[r*0..]-(b)", depthName = Some(depthNameStr))
      .allNodeScan("a")
      .build()

    rewrite(before, names = Seq(depthNameStr)) should equal(after)
  }

  test("use bfs pruning with aggregation when aggregation function is min(length(path)) when min depth is 1") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Map.empty[String, Expression],
        Map("distance" -> min(length(varLengthPathExpression(v"a", v"r", v"b")))),
        None
      )
      .expand("(a)-[r*1..]-(b)")
      .allNodeScan("a")
      .build()

    val depthNameStr = "  depth0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq(s"min(`$depthNameStr`) AS distance"))
      .bfsPruningVarExpand("(a)-[r*1..]-(b)", depthName = Some(depthNameStr))
      .allNodeScan("a")
      .build()

    rewrite(before, names = Seq(depthNameStr)) should equal(after)
  }

  test("do not use pruning with aggregation when aggregation function is min(length(path))") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Map.empty[String, Expression],
        Map("distance" -> min(length(varLengthPathExpression(v"a", v"r", v"b")))),
        None
      )
      .expand("(a)-[r*2..]-(b)")
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("use bfs pruning with aggregation when grouping aggregation function is min(length(path)) when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Map("a" -> v"a"),
        Map("distance" -> min(length(varLengthPathExpression(v"a", v"r", v"b")))),
        None
      )
      .expand("(a)-[r*0..]-(b)")
      .allNodeScan("a")
      .build()

    val depthNameStr = "  depth0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("a AS a"), Seq(s"min(`$depthNameStr`) AS distance"))
      .bfsPruningVarExpand("(a)-[*0..]-(b)", depthName = Some(depthNameStr))
      .allNodeScan("a")
      .build()

    rewrite(before, names = Seq(depthNameStr)) should equal(after)
  }

  test(
    "use bfs pruning with aggregation when grouping aggregation function is min(length(path)) when min depth is 1"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Map("a" -> v"a"),
        Map("distance" -> min(length(varLengthPathExpression(v"a", v"r", v"b")))),
        None
      )
      .expand("(a)-[r*1..]-(b)")
      .allNodeScan("a")
      .build()

    val depthNameStr = "  depth0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("a AS a"), Seq(s"min(`$depthNameStr`) AS distance"))
      .bfsPruningVarExpand("(a)-[*1..]-(b)", depthName = Some(depthNameStr))
      .allNodeScan("a")
      .build()

    rewrite(before, names = Seq(depthNameStr)) should equal(after)
  }

  test("do not use pruning with aggregation when grouping aggregation function is min(length(path))") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Map("a" -> v"a"),
        Map("distance" -> min(length(varLengthPathExpression(v"a", v"r", v"b")))),
        None
      )
      .expand("(a)-[r*2..]-(b)")
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("use bfs pruning with aggregation when grouping aggregation function is min(size(r)) when min depth is 0") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("a AS a"), Seq("min(size(r)) AS distance"))
      .expand("(a)-[r*0..]-(b)")
      .allNodeScan("a")
      .build()

    val depthNameStr = "  depth0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("a AS a"), Seq(s"min(`$depthNameStr`) AS distance"))
      .bfsPruningVarExpand("(a)-[r*0..]-(b)", depthName = Some(depthNameStr))
      .allNodeScan("a")
      .build()

    rewrite(before, names = Seq(depthNameStr)) should equal(after)
  }

  test(
    "use bfs pruning with aggregation when grouping aggregation function is min(size(r)) when min depth is 1"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("a AS a"), Seq("min(size(r)) AS distance"))
      .expand("(a)-[r*1..]-(b)")
      .allNodeScan("a")
      .build()

    val depthNameStr = "  depth0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("a AS a"), Seq(s"min(`$depthNameStr`) AS distance"))
      .bfsPruningVarExpand("(a)-[r*1..]-(b)", depthName = Some(depthNameStr))
      .allNodeScan("a")
      .build()

    rewrite(before, names = Seq(depthNameStr)) should equal(after)
  }

  test("do not use pruning with aggregation when grouping aggregation function is min(size(r))") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq("a AS a"), Seq("min(size(r)) AS distance"))
      .expand("(a)-[r*2..]-(b)")
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("should not rewrite when relationship is used in distinct") {
    // Should not be rewritten since it's asking for a count of all paths leading to a node
    // match (a)-[r:R*1..3]->(b) return distinct r

    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("r AS r")
      .expand("(from)-[r:R*1..3]-(to)")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not rewrite when relationship used safely in some aggregations but unsafely in others") {
    // Should not be rewritten since it's asking for a count of all paths leading to a node
    // match (a)-[r:R*1..3]->(b) return a, min(size(r)), min(length(path)), collect(distinct b), collect(distinct r)

    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Map("from" -> v"from"),
        Map(
          "valid1" -> min(length(varLengthPathExpression(v"a", v"r", v"b"))),
          "valid2" -> min(size(v"r")),
          "valid3" -> collect(v"to", distinct = true),
          "invalid" -> collect(v"r", distinct = true)
        ),
        None
      )
      .expand("(from)-[r:R*1..3]-(to)")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test(
    "should not use bfs pruning to solve min(length(path)) when multiple relationships variables are used in the path"
  ) {
    val pathExpression = PathExpressionBuilder
      .node("a")
      .outToVarLength("r", "b")
      .outToVarLength("r2", "c")
      .build()

    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Map.empty[String, Expression],
        Map("distance" -> min(length(pathExpression))),
        None
      )
      .expand("(b)-[r2*1..]-(c)")
      .expand("(a)-[r*1..]-(b)")
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("should not rewrite to pruning when relationships variable is used in predicate of another var expand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .expand("(b)-[*2..3]-(c)", nodePredicates = Seq(Predicate("n", "n.prop > head(r).prop")))
      .expand("(a)-[r:R*2..3]-(b)") // Should not get rewritten
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .pruningVarExpand("(b)-[*2..3]-(c)", nodePredicates = Seq(Predicate("n", "n.prop > head(r).prop")))
      .expand("(a)-[r:R*2..3]-(b)") // Should not get rewritten
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "should not rewrite to bfs pruning when relationships variable is used in predicate of another var expand when min depth is 0"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .expand("(b)-[*0..]-(c)", nodePredicates = Seq(Predicate("n", "n.prop > head(r).prop")))
      .expand("(a)-[r:R*0..3]-(b)") // Should not get rewritten
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .bfsPruningVarExpand("(b)-[*0..]-(c)", nodePredicates = Seq(Predicate("n", "n.prop > head(r).prop")))
      .expand("(a)-[r:R*0..3]-(b)") // Should not get rewritten
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test(
    "should rewrite to bfs pruning when relationships variable is used in predicate of another var expand when min depth is 1"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .expand("(b)-[*]-(c)", nodePredicates = Seq(Predicate("n", "n.prop > head(r).prop")))
      .expand("(a)-[r:R*1..3]-(b)") // Should not get rewritten
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .bfsPruningVarExpand("(b)-[*1..]-(c)", nodePredicates = Seq(Predicate("n", "n.prop > head(r).prop")))
      .expand("(a)-[r:R*1..3]-(b)") // Should not get rewritten
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("should not rewrite to pruning when relationships variable is used in predicate of another optional expand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .optionalExpandAll("(b)--(c)", predicate = Some("c.prop > head(r).prop"))
      .expand("(a)-[r:R*2..3]-(b)") // Should not get rewritten
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test(
    "should not rewrite to bfs pruning when relationships variable is used in predicate of another optional expand"
  ) {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .distinct("b AS b")
      .optionalExpandAll("(b)--(c)", predicate = Some("c.prop > head(r).prop"))
      .expand("(a)-[r:R*1..3]-(b)") // Should not get rewritten
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  private def assertNotRewritten(
    p: LogicalPlan,
    policy: VarExpandRewritePolicy = VarExpandRewritePolicy.default
  ): Unit = {
    rewrite(p, policy = policy) should equal(p)
  }

  private def rewrite(
    p: LogicalPlan,
    names: Seq[String] = Seq.empty,
    policy: VarExpandRewritePolicy = VarExpandRewritePolicy.default
  ): LogicalPlan =
    p.endoRewrite(pruningVarExpander(new VariableNameGenerator(names), policy))

  class VariableNameGenerator(names: Seq[String]) extends AnonymousVariableNameGenerator {
    private val namesIterator = names.iterator

    override def nextName: String = {
      if (namesIterator.hasNext) {
        namesIterator.next()
      } else {
        super.nextName
      }
    }
  }
}
