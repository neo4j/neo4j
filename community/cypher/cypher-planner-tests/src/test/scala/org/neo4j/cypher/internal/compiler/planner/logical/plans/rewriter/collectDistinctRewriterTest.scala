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
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentAsc
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

class collectDistinctRewriterTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport {

  test("should rewrite collect distinct") {
    val before = new LogicalPlanBuilder()
      .produceResults("set")
      .aggregation(Seq.empty, Seq("collect(distinct a.prop) AS set"))
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("set")
      .aggregation(Map.empty[String, Expression], Map("set" -> collectDistinct(prop("a", "prop"))))
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("should rewrite collect non-distinct when IN") {
    val before = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("'foo' AS result")
      .filter("'foo' IN set")
      .aggregation(Seq.empty, Seq("collect(a.prop) AS set"))
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("'foo' AS result")
      .filter("'foo' IN set")
      .aggregation(Map.empty[String, Expression], Map("set" -> collectDistinct(prop("a", "prop"))))
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("should rewrite collect non-distinct when multiple IN") {
    val before = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("'foo' AS result")
      .filter("'foo' IN set")
      .filter("'foo' IN set")
      .aggregation(Seq.empty, Seq("collect(a.prop) AS set"))
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("result")
      .projection("'foo' AS result")
      .filter("'foo' IN set")
      .filter("'foo' IN set")
      .aggregation(Map.empty[String, Expression], Map("set" -> collectDistinct(prop("a", "prop"))))
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("should rewrite collect distinct for ordered aggregations") {
    val before = new LogicalPlanBuilder()
      .produceResults("set")
      .orderedAggregation(Seq("a AS a"), Seq("collect(distinct a.prop) AS set"), Seq("a"))
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("set")
      .orderedAggregation(Map("a" -> varFor("a")), Map("set" -> collectDistinct(prop("a", "prop"))), Seq("a"))
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("should not rewrite collect non-distinct when IN if there are other usages") {
    val before = new LogicalPlanBuilder()
      .produceResults("anotherSet")
      .projection("set AS anotherSet")
      .filter("'foo' IN set")
      .aggregation(Seq.empty, Seq("collect(a.prop) AS set"))
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("should not rewrite collect when not distinct") {
    val before = new LogicalPlanBuilder()
      .produceResults("set")
      .aggregation(Seq.empty, Seq("collect(a.prop) AS set"))
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("should not rewrite if random access") {
    val before = new LogicalPlanBuilder()
      .produceResults("third")
      .projection("set[3] AS third")
      .aggregation(Seq.empty, Seq("collect(distinct a.prop) AS set"))
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("should rewrite if accessing head") {
    val before = new LogicalPlanBuilder()
      .produceResults("head")
      .projection("head(set) AS head")
      .aggregation(Seq.empty, Seq("collect(distinct a.prop) AS set"))
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("head")
      .projection("head(set) AS head")
      .aggregation(Map.empty[String, Expression], Map("set" -> collectDistinct(prop("a", "prop"))))
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("should not rewrite if accessing last") {
    val before = new LogicalPlanBuilder()
      .produceResults("last")
      .projection("last(set) AS last")
      .aggregation(Seq.empty, Seq("collect(distinct a.prop) AS set"))
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("last")
      .projection("last(set) AS last")
      .aggregation(Map.empty[String, Expression], Map("set" -> collectDistinct(prop("a", "prop"))))
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("should just not rewrite the list that has random access") {
    val before = new LogicalPlanBuilder()
      .produceResults("third", "set2")
      .projection("set1[3] AS third", "set2 AS set2")
      .aggregation(Seq.empty, Seq("collect(distinct a.prop1) AS set1", "collect(distinct a.prop2) AS set2"))
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("third", "set2")
      .projection("set1[3] AS third", "set2 AS set2")
      .aggregation(
        Map.empty[String, Expression],
        Map(
          "set1" -> distinctFunction("collect", prop("a", "prop1")),
          "set2" -> collectDistinct(prop("a", "prop2"))
        )
      )
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("should not rewrite if random access after being aliased") {
    val before = new LogicalPlanBuilder()
      .produceResults("third")
      .projection("sneaky[3] AS third")
      .projection("set AS sneaky")
      .aggregation(Seq.empty, Seq("collect(distinct a.prop) AS set"))
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("should not rewrite if random access after being aliased by distinct") {
    val before = new LogicalPlanBuilder()
      .produceResults("third")
      .projection("sneaky[3] AS third")
      .distinct("set AS sneaky")
      .aggregation(Seq.empty, Seq("collect(distinct a.prop) AS set"))
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("should not rewrite if random access after being aliased multiple times") {
    val before = new LogicalPlanBuilder()
      .produceResults("third")
      .projection("sneaky3[3] AS third")
      .projection("sneaky2 AS sneaky3")
      .unwind("[1,2] AS i")
      .projection("sneaky1 AS sneaky2")
      .projection("set AS sneaky1")
      .aggregation(Seq.empty, Seq("collect(distinct a.prop) AS set"))
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  test("should preserve order") {
    val before = new LogicalPlanBuilder()
      .produceResults("set")
      .aggregation(
        Map.empty[String, Expression],
        Map("set" -> distinctFunction("collect", order = ArgumentAsc, prop("a", "prop")))
      )
      .nodeIndexOperator("a:L(prop)", indexOrder = IndexOrderAscending)
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("set")
      .aggregation(Map.empty[String, Expression], Map("set" -> collectDistinct(prop("a", "prop"), ordered = true)))
      .nodeIndexOperator("a:L(prop)", indexOrder = IndexOrderAscending)
      .build()

    rewrite(before) should equal(after)
  }

  test("should rewrite collect distinct ids") {
    val before = new LogicalPlanBuilder()
      .produceResults("set")
      .aggregation(Seq.empty, Seq("collect(distinct id(a)) AS set"))
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("set")
      .aggregation(Map.empty[String, Expression], Map("set" -> collectDistinctIds(id(varFor("a")))))
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("should not rewrite collect ids without distinct") {
    val plan = new LogicalPlanBuilder()
      .produceResults("set")
      .aggregation(Seq.empty, Seq("collect(id(a)) AS set"))
      .allNodeScan("a")
      .build()

    assertNotRewritten(plan)
  }

  test("should not rewrite collect distinct element ids") {
    val before = new LogicalPlanBuilder()
      .produceResults("set")
      .aggregation(Seq.empty, Seq("collect(distinct elementId(a)) AS set"))
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("set")
      .aggregation(Map.empty[String, Expression], Map("set" -> collectDistinct(elementId(varFor("a")))))
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("should not rewrite collect ids when ordered") {
    val before = new LogicalPlanBuilder()
      .produceResults("set")
      .aggregation(
        Map.empty[String, Expression],
        Map("set" -> distinctFunction("collect", order = ArgumentAsc, id(varFor("a"))))
      )
      .nodeIndexOperator("a:L(prop)", indexOrder = IndexOrderAscending)
      .build()

    val after = new LogicalPlanBuilder()
      .produceResults("set")
      .aggregation(Map.empty[String, Expression], Map("set" -> collectDistinct(id(varFor("a")), ordered = true)))
      .nodeIndexOperator("a:L(prop)", indexOrder = IndexOrderAscending)
      .build()

    rewrite(before) should equal(after)
  }

  test("should not rewrite collect ids if random access") {
    val before = new LogicalPlanBuilder()
      .produceResults("third")
      .projection("set[3] AS third")
      .aggregation(Seq.empty, Seq("collect(distinct id(a)) AS set"))
      .allNodeScan("a")
      .build()

    assertNotRewritten(before)
  }

  private def assertNotRewritten(p: LogicalPlan): Unit = {
    rewrite(p) should equal(p)
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    p.endoRewrite(collectDistinctRewriter)
}
