/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.v4_0.util.attribution.Attributes
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class PushdownPropertyReadsTest extends CypherFunSuite with PlanMatchHelp with LogicalPlanConstructionTestSupport {

  test("should pushdown read in projection") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n.prop AS x").withCardinality(50)
      .expandAll("(n)-->(m)").withCardinality(50)
      .allNodeScan("n").withCardinality(5)

    val rewritten = pushdownPropertyReads.x(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("n.prop AS x")
        .expandAll("(n)-->(m)")
        .cacheProperties("n.prop")
        .allNodeScan("n")
        .build()
  }

  test("should pushdown read in projection, but not too beyond optimum") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n.prop AS x").withCardinality(50)
      .expandAll("(n)-->(m)").withCardinality(50)
      .filter("id(n) <> 0").withCardinality(5)
      .allNodeScan("n").withCardinality(10)

    val rewritten = pushdownPropertyReads.x(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("n.prop AS x")
        .expandAll("(n)-->(m)")
        .cacheProperties("n.prop")
        .filter("id(n) <> 0")
        .allNodeScan("n")
        .build()
  }

  test("should not pushdown past a leaf") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .nodeIndexOperator("n:N(prop > 0)").withCardinality(1)

    val rewritten = pushdownPropertyReads.x(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("x")
        .nodeIndexOperator("n:N(prop > 0)")
        .build()
  }

  test("should pushdown past from RHS to LHS of apply") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .apply().withCardinality(50)
      .|.filter("m. prop > 10").withCardinality(50)
      .|.allNodeScan("n", "m").withCardinality(100)
      .allNodeScan("m").withCardinality(10)

    val rewritten = pushdownPropertyReads.x(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n")
        .apply()
        .|.filter("m. prop > 10")
        .|.allNodeScan("n", "m")
        .cacheProperties("m.prop")
        .allNodeScan("m")
        .build()
  }

  test("should pushdown past from RHS to LHS of apply, from a leaf") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .apply().withCardinality(50)
      .|.nodeIndexOperator("n:N(prop > ???)", paramExpr = Some(prop("m", "prop")), argumentIds = Set("m")).withCardinality(50)
      .allNodeScan("m").withCardinality(10)

    val rewritten = pushdownPropertyReads.x(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n")
        .apply()
        .|.nodeIndexOperator("n:N(prop > ???)", paramExpr = Some(prop("m", "prop")), argumentIds = Set("m"))
        .cacheProperties("m.prop")
        .allNodeScan("m")
        .build()
  }

  test("should pushdown past an apply into LHS") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .filter("m.prop > 10").withCardinality(50)
      .apply().withCardinality(100)
      .|.allNodeScan("n", "m").withCardinality(100)
      .allNodeScan("m").withCardinality(10)

    val rewritten = pushdownPropertyReads.x(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n")
        .filter("m. prop > 10")
        .apply()
        .|.allNodeScan("n", "m")
        .cacheProperties("m.prop")
        .allNodeScan("m")
        .build()
  }

  test("should pushdown past an apply into RHS") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .filter("m.prop > 10").withCardinality(50)
      .apply().withCardinality(2)
      .|.filter("n.prop > 10").withCardinality(2)
      .|.allNodeScan("n", "m").withCardinality(100)
      .allNodeScan("m").withCardinality(10)

    val rewritten = pushdownPropertyReads.x(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n")
        .filter("m. prop > 10")
        .apply()
        .|.cacheProperties("m.prop")
        .|.filter("n.prop > 10")
        .|.allNodeScan("n", "m")
        .allNodeScan("m")
        .build()
  }

  test("should pushdown read in filter") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .filter("n.prop > 0").withCardinality(1)
      .expandAll("(n)-->(m)").withCardinality(50)
      .allNodeScan("n").withCardinality(5)

    val rewritten = pushdownPropertyReads.x(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n")
        .filter("n.prop > 0")
        .expandAll("(n)-->(m)")
        .cacheProperties("n.prop")
        .allNodeScan("n")
        .build()
  }

  test("should pushdown read in projection with multiple properties") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n.prop AS x", "n.foo AS y", "m.bar AS z").withCardinality(50)
      .expandAll("(n)-->(m)").withCardinality(50)
      .filter("id(n) <> 0").withCardinality(5)
      .allNodeScan("n").withCardinality(10)

    val rewritten = pushdownPropertyReads.x(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("n.prop AS x", "n.foo AS y", "m.bar AS z")
        .expandAll("(n)-->(m)")
        .cacheProperties("n.prop", "n.foo")
        .filter("id(n) <> 0")
        .allNodeScan("n")
        .build()
  }

  test("should pushdown read in projection with multiple properties to different points") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n.prop AS x", "n.foo AS y", "m.bar AS z").withCardinality(100)
      .projection("n.prop AS x0").withCardinality(100)
      .expandAll("(m)-->(q)").withCardinality(100)
      .expandAll("(n)-->(m)").withCardinality(50)
      .filter("id(n) <> 0").withCardinality(5)
      .allNodeScan("n").withCardinality(10)

    val rewritten = pushdownPropertyReads.x(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("n.prop AS x", "n.foo AS y", "m.bar AS z")
        .projection("n.prop AS x0")
        .expandAll("(m)-->(q)")
        .cacheProperties("m.bar")
        .expandAll("(n)-->(m)")
        .cacheProperties("n.prop", "n.foo")
        .filter("id(n) <> 0")
        .allNodeScan("n")
        .build()
  }

  test("should pushdown read in aggregation") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .aggregation(Seq("n.prop AS x"), Seq("count(*) AS c")).withCardinality(20)
      .expandAll("(n)-->(m)").withCardinality(50)
      .filter("id(n) <> 0").withCardinality(5)
      .allNodeScan("n").withCardinality(10)

    val rewritten = pushdownPropertyReads.x(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("x")
        .aggregation(Seq("n.prop AS x"), Seq("count(*) AS c"))
        .expandAll("(n)-->(m)")
        .cacheProperties("n.prop")
        .filter("id(n) <> 0")
        .allNodeScan("n")
        .build()
  }

  test("should not pushdown read past aggregation") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n.prop AS x").withCardinality(100)
      .expandAll("(n)-->(q)").withCardinality(100)
      .aggregation(Seq("n AS n"), Seq("count(*) AS c")).withCardinality(10)
      .expandAll("(n)-->(m)").withCardinality(50)
      .filter("id(n) <> 0").withCardinality(5)
      .allNodeScan("n").withCardinality(10)

    val rewritten = pushdownPropertyReads.x(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("n.prop AS x")
        .expandAll("(n)-->(q)")
        .cacheProperties("n.prop")
        .aggregation(Seq("n AS n"), Seq("count(*) AS c"))
        .expandAll("(n)-->(m)")
        .filter("id(n) <> 0")
        .allNodeScan("n")
        .build()
  }
}
