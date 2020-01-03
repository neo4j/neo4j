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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.v4_0.util.attribution.Attributes
import org.neo4j.cypher.internal.v4_0.util.symbols.CTNode
import org.neo4j.cypher.internal.v4_0.util.symbols.CTInteger
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class PushdownPropertyReadsTest extends CypherFunSuite with PlanMatchHelp with LogicalPlanConstructionTestSupport {

  test("should pushdown read in projection") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n.prop AS x").withCardinality(50)
      .expandAll("(n)-->(m)").withCardinality(50)
      .allNodeScan("n").withCardinality(5)

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
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

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
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

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("x")
        .nodeIndexOperator("n:N(prop > 0)")
        .build()
  }

  test("should pushdown from RHS to LHS of apply") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .apply().withCardinality(50)
      .|.filter("m.prop > 10").withCardinality(50)
      .|.allNodeScan("n", "m").withCardinality(100)
      .allNodeScan("m").withCardinality(10)

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n")
        .apply()
        .|.filter("m.prop > 10")
        .|.allNodeScan("n", "m")
        .cacheProperties("m.prop")
        .allNodeScan("m")
        .build()
  }

  test("should not pushdown from the RHS leaf to LHS of apply if cardinalities are the same") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .apply().withCardinality(50)
      .|.nodeIndexOperator("n:N(prop > ???)", paramExpr = Some(prop("m", "prop")), argumentIds = Set("m")).withCardinality(50)
      .allNodeScan("m").withCardinality(10)

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n")
        .apply()
        .|.nodeIndexOperator("n:N(prop > ???)", paramExpr = Some(prop("m", "prop")), argumentIds = Set("m"))
        .allNodeScan("m")
        .build()
  }

  test("should pushdown from the RHS leaf to LHS of apply if beneficial") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .apply().withCardinality(55)
      .|.nodeIndexOperator("n:N(prop > ???)", paramExpr = Some(prop("m", "prop")), argumentIds = Set("m")).withCardinality(55)
      .expandAll("(m)-->(q)").withCardinality(50)
      .allNodeScan("m").withCardinality(10)

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n")
        .apply()
        .|.nodeIndexOperator("n:N(prop > ???)", paramExpr = Some(prop("m", "prop")), argumentIds = Set("m"))
        .expandAll("(m)-->(q)")
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

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n")
        .filter("m.prop > 10")
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
      .apply().withCardinality(20)
      .|.expandAll("(m)-->(q)").withCardinality(20)
      .|.filter("n.prop > 10").withCardinality(2)
      .|.allNodeScan("n", "m").withCardinality(100)
      .allNodeScan("m").withCardinality(10)

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n")
        .filter("m.prop > 10")
        .apply()
        .|.expandAll("(m)-->(q)")
        .|.cacheProperties("m.prop")
        .|.filter("n.prop > 10")
        .|.allNodeScan("n", "m")
        .allNodeScan("m")
        .build()
  }

  test("should not pushdown from RHS to LHS of cartesian product") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .apply().withCardinality(50)
      .|.cartesianProduct().withCardinality(50)
      .|.|.filter("n.prop > 10").withCardinality(5)
      .|.|.argument("n").withCardinality(10)
      .|.expandAll("(n)-->(m)").withCardinality(1)
      .|.argument("n").withCardinality(10)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should pushdown past a cartesian product into LHS") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .filter("m.prop > 10").withCardinality(50)
      .cartesianProduct().withCardinality(100)
      .|.allNodeScan("n").withCardinality(100)
      .allNodeScan("m").withCardinality(10)

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n")
        .filter("m.prop > 10")
        .cartesianProduct()
        .|.allNodeScan("n")
        .cacheProperties("m.prop")
        .allNodeScan("m")
        .build()
  }

  test("should pushdown past a cartesian product into RHS") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .filter("n.prop > 10").withCardinality(50)
      .cartesianProduct().withCardinality(20)
      .|.expandAll("(n)-->(q)").withCardinality(20)
      .|.filter("1 <> 2").withCardinality(2)
      .|.allNodeScan("n").withCardinality(100)
      .allNodeScan("m").withCardinality(10)

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n")
        .filter("n.prop > 10")
        .cartesianProduct()
        .|.expandAll("(n)-->(q)")
        .|.cacheProperties("n.prop")
        .|.filter("1 <> 2")
        .|.allNodeScan("n")
        .allNodeScan("m")
        .build()
  }

  test("should not pushdown from RHS to LHS of join") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .apply().withCardinality(50)
      .|.nodeHashJoin("n").withCardinality(50)
      .|.|.filter("n.prop > 10").withCardinality(5)
      .|.|.argument("n").withCardinality(10)
      .|.expandAll("(n)-->(m)").withCardinality(1)
      .|.argument("n").withCardinality(10)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should pushdown from both RHS and LHS of join into LHS of apply") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .apply().withCardinality(5)
      .|.nodeHashJoin("n").withCardinality(5)
      .|.|.filter("n.prop > 10").withCardinality(5)
      .|.|.argument("n").withCardinality(100)
      .|.filter("n.foo > 10 AND n.prop < 20").withCardinality(1)
      .|.argument("n").withCardinality(100)
      .expandAll("(n)-->(m)").withCardinality(100)
      .allNodeScan("n").withCardinality(1)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe new LogicalPlanBuilder()
      .produceResults("n")
      .apply()
      .|.nodeHashJoin("n")
      .|.|.filter("n.prop > 10")
      .|.|.argument("n")
      .|.filter("n.foo > 10 AND n.prop < 20")
      .|.argument("n")
      .expandAll("(n)-->(m)")
      .cacheProperties("n.prop", "n.foo")
      .allNodeScan("n")
      .build()
  }

  test("should pushdown past a join into LHS and RHS") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .projection("n.prop AS x", "m.prop AS y").withCardinality(100)
      .nodeHashJoin("n").withCardinality(100)
      .|.expandAll("(m)-->(n)").withCardinality(50)
      .|.allNodeScan("m").withCardinality(10)
      .allNodeScan("n").withCardinality(10)

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n")
        .projection("n.prop AS x", "m.prop AS y")
        .nodeHashJoin("n")
        .|.expandAll("(m)-->(n)")
        .|.cacheProperties("m.prop")
        .|.allNodeScan("m")
        .cacheProperties("n.prop")
        .allNodeScan("n")
        .build()
  }

  test("should not pushdown past a join into LHS if already cached in RHS") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .projection("n.prop AS x").withCardinality(100)
      .nodeHashJoin("n").withCardinality(100)
      .|.filter("n.prop <> 0").withCardinality(50)
      .|.expandAll("(m)-->(n)").withCardinality(50)
      .|.allNodeScan("m").withCardinality(10)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should not pushdown past a join into RHS if already cached in LHS") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .projection("n.prop AS x").withCardinality(100)
      .nodeHashJoin("n").withCardinality(100)
      .|.expandAll("(m)-->(n)").withCardinality(50)
      .|.allNodeScan("m").withCardinality(10)
      .filter("n.prop <> 0").withCardinality(100)
      .allNodeScan("n").withCardinality(100)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should not pushdown if already cached") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .projection("n.prop AS x").withCardinality(100)
      .expandAll("(m)-->(q)").withCardinality(100)
      .expandAll("(n)-->(m)").withCardinality(1)
      .filter("n.prop <> 0").withCardinality(100)
      .allNodeScan("n").withCardinality(100)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should pushdown to aggregation even if already cached before aggregation") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .projection("m.prop AS x").withCardinality(100)
      .expandAll("(m)-->(q)").withCardinality(100)
      .aggregation(Seq("m AS m"), Seq("count(*) AS c")).withCardinality(1)
      .filter("m.prop < 5000").withCardinality(250)
      .expandAll("(m)-->(r)").withCardinality(500)
      .allNodeScan("m").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe new LogicalPlanBuilder()
      .produceResults("n")
      .projection("m.prop AS x")
      .expandAll("(m)-->(q)")
      .cacheProperties("m.prop")
      .aggregation(Seq("m AS m"), Seq("count(*) AS c"))
      .filter("m.prop < 5000")
      .expandAll("(m)-->(r)")
      .cacheProperties("m.prop")
      .allNodeScan("m")
      .build()
  }

  test("should pushdown read in filter") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n")
      .filter("n.prop > 0").withCardinality(1)
      .expandAll("(n)-->(m)").withCardinality(50)
      .allNodeScan("n").withCardinality(5)

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
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

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
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

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
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

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
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

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
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

  test("should not pushdown read past ordered aggregation") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n.prop AS x").withCardinality(100)
      .expandAll("(n)-->(q)").withCardinality(100)
      .orderedAggregation(Seq("n AS n"), Seq("count(*) AS c"), Seq(varFor("n"))).withCardinality(10)
      .expandAll("(n)-->(m)").withCardinality(50)
      .filter("id(n) <> 0").withCardinality(5)
      .allNodeScan("n").withCardinality(10)

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("n.prop AS x")
        .expandAll("(n)-->(q)")
        .cacheProperties("n.prop")
        .orderedAggregation(Seq("n AS n"), Seq("count(*) AS c"), Seq(varFor("n")))
        .expandAll("(n)-->(m)")
        .filter("id(n) <> 0")
        .allNodeScan("n")
        .build()
  }

  test("should ignore expression variable property reads (for now)") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("list")
      .projection("[x IN [n] | x.prop] AS list").withCardinality(5).newVar("x", CTNode)
      .allNodeScan("n").withCardinality(5)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should not pushdown read past union into LHS") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n.prop AS x").withCardinality(100)
      .union().withCardinality(100)
      .|.nodeByLabelScan("n", "A").withCardinality(90)
      .nodeByLabelScan("n", "B").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should not pushdown read past union into RHS") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n.prop AS x").withCardinality(100)
      .union().withCardinality(100)
      .|.nodeByLabelScan("n", "A").withCardinality(10)
      .nodeByLabelScan("n", "B").withCardinality(90)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should not pushdown read if property is available from index") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .filter("n.prop == 'NOT-IMPORTANT'").withCardinality(100)
      .expand("(n)-->(m)").withCardinality(235)
      .nodeIndexOperator("n:L(prop > 100)", getValue = CanGetValue).withCardinality(90)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should pushdown read through renaming projection") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .filter("mysteryNode.prop == 'NOT-IMPORTANT'").withCardinality(100)
      .projection("n AS mysteryNode").withCardinality(235).newVar("mysteryNode", CTNode)
      .expand("(n)-->(m)").withCardinality(235)
      .allNodeScan("n").withCardinality(90)

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n", "m")
        .filter("mysteryNode.prop == 'NOT-IMPORTANT'")
        .projection("n AS mysteryNode")
        .expand("(n)-->(m)")
        .cacheProperties("n.prop")
        .allNodeScan("n")
        .build()
  }

  test("should pushdown read through 2 renaming projections") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .filter("unknownNode.prop == 'NOT-IMPORTANT'").withCardinality(100)
      .projection("mysteryNode AS unknownNode").withCardinality(235).newVar("unknownNode", CTNode)
      .projection("n AS mysteryNode").withCardinality(235).newVar("mysteryNode", CTNode)
      .expand("(n)-->(m)").withCardinality(235)
      .allNodeScan("n").withCardinality(90)

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n", "m")
        .filter("unknownNode.prop == 'NOT-IMPORTANT'")
        .projection("mysteryNode AS unknownNode").newVar("unknownNode", CTNode)
        .projection("n AS mysteryNode").newVar("mysteryNode", CTNode)
        .expand("(n)-->(m)")
        .cacheProperties("n.prop")
        .allNodeScan("n")
        .build()
  }

  test("should pushdown read through unrelated renaming projection") {
    val plan = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .filter("n.prop == 'NOT-IMPORTANT'").withCardinality(100)
      .projection("m AS mysteryNode").withCardinality(235).newVar("mysteryNode", CTNode)
      .expand("(n)-->(m)").withCardinality(235)
      .allNodeScan("n").withCardinality(90)

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("n", "m")
        .filter("n.prop == 'NOT-IMPORTANT'")
        .projection("m AS mysteryNode")
        .expand("(n)-->(m)")
        .cacheProperties("n.prop")
        .allNodeScan("n")
        .build()
  }

  test("should not pushdown read if property is available, but renamed") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .filter("mysteryNode.prop == 'NOT-IMPORTANT'").withCardinality(100)
      .projection("n AS mysteryNode").withCardinality(235).newVar("mysteryNode", CTNode)
      .expand("(n)-->(m)").withCardinality(235)
      .nodeIndexOperator("n:L(prop > 100)", getValue = CanGetValue).withCardinality(90)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should ignore unrelated renaming") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .projection("x AS mysteryVariable").withCardinality(5).newVar("mysteryVariable", CTInteger)
      .projection("1 AS x").withCardinality(5).newVar("x", CTInteger)
      .allNodeScan("n").withCardinality(5)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should not pushdown if setProperty") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .filter("n.prop == 'NOT-IMPORTANT'").withCardinality(100)
      .setProperty("n", "prop", "42").withCardinality(100)
      .expand("(n)-->(m)").withCardinality(100)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should not pushdown if setNodeProperty") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .filter("n.prop == 'NOT-IMPORTANT'").withCardinality(100)
      .setNodeProperty("n", "prop", "42").withCardinality(100)
      .expand("(n)-->(m)").withCardinality(100)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should not pushdown if setRelationshipProperty") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .filter("n.prop == 'NOT-IMPORTANT'").withCardinality(100)
      .setRelationshipProperty("n", "prop", "42").withCardinality(100)
      .expand("(n)-->(m)").withCardinality(100)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should not pushdown if setNodePropertiesFromMap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .filter("n.prop == 'NOT-IMPORTANT'").withCardinality(100)
      .setNodePropertiesFromMap("n", "{prop: 42}", removeOtherProps = false).withCardinality(100)
      .expand("(n)-->(m)").withCardinality(100)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should not pushdown if setRelationshipPropertiesFromMap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .filter("r.prop == 'NOT-IMPORTANT'").withCardinality(100)
      .setRelationshipPropertiesFromMap("r", "{prop: 42}", removeOtherProps = false).withCardinality(100)
      .expand("(m)-->(k)").withCardinality(100)
      .expand("(n)-[r]->(m)").withCardinality(20)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should not pushdown if property removed by setNodePropertiesFromMap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .filter("n.prop == 'NOT-IMPORTANT'").withCardinality(100)
      .setNodePropertiesFromMap("n", "{otherProp: 42}", removeOtherProps = true).withCardinality(100)
      .expand("(n)-->(m)").withCardinality(100)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should not pushdown if property removed by setRelationshipPropertiesFromMap") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .filter("r.prop == 'NOT-IMPORTANT'").withCardinality(100)
      .setRelationshipPropertiesFromMap("r", "{otherProp: 42}", removeOtherProps = true).withCardinality(100)
      .expand("(m)-->(k)").withCardinality(100)
      .expand("(n)-[r]->(m)").withCardinality(20)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should not pushdown if property setNodePropertiesFromMap with dynamic map") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .filter("n.prop == 'NOT-IMPORTANT'").withCardinality(100)
      .setNodePropertiesFromMap("n", "m", removeOtherProps = false).withCardinality(100)
      .expand("(n)-->(m)").withCardinality(100)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should not pushdown if property setRelationshipPropertiesFromMap with dynamic map") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("r")
      .filter("r.prop == 'NOT-IMPORTANT'").withCardinality(100)
      .setRelationshipPropertiesFromMap("r", "r2", removeOtherProps = false).withCardinality(100)
      .expand("(m)-[r2]->(k)").withCardinality(100)
      .expand("(n)-[r]->(m)").withCardinality(20)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  test("should ignore setProperty of non-variable when projecting") {
    val planBuilder = new LogicalPlanBuilder()
      .produceResults("aProp", "bProp")
      .projection("a.prop AS aProp", "b.prop AS bProp").withCardinality(10)
      .setProperty("CASE WHEN n.age>m.age THEN n ELSE m END", "prop", "42").withCardinality(10)
      .expand("(n)-->(m)").withCardinality(10)
      .allNodeScan("n").withCardinality(10)

    val plan = planBuilder.build()
    val rewritten = PushdownPropertyReads.pushdown(plan, planBuilder.cardinalities, Attributes(planBuilder.idGen, planBuilder.cardinalities), planBuilder.getSemanticTable)
    rewritten shouldBe plan
  }

  // This is a defensive measure, since push down property reads are performed after eagerness analysis (which inserts
  // eager operators to guarantee semantic correctness of read-write queries where we have data dependencies between reads and writes within the query).
  // If we move reads past eager boundaries we _may_ end up breaking correctness in edge-cases.
  test("should not pushdown read past eager") {
    val plan = new LogicalPlanBuilder()
      .produceResults("x")
      .projection("n.prop AS x").withCardinality(100)
      .expandAll("(n)-->(q)").withCardinality(100)
      .eager().withCardinality(10)
      .expandAll("(n)-->(m)").withCardinality(50)
      .filter("id(n) <> 0").withCardinality(5)
      .allNodeScan("n").withCardinality(10)

    val rewritten = PushdownPropertyReads.pushdown(plan.build(), plan.cardinalities, Attributes(plan.idGen, plan.cardinalities), plan.getSemanticTable)
    rewritten shouldBe
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("n.prop AS x")
        .expandAll("(n)-->(q)")
        .cacheProperties("n.prop")
        .eager()
        .expandAll("(n)-->(m)")
        .filter("id(n) <> 0")
        .allNodeScan("n")
        .build()
  }
}
