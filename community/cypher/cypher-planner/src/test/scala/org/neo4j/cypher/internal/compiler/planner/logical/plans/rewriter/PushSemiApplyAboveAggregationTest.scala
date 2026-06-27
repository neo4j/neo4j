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
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.attribution.Attributes

class PushSemiApplyAboveAggregationTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport {

  test("pushes AntiSemiApply above an aggregation that groups by the apply's dependencies") {
    // MATCH (p1)-[:KNOWS]-(mid)-[:KNOWS]-(p2) WHERE NOT EXISTS { (p1)-[:KNOWS]-(p2) }
    // RETURN p1, p2, count(DISTINCT mid) AS cnt   -- grouping by the nodes p1, p2
    val input = new LogicalPlanBuilder()
      .produceResults("person1", "person2", "cnt")
      .aggregation(Seq("person1 AS person1", "person2 AS person2"), Seq("count(DISTINCT mid) AS cnt"))
      .antiSemiApply()
      .|.expandInto("(person1)-[:KNOWS]-(person2)")
      .|.argument("person1", "person2")
      .expand("(mid)-[:KNOWS]-(person2)")
      .expand("(person1)-[:KNOWS]-(mid)")
      .nodeByLabelScan("person1", "Person")
      .build()

    val expected = new LogicalPlanBuilder()
      .produceResults("person1", "person2", "cnt")
      .antiSemiApply()
      .|.expandInto("(person1)-[:KNOWS]-(person2)")
      .|.argument("person1", "person2")
      .aggregation(Seq("person1 AS person1", "person2 AS person2"), Seq("count(DISTINCT mid) AS cnt"))
      .expand("(mid)-[:KNOWS]-(person2)")
      .expand("(person1)-[:KNOWS]-(mid)")
      .nodeByLabelScan("person1", "Person")
      .build()

    rewrite(input) should equal(expected)
  }

  test("pushes AntiSemiApply above an OrderedAggregation, carrying over the leveraged order") {
    // Same as above but the aggregation streams on an already-ordered input. The anti-join preserves
    // its left-hand side's order, so the leveraged order is the same above or below it.
    val input = new LogicalPlanBuilder()
      .produceResults("person1", "person2", "cnt")
      .orderedAggregation(
        Seq("person1 AS person1", "person2 AS person2"),
        Seq("count(DISTINCT mid) AS cnt"),
        Seq("person1")
      )
      .antiSemiApply()
      .|.expandInto("(person1)-[:KNOWS]-(person2)")
      .|.argument("person1", "person2")
      .expand("(mid)-[:KNOWS]-(person2)")
      .expand("(person1)-[:KNOWS]-(mid)")
      .nodeByLabelScan("person1", "Person")
      .build()

    val expected = new LogicalPlanBuilder()
      .produceResults("person1", "person2", "cnt")
      .antiSemiApply()
      .|.expandInto("(person1)-[:KNOWS]-(person2)")
      .|.argument("person1", "person2")
      .orderedAggregation(
        Seq("person1 AS person1", "person2 AS person2"),
        Seq("count(DISTINCT mid) AS cnt"),
        Seq("person1")
      )
      .expand("(mid)-[:KNOWS]-(person2)")
      .expand("(person1)-[:KNOWS]-(mid)")
      .nodeByLabelScan("person1", "Person")
      .build()

    rewrite(input) should equal(expected)
  }

  test("pushes a positive SemiApply above an OrderedAggregation") {
    // EXISTS (rather than NOT EXISTS) lowers to a SemiApply; the rewrite applies equally.
    val input = new LogicalPlanBuilder()
      .produceResults("person1", "person2", "cnt")
      .orderedAggregation(
        Seq("person1 AS person1", "person2 AS person2"),
        Seq("count(DISTINCT mid) AS cnt"),
        Seq("person1")
      )
      .semiApply()
      .|.expandInto("(person1)-[:KNOWS]-(person2)")
      .|.argument("person1", "person2")
      .expand("(mid)-[:KNOWS]-(person2)")
      .expand("(person1)-[:KNOWS]-(mid)")
      .nodeByLabelScan("person1", "Person")
      .build()

    val expected = new LogicalPlanBuilder()
      .produceResults("person1", "person2", "cnt")
      .semiApply()
      .|.expandInto("(person1)-[:KNOWS]-(person2)")
      .|.argument("person1", "person2")
      .orderedAggregation(
        Seq("person1 AS person1", "person2 AS person2"),
        Seq("count(DISTINCT mid) AS cnt"),
        Seq("person1")
      )
      .expand("(mid)-[:KNOWS]-(person2)")
      .expand("(person1)-[:KNOWS]-(mid)")
      .nodeByLabelScan("person1", "Person")
      .build()

    rewrite(input) should equal(expected)
  }

  test("does not fire when the RHS reads a variable that is not a grouping key (soundness guard)") {
    // The anti-join depends on `mid`, which the aggregation collapses — the predicate is
    // NOT constant within a group, so the rewrite must not fire.
    val input = new LogicalPlanBuilder()
      .produceResults("person1", "cnt")
      .aggregation(Seq("person1 AS person1"), Seq("count(DISTINCT mid) AS cnt"))
      .antiSemiApply()
      .|.expandInto("(person1)-[:KNOWS]-(mid)")
      .|.argument("person1", "mid")
      .expand("(person1)-[:KNOWS]-(mid)")
      .nodeByLabelScan("person1", "Person")
      .build()

    rewrite(input) should equal(input)
  }

  test("does not fire when grouping renames the dependency (the p1.id case — needs the FD increment)") {
    // RETURN p1.id AS a, ... groups by a NEW key `a`, so deps(R) = {person1} is not a grouping
    // key. Firing here requires proving p1.id functionally determines p1 (uniqueness constraint).
    val input = new LogicalPlanBuilder()
      .produceResults("a", "cnt")
      .aggregation(Seq("person1.id AS a"), Seq("count(DISTINCT mid) AS cnt"))
      .antiSemiApply()
      .|.expandInto("(person1)-[:KNOWS]-(person2)")
      .|.argument("person1", "person2")
      .expand("(mid)-[:KNOWS]-(person2)")
      .expand("(person1)-[:KNOWS]-(mid)")
      .nodeByLabelScan("person1", "Person")
      .build()

    rewrite(input) should equal(input)
  }

  test("does not fire when the estimated fan-out is below the threshold (cost guard)") {
    val input = antiSemiApplyOverAggregationByNodes()
    // ~1.1 filtered rows per group (110 / 100) is below MinFanOut (2.0): pushing would save almost
    // nothing, so the rewrite must not fire.
    rewrite(input, fanOut(input, semiApplyCard = 110, aggCard = 100)) should equal(input)
  }

  test("fires when the estimated fan-out meets the threshold (cost guard)") {
    val input = antiSemiApplyOverAggregationByNodes()
    val expected = new LogicalPlanBuilder()
      .produceResults("person1", "person2", "cnt")
      .antiSemiApply()
      .|.expandInto("(person1)-[:KNOWS]-(person2)")
      .|.argument("person1", "person2")
      .aggregation(Seq("person1 AS person1", "person2 AS person2"), Seq("count(DISTINCT mid) AS cnt"))
      .expand("(mid)-[:KNOWS]-(person2)")
      .expand("(person1)-[:KNOWS]-(mid)")
      .nodeByLabelScan("person1", "Person")
      .build()
    // 5 filtered rows per group (500 / 100) is above MinFanOut (2.0).
    rewrite(input, fanOut(input, semiApplyCard = 500, aggCard = 100)) should equal(expected)
  }

  private def antiSemiApplyOverAggregationByNodes(): LogicalPlan =
    new LogicalPlanBuilder()
      .produceResults("person1", "person2", "cnt")
      .aggregation(Seq("person1 AS person1", "person2 AS person2"), Seq("count(DISTINCT mid) AS cnt"))
      .antiSemiApply()
      .|.expandInto("(person1)-[:KNOWS]-(person2)")
      .|.argument("person1", "person2")
      .expand("(mid)-[:KNOWS]-(person2)")
      .expand("(person1)-[:KNOWS]-(mid)")
      .nodeByLabelScan("person1", "Person")
      .build()

  private def fanOut(plan: LogicalPlan, semiApplyCard: Double, aggCard: Double): Cardinalities = {
    val cardinalities = new Cardinalities
    cardinalities.set(plan.folder.findAllByClass[AntiSemiApply].head.id, Cardinality(semiApplyCard))
    cardinalities.set(plan.folder.findAllByClass[Aggregation].head.id, Cardinality(aggCard))
    cardinalities
  }

  private def rewrite(p: LogicalPlan, cardinalities: Cardinalities): LogicalPlan = {
    val rewriter = pushSemiApplyAboveAggregation(
      newMockedPlanContext(),
      new StubSolveds,
      cardinalities,
      new StubProvidedOrders,
      Attributes(idGen)
    )
    p.endoRewrite(rewriter)
  }

  private def rewrite(p: LogicalPlan): LogicalPlan = {
    val rewriter = pushSemiApplyAboveAggregation(
      newMockedPlanContext(),
      new StubSolveds,
      new StubCardinalities,
      new StubProvidedOrders,
      Attributes(idGen)
    )
    p.endoRewrite(rewriter)
  }
}
