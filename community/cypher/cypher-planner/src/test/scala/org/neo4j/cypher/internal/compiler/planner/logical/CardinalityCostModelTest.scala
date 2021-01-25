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

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.ExecutionModel.Batched
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.DEFAULT_COST_PER_ROW
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.LABEL_CHECK_DB_HITS
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.PROPERTY_ACCESS_DB_HITS
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.CostPerRow
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CardinalityCostModelTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val SMALL_CHUNK_SIZE = 128
  private val BIG_CHUNK_SIZE = 1024

  private def costFor(plan: LogicalPlan,
                      input: QueryGraphSolverInput,
                      semanticTable: SemanticTable,
                      cardinalities: Cardinalities,
                      providedOrders: ProvidedOrders,
                      executionModel: ExecutionModel = ExecutionModel.default): Cost = {
    CardinalityCostModel(executionModel).costFor(plan, input, semanticTable, cardinalities, providedOrders, CostModelMonitor.DEFAULT)
  }

  test("expand should only be counted once") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .filterExpression(hasLabels("a", "Awesome")).withCardinality(10)
      .expand("(a)-[r1]->(b)").withCardinality(100)
      .filterExpression(hasLabels("a", "Awesome")).withCardinality(10)
      .expand("(a)-[r1]->(b)").withCardinality(100)
      .argument("a").withCardinality(10)
      .build()

    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders) should equal(Cost(231))
  }

  test("multiple property expressions are counted for in cost") {
    val cardinality = 10.0
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .filter("a.prop1 = 42", "a.prop1 = 43", "a.prop1 = 44").withCardinality(cardinality)
      .argument("a").withCardinality(cardinality).newVar("a", CTNode)
      .build()

    val numberOfPredicates = 3
    val costForSelection = cardinality * numberOfPredicates * PROPERTY_ACCESS_DB_HITS
    val costForArgument = cardinality * DEFAULT_COST_PER_ROW.cost
    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders) should equal(Cost(costForSelection + costForArgument))
  }

  test("deeply nested property access does not increase cost") {
    val cardinality = 10.0

    val shallowPlanBuilder = new LogicalPlanBuilder(wholePlan = false)
    val shallowPlan = shallowPlanBuilder
      .filter("a.prop1 = 42").withCardinality(cardinality)
      .argument("a").withCardinality(cardinality).newVar("a", CTNode)
      .build()
    val deepPlanBuilder = new LogicalPlanBuilder(wholePlan = false)
    val deepPlan = deepPlanBuilder
      .filter("a.foo.bar.baz.blob.boing.peng.brrt = 2").withCardinality(cardinality)
      .argument("a").withCardinality(cardinality).newVar("a", CTNode)
      .build()

    costFor(shallowPlan, QueryGraphSolverInput.empty, shallowPlanBuilder.getSemanticTable, shallowPlanBuilder.cardinalities, shallowPlanBuilder.providedOrders) should
      equal(costFor(deepPlan, QueryGraphSolverInput.empty, deepPlanBuilder.getSemanticTable, deepPlanBuilder.cardinalities, deepPlanBuilder.providedOrders))
  }

  test("limit should retain its cardinality") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .limit(10).withCardinality(10)
      .allNodeScan("n").withCardinality(100)
      .build()

    val withoutLimit = QueryGraphSolverInput.empty

    val costLimit = Cost(DEFAULT_COST_PER_ROW.cost * 10)
    val costTot = costLimit + Cost(10 * 1.2)

    costFor(plan, withoutLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders) shouldBe costTot
  }

  test("lazy plans should be cheaper when limit selectivity is < 1.0") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .filter("n:Label").withCardinality(47)
      .allNodeScan("n").withCardinality(123)
      .build()

    val withoutLimit = QueryGraphSolverInput.empty
    val withLimit = QueryGraphSolverInput.empty.withLimitSelectivity(Selectivity.of(0.5).get)

    costFor(plan, withLimit, builder.getSemanticTable, builder.cardinalities, new StubProvidedOrders) should be < costFor(plan, withoutLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders)
  }

  test("hash join should be cheaper when limit selectivity is < 1.0") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .nodeHashJoin("b").withCardinality(56)
      .|.expandAll("(c)-[:REL]->(b)").withCardinality(78)
      .|.nodeByLabelScan("c", "C").withCardinality(321)
      .filter("b:B").withCardinality(77)
      .expandAll("(a)-[:REL]->(b)").withCardinality(42)
      .nodeByLabelScan("a", "A").withCardinality(123)
      .build()

    val withoutLimit = QueryGraphSolverInput.empty
    val withLimit = QueryGraphSolverInput.empty.withLimitSelectivity(Selectivity.of(0.5).get)

    costFor(plan, withLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders) should be < costFor(plan, withoutLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders)
  }

  test("eager plans should cost the same regardless of limit selectivity") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .aggregation(Seq("n.prop AS x"), Seq("max(n.val) AS y"))
      .allNodeScan("n").withCardinality(123)
      .build()

    val withoutLimit = QueryGraphSolverInput.empty
    val withLimit = QueryGraphSolverInput.empty.withLimitSelectivity(Selectivity.of(0.5).get)

    costFor(plan, withLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders) should equal(costFor(plan, withoutLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders))
  }

  test("cartesian product with 1 row from the left is equally expensive in both execution models") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(100)
      .|.argument("b").withCardinality(100)
      .argument("a").withCardinality(1)
      .build()

    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, Volcano) should equal(
      costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, Batched(SMALL_CHUNK_SIZE, BIG_CHUNK_SIZE))
    )
  }

  test("cartesian product with many row from the left is cheaper in PushBatchedExecution, big chunk size") {
    val cardinality = 1500.0
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(cardinality * cardinality) // 2250000 > BIG_CHUNK_SIZE, so big chunk size should be picked
      .|.argument("b").withCardinality(cardinality)
      .argument("a").withCardinality(cardinality)
      .build()

    val argCost = Cardinality(cardinality) * DEFAULT_COST_PER_ROW

    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, Volcano)should
      equal(argCost + argCost * cardinality)
    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, Batched(SMALL_CHUNK_SIZE, BIG_CHUNK_SIZE)) should
      equal(argCost + argCost * Math.ceil(cardinality / BIG_CHUNK_SIZE))
  }

  test("cartesian product with many row from the left is cheaper in PushBatchedExecution, small chunk size") {
    val cardinalityLeft = 200.0
    val cardinalityRight = 2.0
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(cardinalityLeft * cardinalityRight) // 400 < BIG_CHUNK_SIZE, so small batch size should be picked
      .|.argument("b").withCardinality(cardinalityRight)
      .argument("a").withCardinality(cardinalityLeft)
      .build()

    val argLeftCost = Cardinality(cardinalityLeft) * DEFAULT_COST_PER_ROW
    val argRightCost = Cardinality(cardinalityRight) * DEFAULT_COST_PER_ROW

    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, Volcano) should
      equal(argLeftCost + argRightCost * cardinalityLeft)
    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, Batched(SMALL_CHUNK_SIZE, BIG_CHUNK_SIZE)) should
      equal(argLeftCost + argRightCost * Math.ceil(cardinalityLeft / SMALL_CHUNK_SIZE))
  }

  test("should pick big chunk size if a plan below the cartesian product has a higher cardinality than big chunk size") {
    val cardinalityLeaves = 1500.0
    val cardinalityCP = 500.0
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(cardinalityCP) // This does not make sense mathematically, but that is OK for this test
      .|.argument("b").withCardinality(cardinalityLeaves)
      .argument("a").withCardinality(cardinalityLeaves)
      .build()

    val argCost = Cardinality(cardinalityLeaves) * DEFAULT_COST_PER_ROW

    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, Batched(SMALL_CHUNK_SIZE, BIG_CHUNK_SIZE)) should
      equal(argCost + argCost * Math.ceil(cardinalityLeaves / BIG_CHUNK_SIZE))
  }

  test("should pick big chunk size if a plan above the cartesian product has a higher cardinality than big chunk size") {
    val cardinalityLater = 1500.0
    val cardinalityEarlier = 500.0
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .unwind("[1, 2, 3] AS x").withCardinality(cardinalityLater)
      .cartesianProduct().withCardinality(cardinalityEarlier) // This does not make sense mathematically, but that is OK for this test
      .|.argument("b").withCardinality(cardinalityEarlier)
      .argument("a").withCardinality(cardinalityEarlier)
      .build()

    val argCost = Cardinality(cardinalityEarlier) * DEFAULT_COST_PER_ROW
    val unwindCost = Cardinality(cardinalityEarlier) * DEFAULT_COST_PER_ROW // cost of unwind is determined on the amount of incoming rows

    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, Batched(SMALL_CHUNK_SIZE, BIG_CHUNK_SIZE)) should
      equal(unwindCost + argCost + argCost * Math.ceil(cardinalityEarlier / BIG_CHUNK_SIZE))
  }


  test("cartesian product with many row from the left but with provided order is equally expensive in both execution models") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(10000).withProvidedOrder(ProvidedOrder.asc(varFor("a")))
      .|.argument("b").withCardinality(100)
      .argument("a").withCardinality(100).withProvidedOrder(ProvidedOrder.asc(varFor("a")))
      .build()

    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, Volcano) should equal(
      costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, Batched(SMALL_CHUNK_SIZE, BIG_CHUNK_SIZE))
    )
  }

  test("should count cost for different label checks") {
    val cardinality = 100.0
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .filterExpression(hasLabels("n", "N"), hasTypes("r", "R"), hasLabelsOrTypes("x", "X"))
      .argument("n", "r", "x").withCardinality(cardinality)
      .build()

    val expectedCost = Cardinality(cardinality) * (DEFAULT_COST_PER_ROW + CostPerRow(LABEL_CHECK_DB_HITS) * 3)

    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders) shouldBe expectedCost
  }

  test("sort should cost the same regardless of limit selectivity") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .sort(Seq(Ascending("n"))).withCardinality(100)
      .allNodeScan("n").withCardinality(100)
      .build()
    val semanticTable = SemanticTable().addNode(varFor("n"))
    val withoutLimit = QueryGraphSolverInput.empty
    val withLimit = QueryGraphSolverInput.empty.withLimitSelectivity(Selectivity.of(0.5).get)
    val unlimited = costFor(plan, withLimit, semanticTable, builder.cardinalities, builder.providedOrders)
    val limited = costFor(plan, withoutLimit, semanticTable, builder.cardinalities, builder.providedOrders)
    unlimited should equal(limited)
  }
}
