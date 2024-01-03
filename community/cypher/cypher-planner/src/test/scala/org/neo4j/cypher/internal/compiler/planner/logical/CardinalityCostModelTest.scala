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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.ExecutionModel.BatchedSingleThreaded
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.HardcodedGraphStatistics
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.ALL_SCAN_COST_PER_ROW
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.DEFAULT_COST_PER_ROW
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.EXPAND_ALL_COST
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.INDEX_SCAN_COST_PER_ROW
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.LABEL_CHECK_DB_HITS
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.PROPERTY_ACCESS_DB_HITS
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.SHORTEST_PRODUCT_GRAPH_COST
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.limit.LimitSelectivityConfig
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.CostPerRow
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.WorkReduction
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion

class CardinalityCostModelTest extends CypherFunSuite with AstConstructionTestSupport
    with LogicalPlanConstructionTestSupport {

  private val SMALL_CHUNK_SIZE = 128
  private val BIG_CHUNK_SIZE = 1024

  private def costFor(
    plan: LogicalPlan,
    input: QueryGraphSolverInput,
    semanticTable: SemanticTable,
    cardinalities: Cardinalities,
    providedOrders: ProvidedOrders,
    executionModel: ExecutionModel = ExecutionModel.default,
    statistics: GraphStatistics = HardcodedGraphStatistics,
    propertyAccess: Set[PropertyAccess] = Set.empty
  ): Cost = {
    CardinalityCostModel(executionModel).costFor(
      plan,
      input,
      semanticTable,
      cardinalities,
      providedOrders,
      propertyAccess,
      statistics,
      CostModelMonitor.DEFAULT
    )
  }

  test("expand should only be counted once") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .filter("a:Awesome").withCardinality(10)
      .expand("(a)-[r1]->(b)").withCardinality(100)
      .filter("a:Awesome").withCardinality(10)
      .expand("(a)-[r1]->(b)").withCardinality(100)
      .argument("a").withCardinality(10)
      .build()

    costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders
    ) should equal(Cost(231))
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
    costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders
    ) should equal(Cost(costForSelection + costForArgument))
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

    costFor(
      shallowPlan,
      QueryGraphSolverInput.empty,
      shallowPlanBuilder.getSemanticTable,
      shallowPlanBuilder.cardinalities,
      shallowPlanBuilder.providedOrders
    ) should
      equal(costFor(
        deepPlan,
        QueryGraphSolverInput.empty,
        deepPlanBuilder.getSemanticTable,
        deepPlanBuilder.cardinalities,
        deepPlanBuilder.providedOrders
      ))
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

    costFor(
      plan,
      withoutLimit,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders
    ) shouldBe costTot
  }

  test("lazy plans should be cheaper when limit selectivity is < 1.0") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .filter("n:Label").withCardinality(47)
      .allNodeScan("n").withCardinality(123)
      .build()

    val withoutLimit = QueryGraphSolverInput.empty
    val withLimit = QueryGraphSolverInput.empty.withLimitSelectivityConfig(LimitSelectivityConfig(
      Selectivity.of(0.5).get,
      Selectivity.ONE
    ))

    costFor(
      plan,
      withLimit,
      builder.getSemanticTable,
      builder.cardinalities,
      new StubProvidedOrders
    ) should be < costFor(plan, withoutLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders)
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
    val withLimit = QueryGraphSolverInput.empty.withLimitSelectivityConfig(LimitSelectivityConfig(
      Selectivity.of(0.5).get,
      Selectivity.ONE
    ))

    costFor(
      plan,
      withLimit,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders
    ) should be < costFor(plan, withoutLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders)
  }

  test("eager plans should cost the same regardless of limit selectivity") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .aggregation(Seq("n.prop AS x"), Seq("max(n.val) AS y"))
      .allNodeScan("n").withCardinality(123)
      .build()

    val withoutLimit = QueryGraphSolverInput.empty
    val withLimit = QueryGraphSolverInput.empty.withLimitSelectivityConfig(LimitSelectivityConfig(
      Selectivity.of(0.5).get,
      Selectivity.ONE
    ))

    costFor(plan, withLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders) should equal(
      costFor(plan, withoutLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders)
    )
  }

  test("cartesian product with 1 row from the left is equally expensive in both execution models") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(100)
      .|.argument("b").withCardinality(100)
      .argument("a").withCardinality(1)
      .build()

    costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders,
      Volcano
    ) should equal(
      costFor(
        plan,
        QueryGraphSolverInput.empty,
        builder.getSemanticTable,
        builder.cardinalities,
        builder.providedOrders,
        BatchedSingleThreaded(SMALL_CHUNK_SIZE, BIG_CHUNK_SIZE)
      )
    )
  }

  test(
    "cartesian product with many rows from the left is multiple factors cheaper in Batched execution, big chunk size"
  ) {
    val cardinality = 1500.0
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(
        cardinality * cardinality
      ) // 2250000 > BIG_CHUNK_SIZE, so big chunk size should be picked
      .|.argument("b").withCardinality(cardinality)
      .argument("a").withCardinality(cardinality)
      .build()

    val volcanoCost = costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders,
      Volcano
    )
    val batchedCost = costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders,
      BatchedSingleThreaded(SMALL_CHUNK_SIZE, BIG_CHUNK_SIZE)
    )

    // The reduction in cost should be proportional to some factor of batch size. This is a somewhat made up but hopefully conservative estimate.
    val factor = BIG_CHUNK_SIZE / 3
    batchedCost * factor should /*still*/ be < volcanoCost
  }

  test("cartesian product with many rows from the left is cheaper in Batched execution, small chunk size") {
    val cardinalityLeft = 200.0
    val cardinalityRight = 2.0
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(
        cardinalityLeft * cardinalityRight
      ) // 400 < BIG_CHUNK_SIZE, so small batch size should be picked
      .|.argument("b").withCardinality(cardinalityRight)
      .argument("a").withCardinality(cardinalityLeft)
      .build()

    val volcanoCost = costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders,
      Volcano
    )
    val batchedCost = costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders,
      BatchedSingleThreaded(SMALL_CHUNK_SIZE, BIG_CHUNK_SIZE)
    )

    // when RHS has small cardinality, the effect of batching is not as pronounced, so we don't multiply with any factor as in test above.
    batchedCost should be < volcanoCost
  }

  test(
    "should pick big chunk size if a plan below the cartesian product has a higher cardinality than big chunk size"
  ) {
    val cardinalityLeaves = 1500.0
    val cardinalityCP = 500.0
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(
        cardinalityCP
      ) // This does not make sense mathematically, but that is OK for this test
      .|.argument("b").withCardinality(cardinalityLeaves)
      .argument("a").withCardinality(cardinalityLeaves)
      .build()

    val argCost = Cardinality(cardinalityLeaves) * DEFAULT_COST_PER_ROW

    costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders,
      BatchedSingleThreaded(SMALL_CHUNK_SIZE, BIG_CHUNK_SIZE)
    ) should
      equal(argCost + argCost * Math.ceil(cardinalityLeaves / BIG_CHUNK_SIZE))
  }

  test(
    "should pick big chunk size if a plan above the cartesian product has a higher cardinality than big chunk size"
  ) {
    val cardinalityLater = 1500.0
    val cardinalityEarlier = 500.0
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .unwind("[1, 2, 3] AS x").withCardinality(cardinalityLater)
      .cartesianProduct().withCardinality(
        cardinalityEarlier
      ) // This does not make sense mathematically, but that is OK for this test
      .|.argument("b").withCardinality(cardinalityEarlier)
      .argument("a").withCardinality(cardinalityEarlier)
      .build()

    val argCost = Cardinality(cardinalityEarlier) * DEFAULT_COST_PER_ROW
    val unwindCost =
      Cardinality(
        cardinalityEarlier
      ) * DEFAULT_COST_PER_ROW // cost of unwind is determined on the amount of incoming rows

    costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders,
      BatchedSingleThreaded(SMALL_CHUNK_SIZE, BIG_CHUNK_SIZE)
    ) should
      equal(unwindCost + argCost + argCost * Math.ceil(cardinalityEarlier / BIG_CHUNK_SIZE))
  }

  test(
    "cartesian product with many row from the left but with provided order is equally expensive in both execution models"
  ) {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(10000).withProvidedOrder(ProvidedOrder.asc(varFor("a")))
      .|.argument("b").withCardinality(100)
      .argument("a").withCardinality(100).withProvidedOrder(ProvidedOrder.asc(varFor("a")))
      .build()

    costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders,
      Volcano
    ) should equal(
      costFor(
        plan,
        QueryGraphSolverInput.empty,
        builder.getSemanticTable,
        builder.cardinalities,
        builder.providedOrders,
        BatchedSingleThreaded(SMALL_CHUNK_SIZE, BIG_CHUNK_SIZE)
      )
    )
  }

  test("should not affect cardinality of cartesian with no limit in volcano mode") {
    val lhsCard = 10.0
    val rhsCard = 10.0
    val chunkSize = 1
    val effective = effectiveCardinalitiesOfCartesian(lhsCard, rhsCard, chunkSize)
    effective.lhs shouldEqual Cardinality(10)
    effective.rhs shouldEqual Cardinality(10)
  }

  test("should reduce cardinality of cartesian under limit in volcano mode") {
    val limit = 1
    val lhsCard = 10.0
    val rhsCard = 10.0
    val chunkSize = 1
    val effective = effectiveCardinalitiesOfCartesian(lhsCard, rhsCard, chunkSize, Some(limit))
    effective.lhs shouldEqual Cardinality(1)
    effective.rhs shouldEqual Cardinality(1)
  }

  test("effective cardinality of cartesian is unaffected by batches that divide cardinalities evenly") {
    val lhsCard = 100.0
    val rhsCard = 100.0
    val chunkSize = 10
    val effective = effectiveCardinalitiesOfCartesian(lhsCard, rhsCard, chunkSize)
    // 10 batches needed from lhs, each yielding 10*100 final rows, so rhs needs to produce 100 rows per execution
    effective.lhs shouldEqual Cardinality(lhsCard)
    effective.rhs shouldEqual Cardinality(rhsCard)
  }

  test("effective cardinality of cartesian is unaffected by batches that are larger than inputs") {
    val lhsCard = 10.0
    val rhsCard = 100.0
    val chunkSize = 200
    val effective = effectiveCardinalitiesOfCartesian(lhsCard, rhsCard, chunkSize)
    // Only a single batch from each side is needed
    effective.lhs shouldEqual Cardinality(lhsCard)
    effective.rhs shouldEqual Cardinality(rhsCard)
  }

  test("effective cardinality of cartesian is unaffected by batches that are smaller than rhs") {
    val lhsCard = 10.0
    val rhsCard = 15.0
    val chunkSize = 10
    val effective = effectiveCardinalitiesOfCartesian(lhsCard, rhsCard, chunkSize)
    // Only a single batch from each side is needed
    effective.lhs shouldEqual Cardinality(lhsCard)
    effective.rhs shouldEqual Cardinality(rhsCard)
  }

  test("effective cardinality of cartesian is unaffected by batches that are larger than inputs, mirrored plan") {
    val lhsCard = 100.0
    val rhsCard = 10.0
    val chunkSize = 200
    val effective = effectiveCardinalitiesOfCartesian(lhsCard, rhsCard, chunkSize)
    // Only a single batch from each side is needed
    effective.lhs shouldEqual Cardinality(lhsCard)
    effective.rhs shouldEqual Cardinality(rhsCard)
  }

  test("effective cardinality of cartesian is unaffected by small batches that are larger than inputs, mirrored plan") {
    val limit = 1
    val lhsCard = 10.0
    val rhsCard = 10.0
    val chunkSize = 16
    val effective = effectiveCardinalitiesOfCartesian(lhsCard, rhsCard, chunkSize, Some(limit))

    // single batch from each side, but input is depleted at 10 rows. rhs produces 10/10=1 row per lhs row.
    effective.lhs shouldEqual Cardinality(lhsCard)
    effective.rhs shouldEqual Cardinality(rhsCard)
  }

  test("effective cardinality of cartesian should account for batch size smaller than sides under large limit") {
    val limit = 70
    val lhsCard = 10.0
    val rhsCard = 10.0
    val chunkSize = 4
    val effective = effectiveCardinalitiesOfCartesian(lhsCard, rhsCard, chunkSize, Some(limit))

    // We need approx. 1.75 executions of rhs to produce 70 rows
    val executions = limit.toDouble / (rhsCard * chunkSize)
    // This means we will produce 2 chunks from lhs
    val chunks = Math.ceil(executions)
    val lhsEff = chunks * chunkSize
    // And produce approx. this many rows from rhs per execution
    val rhsEff = rhsCard

    effective.lhs shouldEqual Cardinality(lhsEff)
    effective.rhs shouldEqual Cardinality(rhsEff)
  }

  test("computing effective cardinality of cartesian should be safe when LHS and RHS cardinality is zero") {
    val limit = 10
    val lhsCard = 0.0
    val rhsCard = 0.0
    val chunkSize = 4
    val effective = effectiveCardinalitiesOfCartesian(lhsCard, rhsCard, chunkSize, Some(limit))

    effective.lhs shouldEqual Cardinality(lhsCard)
    effective.rhs shouldEqual Cardinality(rhsCard)
  }

  test("computing effective cardinality of cartesian should be safe when limit is zero") {
    val limit = 0
    val lhsCard = 10.0
    val rhsCard = 10.0
    val chunkSize = 4
    val effective = effectiveCardinalitiesOfCartesian(lhsCard, rhsCard, chunkSize, Some(limit))

    effective.lhs shouldEqual Cardinality(limit)
    effective.rhs shouldEqual Cardinality(limit)
  }

  test("effective cardinality of nested cartesian products") {
    val cardA = 10.0
    val cardB = 10.0
    val cardC = 10.0

    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(cardA * cardB * cardC)
      .|.cartesianProduct().withCardinality(cardB * cardC)
      .|.|.allNodeScan("C").withCardinality(cardC)
      .|.allNodeScan("B").withCardinality(cardB)
      .allNodeScan("A").withCardinality(cardA)

    val effective = CardinalityCostModel.effectiveCardinalities(
      plan.build(),
      WorkReduction.NoReduction,
      ExecutionModel.VolcanoBatchSize,
      plan.cardinalities
    )

    effective.lhs shouldEqual Cardinality(10)
    effective.rhs shouldEqual Cardinality(100)
  }

  private def effectiveCardinalitiesOfCartesian(
    lhsCard: Double,
    rhsCard: Double,
    chunkSize: Int,
    limit: Option[Int] = None
  ) = {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(lhsCard * rhsCard)
      .|.allNodeScan("rhs").withCardinality(rhsCard)
      .allNodeScan("lhs").withCardinality(lhsCard)
      .build()
    val executionModel =
      if (chunkSize == 1) ExecutionModel.Volcano else ExecutionModel.BatchedSingleThreaded(chunkSize, chunkSize)
    val batchSize = executionModel.selectBatchSize(plan, builder.cardinalities)
    val workReduction = limit.map(l =>
      WorkReduction(Selectivity(Multiplier.of(l.toDouble / (lhsCard * rhsCard)).getOrElse(Multiplier.ZERO).coefficient))
    )
      .getOrElse(WorkReduction.NoReduction)
    CardinalityCostModel.effectiveCardinalities(plan, workReduction, batchSize, builder.cardinalities)
  }

  test("should count cost for different label checks") {
    val cardinality = 100.0
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .filterExpression(hasLabels("n", "N"), hasTypes("r", "R"), hasLabelsOrTypes("x", "X"))
      .argument("n", "r", "x").withCardinality(cardinality)
      .build()

    val expectedCost = Cardinality(cardinality) * (DEFAULT_COST_PER_ROW + CostPerRow(LABEL_CHECK_DB_HITS) * 3)

    costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders
    ) shouldBe expectedCost
  }

  test("sort should cost the same regardless of limit selectivity") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .sort("n ASC").withCardinality(100)
      .allNodeScan("n").withCardinality(100)
      .build()
    val semanticTable = SemanticTable().addNode(varFor("n"))
    val withoutLimit = QueryGraphSolverInput.empty
    val withLimit = QueryGraphSolverInput.empty.withLimitSelectivityConfig(LimitSelectivityConfig(
      Selectivity.of(0.5).get,
      Selectivity.ONE
    ))
    val unlimited = costFor(plan, withLimit, semanticTable, builder.cardinalities, builder.providedOrders)
    val limited = costFor(plan, withoutLimit, semanticTable, builder.cardinalities, builder.providedOrders)
    unlimited should equal(limited)
  }

  test("should reduce cardinality of all semiApply variants") {
    val plans = Seq[LogicalPlanBuilder => LogicalPlan](
      _.semiApply().withCardinality(75)
        .|.expandAll("(n)-->()").withCardinality(12345)
        .|.argument("n").withCardinality(100)
        .allNodeScan("n").withCardinality(100)
        .build(),
      _.antiSemiApply().withCardinality(25)
        .|.expandAll("(n)-->()").withCardinality(12345)
        .|.argument("n").withCardinality(100)
        .allNodeScan("n").withCardinality(100)
        .build(),
      _.letSemiApply("x").withCardinality(100)
        .|.expandAll("(n)-->()").withCardinality(12345)
        .|.argument("n").withCardinality(100)
        .allNodeScan("n").withCardinality(100)
        .build(),
      _.letAntiSemiApply("x").withCardinality(100)
        .|.expandAll("(n)-->()").withCardinality(12345)
        .|.argument("n").withCardinality(100)
        .allNodeScan("n").withCardinality(100)
        .build(),
      _.selectOrSemiApply("x").withCardinality(50)
        .|.expandAll("(n)-->()").withCardinality(12345)
        .|.argument("n").withCardinality(100)
        .allNodeScan("n").withCardinality(100)
        .build(),
      _.selectOrAntiSemiApply("x").withCardinality(50)
        .|.expandAll("(n)-->()").withCardinality(12345)
        .|.argument("n").withCardinality(100)
        .allNodeScan("n").withCardinality(100)
        .build(),
      _.letSelectOrSemiApply("x", "true").withCardinality(50)
        .|.expandAll("(n)-->()").withCardinality(12345)
        .|.argument("n").withCardinality(100)
        .allNodeScan("n").withCardinality(100)
        .build(),
      _.letSelectOrAntiSemiApply("x", "true").withCardinality(50)
        .|.expandAll("(n)-->()").withCardinality(12345)
        .|.argument("n").withCardinality(100)
        .allNodeScan("n").withCardinality(100)
        .build()
    )

    def workReductionOf(
      buildPlan: LogicalPlanBuilder => LogicalPlan,
      incomingWorkReduction: WorkReduction,
      executionModel: ExecutionModel = ExecutionModel.default
    ) = {
      val builder = new LogicalPlanBuilder(wholePlan = false)
      val plan = buildPlan(builder)
      val batchSize = executionModel.selectBatchSize(plan, builder.cardinalities)
      val reduction =
        CardinalityCostModel.childrenWorkReduction(plan, incomingWorkReduction, batchSize, builder.cardinalities)
      (plan, reduction)
    }

    plans.foreach { buildPlan =>
      val incoming = WorkReduction.NoReduction
      val (plan, reduction) = workReductionOf(buildPlan, incoming)
      withClue(LogicalPlanToPlanBuilderString(plan)) {
        reduction shouldEqual (
          (
            incoming,
            WorkReduction(fraction = Selectivity(1.0 / 12345.0), minimum = Some(Cardinality(1)))
          )
        )
      }
    }

    plans.foreach { buildPlan =>
      val incoming = WorkReduction(Selectivity(0.3))
      val (plan, reduction) = workReductionOf(buildPlan, incoming)
      withClue(LogicalPlanToPlanBuilderString(plan)) {
        reduction shouldEqual (
          (
            incoming,
            WorkReduction(fraction = Selectivity(1.0 / 12345.0), minimum = Some(Cardinality(1)))
          )
        )
      }
    }
  }

  test("should raise the cost of directed relationship type scan when property is used") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .relationshipTypeScan("(a)-[r:REL]->(b)").withCardinality(100.0)
      .build()

    val lowCost = costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders
    )

    val highCost = costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders,
      propertyAccess = Set(PropertyAccess(v"r", "prop"))
    )

    lowCost should be < highCost
  }

  test("should raise the cost of undirected relationship type scan when property is used") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .relationshipTypeScan("(a)-[r:REL]-(b)").withCardinality(100.0)
      .build()

    val lowCost = costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders
    )

    val highCost = costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders,
      propertyAccess = Set(PropertyAccess(v"r", "prop"))
    )

    lowCost should be < highCost
  }

  test("shouldn't round LHS of Apply cardinality to one when calculating costs") {
    def costWithLhsCardinality(lhsCardinality: Double): Cost = {
      val builder = new LogicalPlanBuilder(wholePlan = false)
      val plan = builder
        .apply()
        .|.allNodeScan("b", "a").withCardinality(100)
        .filter("a.prop1 = 42").withCardinality(lhsCardinality)
        .allNodeScan("a").withCardinality(100)
        .build()
      costFor(
        plan,
        QueryGraphSolverInput.empty,
        builder.getSemanticTable,
        builder.cardinalities,
        builder.providedOrders
      )
    }

    costWithLhsCardinality(0.5) should be < costWithLhsCardinality(1.0)
  }

  test("should only count properties inside the inequalities of an AndedPropertyInequalities") {

    def costForPredicate(predicate: Expression): Cost = {
      val builder = new LogicalPlanBuilder(wholePlan = false)

      val plan =
        builder
          .filterExpression(predicate).withCardinality(36)
          .nodeByLabelScan("a", "Foo").withCardinality(120)
          .build()

      costFor(
        plan,
        QueryGraphSolverInput.empty,
        builder.getSemanticTable,
        builder.cardinalities,
        builder.providedOrders
      )
    }

    val inequality = greaterThan(prop("a", "prop"), literalUnsignedInt(10))

    costForPredicate(andedPropertyInequalities(inequality)) shouldEqual costForPredicate(inequality)
  }

  test("should not calculate cost of plans on RHS of semiApply to less than cost of producing one row") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .semiApply().withCardinality(500)
      .|.expandAll("(a)<-[r:REL]-(b)").withCardinality(2)
      .|.argument("a").withCardinality(1)
      .nodeByLabelScan("a", "Label").withCardinality(1000)
      .build()

    val cost = costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders
    )

    val labelScanCost = INDEX_SCAN_COST_PER_ROW * 1000

    val semiApplyRhsCost =
      EXPAND_ALL_COST * 1 + // expandAll
        DEFAULT_COST_PER_ROW * 1 // argument

    val semiApplyCost = Cost(labelScanCost + 1000 * semiApplyRhsCost.cost)

    cost should equal(semiApplyCost)
  }

  case class TrailTestCase(
    builder: LogicalPlanBuilder,
    plan: LogicalPlan,
    lhsCardinality: Cardinality,
    rhsCardinality: Cardinality,
    lhsCost: Cost,
    rhsCost: Cost
  )

  def trailTestCase(min: Int, max: UpperBound): TrailTestCase = {
    val lhsCardinality = Cardinality(10)
    val rhsCardinality = Cardinality(1.5)
    val trailParams = TrailParameters(
      min = min,
      max = max,
      start = "a",
      end = "b",
      innerStart = "a",
      innerEnd = "b",
      groupNodes = Set(("a", "a"), ("b", "b")),
      groupRelationships = Set(("r", "r")),
      innerRelationships = Set("r"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty,
      reverseGroupVariableProjections = false
    )

    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .trail(trailParams)
      .|.expand("(a)-[r]->(b)").withCardinality(rhsCardinality.amount)
      .|.argument("a").withCardinality(1)
      .argument("a").withCardinality(lhsCardinality.amount)
      .build()

    val lhsCost = costFor(
      plan.lhs.get,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders
    )
    val rhsCost = costFor(
      plan.rhs.get,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders
    )
    TrailTestCase(builder, plan, lhsCardinality, rhsCardinality, lhsCost, rhsCost)
  }

  def assertTrailHasExpectedCost(testCase: TrailTestCase, expectedCost: Cost): Assertion = {
    costFor(
      testCase.plan,
      QueryGraphSolverInput.empty,
      testCase.builder.getSemanticTable,
      testCase.builder.cardinalities,
      testCase.builder.providedOrders
    ) should equal(expectedCost)
  }

  test("trail cost {X, 1}") {
    val testCase0_1 = trailTestCase(0, Limited(1))
    val testCase1_1 = trailTestCase(1, Limited(1))

    val expected = testCase0_1.lhsCost + testCase0_1.lhsCardinality * testCase0_1.rhsCost
    assertTrailHasExpectedCost(testCase0_1, expected)
    assertTrailHasExpectedCost(testCase1_1, expected)
  }

  test("trail cost {X, 2}") {
    val testCase0_2 = trailTestCase(0, Limited(2))
    val testCase1_2 = trailTestCase(1, Limited(2))
    val testCase2_2 = trailTestCase(2, Limited(2))

    val iter1Cost = testCase0_2.lhsCardinality * testCase0_2.rhsCost
    val iter2Cost = testCase0_2.lhsCardinality * testCase0_2.rhsCardinality * testCase0_2.rhsCost
    val expected = testCase0_2.lhsCost + iter1Cost + iter2Cost
    assertTrailHasExpectedCost(testCase0_2, expected)
    assertTrailHasExpectedCost(testCase1_2, expected)
    assertTrailHasExpectedCost(testCase2_2, expected)
  }

  test("trail cost {X, 3}") {
    val testCase0_3 = trailTestCase(0, Limited(3))
    val testCase1_3 = trailTestCase(1, Limited(3))
    val testCase2_3 = trailTestCase(2, Limited(3))
    val testCase3_3 = trailTestCase(3, Limited(3))

    val iter1Cost = testCase0_3.lhsCardinality * testCase0_3.rhsCost
    val iterNCost = testCase0_3.lhsCardinality * testCase0_3.rhsCardinality * testCase0_3.rhsCost
    val expected = testCase0_3.lhsCost + iter1Cost + iterNCost + iterNCost
    assertTrailHasExpectedCost(testCase0_3, expected)
    assertTrailHasExpectedCost(testCase1_3, expected)
    assertTrailHasExpectedCost(testCase2_3, expected)
    assertTrailHasExpectedCost(testCase3_3, expected)
  }

  test("statefulShortestPath") {
    val nfa = new TestNFABuilder(0, "u")
      .addTransition(0, 1, "(u) (n)")
      .addTransition(1, 2, "(n)-[r]->(m)")
      .addTransition(2, 1, "(m) (n)")
      .addTransition(2, 3, "(m) (v)")
      .setFinalState(3)
      .build()

    val ansCardinality = 10
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .statefulShortestPath(
        "u",
        "v",
        "SHORTEST 1 ((u) ((n)-[r]->(m)){1, } (v) WHERE unique(`r`))",
        None,
        groupNodes = Set(("n", "n"), ("m", "m")),
        groupRelationships = Set(("r", "r")),
        singletonNodeVariables = Set("v" -> "v"),
        singletonRelationshipVariables = Set(),
        StatefulShortestPath.Selector.Shortest(1),
        nfa,
        ExpandAll,
        reverseGroupVariableProjections = false
      ).withCardinality(100)
      .allNodeScan("u").withCardinality(ansCardinality)
      .build()

    costFor(
      plan,
      QueryGraphSolverInput.empty,
      builder.getSemanticTable,
      builder.cardinalities,
      builder.providedOrders
    ) should equal(Cost(
      ansCardinality * ALL_SCAN_COST_PER_ROW +
        ansCardinality * SHORTEST_PRODUCT_GRAPH_COST
    ))
  }

  test("should prefer relationship type scan over all nodes scan + expand") {
    val ansExpandCost = {
      val builder = new LogicalPlanBuilder(wholePlan = false)
      val plan = builder
        .expandAll("(a)-[r:REL]->(b)").withCardinality(1000)
        .allNodeScan("a").withCardinality(100)
        .build()

      costFor(
        plan,
        QueryGraphSolverInput.empty,
        builder.getSemanticTable,
        builder.cardinalities,
        builder.providedOrders
      )
    }

    val relTypeScanCost = {
      val builder = new LogicalPlanBuilder(wholePlan = false)
      val plan = builder
        .relationshipTypeScan("(a)-[r:REL]->(b)").withCardinality(1000)
        .build()

      costFor(
        plan,
        QueryGraphSolverInput.empty,
        builder.getSemanticTable,
        builder.cardinalities,
        builder.providedOrders
      )
    }

    ansExpandCost should be > relTypeScanCost
  }

  test("should prefer all relationship scan over all nodes scan + expand") {
    val ansExpandCost = {
      val builder = new LogicalPlanBuilder(wholePlan = false)
      val plan = builder
        .expandAll("(a)-[r]->(b)").withCardinality(1000)
        .allNodeScan("a").withCardinality(100)
        .build()

      costFor(
        plan,
        QueryGraphSolverInput.empty,
        builder.getSemanticTable,
        builder.cardinalities,
        builder.providedOrders
      )
    }

    val allRelScanCost = {
      val builder = new LogicalPlanBuilder(wholePlan = false)
      val plan = builder
        .allRelationshipsScan("(a)-[r]->(b)").withCardinality(1000)
        .build()

      costFor(
        plan,
        QueryGraphSolverInput.empty,
        builder.getSemanticTable,
        builder.cardinalities,
        builder.providedOrders
      )
    }

    ansExpandCost should be > allRelScanCost
  }

  test("should prefer relationship type scan over all nodes scan + projection + sort + expand") {
    val ansExpandCost = {
      val builder = new LogicalPlanBuilder(wholePlan = false)
      val plan = builder
        .expandAll("(a)-[r:REL]->(b)").withCardinality(1000)
        .sort("p ASC").withCardinality(100)
        .projection("a.prop AS p").withCardinality(100)
        .allNodeScan("a").withCardinality(100)
        .build()

      costFor(
        plan,
        QueryGraphSolverInput.empty,
        builder.getSemanticTable,
        builder.cardinalities,
        builder.providedOrders
      )
    }

    val relTypeScanCost = {
      val builder = new LogicalPlanBuilder(wholePlan = false)
      val plan = builder
        .relationshipTypeScan("(a)-[r:REL]->(b)").withCardinality(1000)
        .build()

      costFor(
        plan,
        QueryGraphSolverInput.empty,
        builder.getSemanticTable,
        builder.cardinalities,
        builder.providedOrders
      )
    }

    ansExpandCost should be > relTypeScanCost
  }

  test("should prefer all relationship scan over all nodes scan + projection + sort + expand") {
    val ansExpandCost = {
      val builder = new LogicalPlanBuilder(wholePlan = false)
      val plan = builder
        .expandAll("(a)-[r]->(b)").withCardinality(1000)
        .sort("p ASC").withCardinality(100)
        .projection("a.prop AS p").withCardinality(100)
        .allNodeScan("a").withCardinality(100)
        .build()

      costFor(
        plan,
        QueryGraphSolverInput.empty,
        builder.getSemanticTable,
        builder.cardinalities,
        builder.providedOrders
      )
    }

    val allRelScanCost = {
      val builder = new LogicalPlanBuilder(wholePlan = false)
      val plan = builder
        .allRelationshipsScan("(a)-[r]->(b)").withCardinality(1000)
        .build()

      costFor(
        plan,
        QueryGraphSolverInput.empty,
        builder.getSemanticTable,
        builder.cardinalities,
        builder.providedOrders
      )
    }

    ansExpandCost should be > allRelScanCost
  }

  test("should not prefer relationship type scan over all nodes scan + filter + expand") {
    val ansExpandCost = {
      val builder = new LogicalPlanBuilder(wholePlan = false)
      val plan = builder
        .expandAll("(a)-[r:REL]->(b)").withCardinality(1000)
        .filter("a.prop = 0").withCardinality(10)
        .allNodeScan("a").withCardinality(100)
        .build()

      costFor(
        plan,
        QueryGraphSolverInput.empty,
        builder.getSemanticTable,
        builder.cardinalities,
        builder.providedOrders
      )
    }

    val relTypeScanCost = {
      val builder = new LogicalPlanBuilder(wholePlan = false)
      val plan = builder
        .relationshipTypeScan("(a)-[r:REL]->(b)").withCardinality(1000)
        .build()

      costFor(
        plan,
        QueryGraphSolverInput.empty,
        builder.getSemanticTable,
        builder.cardinalities,
        builder.providedOrders
      )
    }

    ansExpandCost should be < relTypeScanCost
  }
}
