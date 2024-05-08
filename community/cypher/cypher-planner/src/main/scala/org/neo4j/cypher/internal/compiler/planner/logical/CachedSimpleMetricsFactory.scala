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

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.LabelInferenceStrategy
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Ref

object CachedSimpleMetricsFactory extends MetricsFactory {

  override def newCardinalityEstimator(
    queryGraphCardinalityModel: QueryGraphCardinalityModel,
    selectivityCalculator: SelectivityCalculator,
    evaluator: ExpressionEvaluator
  ): CardinalityModel = {
    val wrapped: StatisticsBackedCardinalityModel =
      SimpleMetricsFactory.newCardinalityEstimator(queryGraphCardinalityModel, selectivityCalculator, evaluator)
    new CachedStatisticsBackedCardinalityModel(wrapped)
  }

  /*
   * The arguments to the CachedFunction are the cache-key, e.g plan, input, semanticTable, cardinalities, providedOrders, monitor.
   *
   * We wrap the Cardinalities/ProvidedOrders object in the `Ref` class since we don't need to compare the attributes inside
   * the Cardinalities/ProvidedOrders object.
   * The reason for this is that the objects are mutable and during planning we modify them instead of creating new ones.
   */
  override def newCostModel(executionModel: ExecutionModel, cancellationChecker: CancellationChecker): CostModel = {
    val cached = CachedFunction(
      (
        plan: LogicalPlan,
        input: QueryGraphSolverInput,
        semanticTable: SemanticTable,
        cardinalities: Ref[Cardinalities],
        providedOrders: Ref[ProvidedOrders],
        propertyAccess: Set[PropertyAccess],
        statistics: Ref[GraphStatistics],
        monitor: CostModelMonitor
      ) => {
        SimpleMetricsFactory.newCostModel(executionModel, cancellationChecker).costFor(
          plan,
          input,
          semanticTable,
          cardinalities.value,
          providedOrders.value,
          propertyAccess,
          statistics.value,
          monitor
        )
      }
    )
    (
      plan: LogicalPlan,
      input: Metrics.QueryGraphSolverInput,
      semanticTable: SemanticTable,
      cardinalities: Cardinalities,
      providedOrders: ProvidedOrders,
      propertyAccess: Set[PropertyAccess],
      statistics: GraphStatistics,
      monitor: CostModelMonitor
    ) => {
      cached(
        plan,
        input,
        semanticTable,
        Ref(cardinalities),
        Ref(providedOrders),
        propertyAccess,
        Ref(statistics),
        monitor
      )
    }
  }

  override def newQueryGraphCardinalityModel(
    planContext: PlanContext,
    selectivityCalculator: SelectivityCalculator,
    labelInferenceStrategy: LabelInferenceStrategy
  ): QueryGraphCardinalityModel = {
    val wrapped: QueryGraphCardinalityModel =
      SimpleMetricsFactory.newQueryGraphCardinalityModel(planContext, selectivityCalculator, labelInferenceStrategy)
    new CachedQueryGraphCardinalityModel(wrapped)
  }
}
