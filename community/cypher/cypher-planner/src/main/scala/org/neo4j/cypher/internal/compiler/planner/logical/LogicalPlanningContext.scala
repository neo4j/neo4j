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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.csv.reader.Configuration.DEFAULT_LEGACY_STYLE_QUOTING
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.limit.LimitSelectivityConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.CostComparisonListener
import org.neo4j.cypher.internal.compiler.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.DefaultProvidedOrderFactory
import org.neo4j.cypher.internal.ir.ordering.NoProvidedOrderFactory
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.attribution.IdGen

case class LogicalPlanningContext(planContext: PlanContext,
                                  logicalPlanProducer: LogicalPlanProducer,
                                  metrics: Metrics,
                                  semanticTable: SemanticTable,
                                  strategy: QueryGraphSolver,
                                  input: QueryGraphSolverInput = QueryGraphSolverInput.empty,
                                  // When planning tails, this gives contextual information about the plan of the so-far solved
                                  // query graphs, which will be connected with an Apply with the tail-query graph plan.
                                  outerPlan: Option[LogicalPlan] = None,
                                  isInSubquery: Boolean = false,
                                  notificationLogger: InternalNotificationLogger,
                                  useErrorsOverWarnings: Boolean = false,
                                  errorIfShortestPathFallbackUsedAtRuntime: Boolean = false,
                                  errorIfShortestPathHasCommonNodesAtRuntime: Boolean = true,
                                  legacyCsvQuoteEscaping: Boolean = DEFAULT_LEGACY_STYLE_QUOTING,
                                  csvBufferSize: Int = 2 * 1024 * 1024,
                                  config: QueryPlannerConfiguration = QueryPlannerConfiguration.default,
                                  costComparisonListener: CostComparisonListener,
                                  planningAttributes: PlanningAttributes,
                                  indexCompatiblePredicatesProviderContext: IndexCompatiblePredicatesProviderContext = IndexCompatiblePredicatesProviderContext.default,
                                  /*
                                   * All properties that are referenced (in the head planner query)
                                   * Used to break potential ties between index leaf plans
                                   */
                                  accessedProperties: Set[PropertyAccess] = Set.empty,
                                  idGen: IdGen,
                                  executionModel: ExecutionModel,
                                  debugOptions: CypherDebugOptions,
                                  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
                                  cancellationChecker: CancellationChecker,
                                  planningTextIndexesEnabled: Boolean = true,
                                  planningRangeIndexesEnabled: Boolean = GraphDatabaseInternalSettings.planning_range_indexes_enabled.defaultValue(),
                                  planningPointIndexesEnabled: Boolean = GraphDatabaseInternalSettings.planning_point_indexes_enabled.defaultValue(),
                                 ) {

  def withLimitSelectivityConfig(cfg: LimitSelectivityConfig): LogicalPlanningContext =
    copy(input = input.withLimitSelectivityConfig(cfg))

  def withAggregationProperties(properties: Set[PropertyAccess]): LogicalPlanningContext =
    copy(indexCompatiblePredicatesProviderContext = indexCompatiblePredicatesProviderContext.copy(aggregatingProperties = properties))

  def withAccessedProperties(properties: Set[PropertyAccess]): LogicalPlanningContext =
    copy(accessedProperties = properties)

  def withUpdatedLabelInfo(plan: LogicalPlan): LogicalPlanningContext =
    copy(input = input.withUpdatedLabelInfo(plan, planningAttributes.solveds))

  def withFusedLabelInfo(newLabelInfo: LabelInfo): LogicalPlanningContext =
    copy(input = input.withFusedLabelInfo(newLabelInfo))

  def withConfig(newConfig: QueryPlannerConfiguration): LogicalPlanningContext = {
    copy(config = newConfig)
  }

  /**
   * When planning tails, the outer plan gives contextual information about the plan of the so-far solved
   * query graphs, which will be connected with an Apply with the tail-query graph plan.
   */
  def withOuterPlan(outerPlan: LogicalPlan): LogicalPlanningContext = {
    copy(outerPlan = Some(outerPlan))
  }

  def forSubquery(): LogicalPlanningContext = {
    copy(isInSubquery = true)
  }

  def withActivePlanner(planner: PlannerType): LogicalPlanningContext =
    copy(input = input.copy(activePlanner = planner))

  def statistics: GraphStatistics = planContext.statistics

  def cost: CostModel = metrics.cost

  def cardinality: CardinalityModel = metrics.cardinality

  def providedOrderFactory: ProvidedOrderFactory = {
    if (executionModel.providedOrderPreserving) {
      DefaultProvidedOrderFactory
    } else {
      NoProvidedOrderFactory
    }
  }

  def withLastSolvedQueryPart(queryPart: SinglePlannerQuery): LogicalPlanningContext = {
    val hasUpdates = indexCompatiblePredicatesProviderContext.outerPlanHasUpdates || !queryPart.readOnlySelf
    copy(indexCompatiblePredicatesProviderContext = indexCompatiblePredicatesProviderContext.copy(outerPlanHasUpdates = hasUpdates))
  }
}
