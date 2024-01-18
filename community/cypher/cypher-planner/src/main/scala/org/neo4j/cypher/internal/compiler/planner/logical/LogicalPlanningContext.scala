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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.UpdateStrategy
import org.neo4j.cypher.internal.compiler.defaultUpdateStrategy
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext.PlannerState
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext.Settings
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext.StaticComponents
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.limit.LimitSelectivityConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.CostComparisonListener
import org.neo4j.cypher.internal.compiler.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ordering.DefaultProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.ordering.ParallelExecutionProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrderFactory
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.attribution.IdGen

object LogicalPlanningContext {

  /**
   * Components that remain the same object during one planner invocation.
   * Some of the components are mutable.
   *
   * These components '''must''' not have any influence on caching.
   *
   * @param planContext the planContext can be used to ask about indexes and statistics.
   *                    It is not relevant for caching, because we protect the cache by other measures.
   *                    A new index automatically clears the caches.
   *                    And plans get evicted if the statistics they are based on diverge too much.
   * @param notificationLogger This can be used to log notifications to the user.
   * @param planningAttributes mutable attributes for LogicalPlans
   * @param logicalPlanProducer a planner component that created LogicalPlans
   * @param queryGraphSolver  a planner component to plan query graphs
   * @param metrics the cost and cardinality models, based on database statistics.
   *                It is not relevant for caching, because we protect the cache by other measures.
   *                Plans get evicted if the statistics they are based on diverge too much.
   * @param idGen a mutable generator for LogicalPlan IDs
   * @param anonymousVariableNameGenerator a mutable generator for anonymous variable names.
   * @param cancellationChecker used to abort long computations if the transaction has been killed.
   * @param semanticTable the semantic table
   *
   */
  case class StaticComponents(
    planContext: PlanContext,
    notificationLogger: InternalNotificationLogger,
    planningAttributes: PlanningAttributes,
    logicalPlanProducer: LogicalPlanProducer,
    queryGraphSolver: QueryGraphSolver,
    metrics: Metrics,
    idGen: IdGen,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker,
    semanticTable: SemanticTable,
    costComparisonListener: CostComparisonListener
  )

  /**
   * Settings that remain constant during one planner invocation.
   *
   * These settings '''may''' have an influence on caching.
   *
   * @param executionModel the executionModel is derived from the targeted runtime.
   *                       It is relevant for caching.
   * @param updateStrategy the updateStrategy influence eagerness analysis.
   *                       Relevant for caching, but already included through CypherUpdateStrategy query option.
   * @param debugOptions any debug options set in the query. Those disable caching, thus not relevant for caching.
   * @param predicatesAsUnionMaxSize a setting for the maximum amount of predicates that are attempted to solve with a distinct union.
   *                                 Relevant for caching.
   * @param useErrorsOverWarnings a setting to fail with an error instead of issuing warnings in certain cases.
   * @param errorIfShortestPathFallbackUsedAtRuntime a setting to fail if the shortest path fallback is used at runtime.
   *                                                 Relevant for caching.
   * @param errorIfShortestPathHasCommonNodesAtRuntime a setting to fail if the start and the end node is the same for shortestPaths, at runtime.
   * @param legacyCsvQuoteEscaping a setting to configure quoting in LOAD CSV
   * @param csvBufferSize the buffer size for LOAD CSV
   */
  case class Settings(
    executionModel: ExecutionModel,
    updateStrategy: UpdateStrategy = defaultUpdateStrategy,
    debugOptions: CypherDebugOptions,
    predicatesAsUnionMaxSize: Int = GraphDatabaseInternalSettings.predicates_as_union_max_size.defaultValue(),
    useErrorsOverWarnings: Boolean = GraphDatabaseSettings.cypher_hints_error.defaultValue(),
    errorIfShortestPathFallbackUsedAtRuntime: Boolean =
      GraphDatabaseSettings.forbid_exhaustive_shortestpath.defaultValue(),
    errorIfShortestPathHasCommonNodesAtRuntime: Boolean =
      GraphDatabaseSettings.forbid_shortestpath_common_nodes.defaultValue(),
    legacyCsvQuoteEscaping: Boolean = GraphDatabaseSettings.csv_legacy_quote_escaping.defaultValue(),
    csvBufferSize: Int = GraphDatabaseSettings.csv_buffer_size.defaultValue().intValue(),
    planningIntersectionScansEnabled: Boolean =
      GraphDatabaseInternalSettings.planning_intersection_scans_enabled.defaultValue(),
    eagerAnalyzer: CypherEagerAnalyzerOption = CypherEagerAnalyzerOption.default
  ) {

    def cacheKey(): Seq[Any] = this match {
      // Note: This extra match is here to trigger a compilation error whenever the Signature of Settings is changed,
      // to make the author aware and make them think about whether they want to include a new field in the cache key.
      case Settings(
          executionModel: ExecutionModel,
          _: UpdateStrategy,
          _: CypherDebugOptions,
          predicatesAsUnionMaxSize: Int,
          useErrorsOverWarnings: Boolean,
          errorIfShortestPathFallbackUsedAtRuntime: Boolean,
          errorIfShortestPathHasCommonNodesAtRuntime: Boolean,
          legacyCsvQuoteEscaping: Boolean,
          csvBufferSize: Int,
          planningIntersectionScansEnabled: Boolean,
          eagerAnalyzer: CypherEagerAnalyzerOption
        ) =>
        val builder = Seq.newBuilder[Any]

        builder.addAll(executionModel.cacheKey())

        if (GraphDatabaseInternalSettings.predicates_as_union_max_size.dynamic())
          builder.addOne(predicatesAsUnionMaxSize)

        if (GraphDatabaseSettings.cypher_hints_error.dynamic())
          builder.addOne(useErrorsOverWarnings)

        if (GraphDatabaseSettings.forbid_exhaustive_shortestpath.dynamic())
          builder.addOne(errorIfShortestPathFallbackUsedAtRuntime)

        if (GraphDatabaseSettings.forbid_shortestpath_common_nodes.dynamic())
          builder.addOne(errorIfShortestPathHasCommonNodesAtRuntime)

        if (GraphDatabaseSettings.csv_legacy_quote_escaping.dynamic())
          builder.addOne(legacyCsvQuoteEscaping)

        if (GraphDatabaseSettings.csv_buffer_size.dynamic())
          builder.addOne(csvBufferSize)

        if (GraphDatabaseInternalSettings.planning_intersection_scans_enabled.dynamic())
          builder.addOne(planningIntersectionScansEnabled)

        if (GraphDatabaseInternalSettings.cypher_eager_analysis_implementation.dynamic())
          builder.addOne(eagerAnalyzer)

        builder.result()
    }

    // Note: We currently have no infrastructure to include fields from these settings in the cache key from this place.
    //  If we ever have cache-key-relevant things here, we must either include them through some other place (e.g. QueryOptions),
    // or build the necessary infrastructure.
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      cacheKey().isEmpty
    )
  }

  /**
   * Internal state that changes during one planner invocation.
   *
   * These components '''must''' not have any influence on caching.
   *
   * @param input extracted information from the query, relevant to the QueryGraphSolver.
   * @param outerPlan When planning tails, this gives contextual information about the plan of the so-far solved
   *                                     query graphs, which will be connected with an Apply with the tail-query graph plan.
   * @param isInSubquery whether we are currently planning a subquery
   * @param indexCompatiblePredicatesProviderContext extracted information from the query, relevant to the index planning.
   * @param accessedProperties All properties that are referenced (in the head planner query)
   *                           Used to break potential ties between index leaf plans
   * @param config several internal configurations bundled in an object.
   *                           These are purely internal, and there is no way to use something other than the default when running neo4j.
   *                           Thus not relevant for caching.
   */
  case class PlannerState(
    input: QueryGraphSolverInput = QueryGraphSolverInput.empty,
    outerPlan: Option[LogicalPlan] = None,
    isInSubquery: Boolean = false,
    indexCompatiblePredicatesProviderContext: IndexCompatiblePredicatesProviderContext =
      IndexCompatiblePredicatesProviderContext.default,
    accessedProperties: Set[PropertyAccess] = Set.empty,
    config: QueryPlannerConfiguration = QueryPlannerConfiguration.default
  ) {

    val accessedAndAggregatingProperties: Set[PropertyAccess] =
      accessedProperties ++ indexCompatiblePredicatesProviderContext.aggregatingProperties

    def withLimitSelectivityConfig(cfg: LimitSelectivityConfig): PlannerState =
      copy(input = input.withLimitSelectivityConfig(cfg))

    def withAggregationProperties(properties: Set[PropertyAccess]): PlannerState =
      copy(indexCompatiblePredicatesProviderContext =
        indexCompatiblePredicatesProviderContext.copy(aggregatingProperties = properties)
      )

    def withAccessedProperties(properties: Set[PropertyAccess]): PlannerState =
      copy(accessedProperties = properties)

    def withFusedLabelInfo(newLabelInfo: LabelInfo): PlannerState =
      copy(input = input.withFusedLabelInfo(newLabelInfo))

    /**
     * When planning tails, the outer plan gives contextual information about the plan of the so-far solved
     * query graphs, which will be connected with an Apply with the tail-query graph plan.
     */
    def withOuterPlan(outerPlan: LogicalPlan): PlannerState = {
      copy(outerPlan = Some(outerPlan))
    }

    def forSubquery(): PlannerState = {
      copy(isInSubquery = true)
    }

    def withActivePlanner(planner: PlannerType): PlannerState =
      copy(input = input.copy(activePlanner = planner))

    def withConfig(newConfig: QueryPlannerConfiguration): PlannerState = {
      copy(config = newConfig)
    }

    def withUpdatedLabelInfo(plan: LogicalPlan, solveds: Solveds): PlannerState =
      copy(input = input.withUpdatedLabelInfo(plan, solveds))

    def withLastSolvedPlannerQuery(plannerQuery: SinglePlannerQuery): PlannerState = {
      val hasUpdates = indexCompatiblePredicatesProviderContext.outerPlanHasUpdates || !plannerQuery.readOnlySelf
      copy(indexCompatiblePredicatesProviderContext =
        indexCompatiblePredicatesProviderContext.copy(outerPlanHasUpdates = hasUpdates)
      )
    }
  }

}

case class LogicalPlanningContext(
  staticComponents: StaticComponents,
  settings: Settings,
  plannerState: PlannerState = PlannerState()
) {

  def withModifiedPlannerState(f: PlannerState => PlannerState): LogicalPlanningContext =
    copy(plannerState = f(plannerState))

  def withModifiedSettings(f: Settings => Settings): LogicalPlanningContext =
    copy(settings = f(settings))

  def statistics: GraphStatistics = staticComponents.planContext.statistics

  def cost: CostModel = staticComponents.metrics.cost

  def cardinality: CardinalityModel = staticComponents.metrics.cardinality

  def semanticTable: SemanticTable = staticComponents.semanticTable

  def providedOrderFactory: ProvidedOrderFactory = {
    if (settings.executionModel.providedOrderPreserving) {
      DefaultProvidedOrderFactory
    } else {
      ParallelExecutionProvidedOrderFactory
    }
  }
}
