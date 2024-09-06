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
package org.neo4j.cypher.internal.compiler

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.planPipeLine
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.prepareForCaching
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.systemPipeLine
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.CachedSimpleMetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.MetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.debug.DebugPrinter
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStats
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption
import org.neo4j.cypher.internal.options.CypherPlanVarExpandInto
import org.neo4j.cypher.internal.options.CypherStatefulShortestPlanningModeOption
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.values.virtual.MapValue

import java.time.Clock

import scala.jdk.CollectionConverters.MapHasAsJava

object CypherPlanner {

  def apply[Context <: PlannerContext](
    monitors: Monitors,
    parsingConfig: CypherParsingConfig,
    plannerConfig: CypherPlannerConfiguration,
    clock: Clock,
    internalSyntaxUsageStats: InternalSyntaxUsageStats
  ): CypherPlanner[Context] = {
    val metricsFactory = CachedSimpleMetricsFactory
    CypherPlanner(monitors, metricsFactory, parsingConfig, plannerConfig, clock, internalSyntaxUsageStats)
  }
}

case class CypherPlanner[Context <: PlannerContext](
  monitors: Monitors,
  metricsFactory: MetricsFactory,
  parsingConfig: CypherParsingConfig,
  plannerConfig: CypherPlannerConfiguration,
  clock: Clock,
  internalSyntaxUsageStats: InternalSyntaxUsageStats
) {

  private val parsing = new CypherParsing(monitors, parsingConfig, internalSyntaxUsageStats)

  def clearParserCache(): Unit = {
    parsing.clearDFACaches()
  }

  def normalizeQuery(state: BaseState, context: Context): BaseState = prepareForCaching.transform(state, context)

  def planPreparedQuery(state: BaseState, context: PlannerContext): LogicalPlanState = {
    val features: Seq[SemanticFeature] = CypherParsingConfig.getEnabledFeatures(
      parsingConfig.semanticFeatures,
      Some(context.config.targetsComposite),
      parsingConfig.queryRouterForCompositeEnabled
    )
    val pipeLine =
      if (plannerConfig.planSystemCommands)
        systemPipeLine
      else if (context.debugOptions.toStringEnabled) {
        planPipeLine(semanticFeatures = features) andThen DebugPrinter
      } else
        planPipeLine(semanticFeatures = features)

    pipeLine.transform(state, context)
  }

  def parseQuery(
    queryText: String,
    rawQueryText: String,
    cypherVersion: CypherVersion,
    notificationLogger: InternalNotificationLogger,
    plannerNameText: String = IDPPlannerName.name,
    offset: Option[InputPosition],
    tracer: CompilationPhaseTracer,
    params: MapValue,
    cancellationChecker: CancellationChecker,
    sessionDatabase: DatabaseReference
  ): BaseState = {
    parsing.parseQuery(
      queryText,
      rawQueryText,
      cypherVersion,
      notificationLogger,
      plannerNameText,
      offset,
      tracer,
      params,
      cancellationChecker,
      sessionDatabase = sessionDatabase
    )
  }

}

object CypherPlannerConfiguration {

  def fromCypherConfiguration(
    config: CypherConfiguration,
    cfg: Config,
    planSystemCommands: Boolean,
    targetsComposite: Boolean
  ): CypherPlannerConfiguration =
    new CypherPlannerConfiguration(config, cfg, planSystemCommands, targetsComposite)

  def defaults(): CypherPlannerConfiguration = {
    val cfg = Config.defaults()
    fromCypherConfiguration(
      CypherConfiguration.fromConfig(cfg),
      cfg,
      planSystemCommands = false,
      targetsComposite = false
    )
  }

  def withSettings(settings: Map[Setting[_], AnyRef]): CypherPlannerConfiguration = {
    val cfg = Config.defaults(
      settings.asJava
    )
    fromCypherConfiguration(
      CypherConfiguration.fromConfig(cfg),
      cfg,
      planSystemCommands = false,
      targetsComposite = false
    )
  }
}

/**
 * Static configuration for the planner.
 *
 * Any field below must either be a static configuration, or one that does not affect caching.
 * If you introduce a dynamic setting here, you will have to make sure it ends up in the relevant cache keys.
 */
class CypherPlannerConfiguration(
  config: CypherConfiguration,
  cfg: Config,
  val planSystemCommands: Boolean,
  val targetsComposite: Boolean
) {

  val statsDivergenceCalculator: () => StatsDivergenceCalculator = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(Seq(
      GraphDatabaseSettings.query_statistics_divergence_threshold,
      GraphDatabaseInternalSettings.query_statistics_divergence_target,
      GraphDatabaseSettings.cypher_min_replan_interval,
      GraphDatabaseInternalSettings.cypher_replan_interval_target,
      GraphDatabaseInternalSettings.cypher_replan_algorithm
    ).forall(!_.dynamic()))
    () => StatsDivergenceCalculator.divergenceCalculatorFor(config.statsDivergenceCalculator)
  }

  val useErrorsOverWarnings: () => Boolean = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(!GraphDatabaseSettings.cypher_hints_error.dynamic())
    () => config.useErrorsOverWarnings
  }

  val idpMaxTableSize: () => Int = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.cypher_idp_solver_table_threshold.dynamic()
    )
    () => config.idpMaxTableSize
  }

  val idpIterationDuration: () => Long = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.cypher_idp_solver_duration_threshold.dynamic()
    )
    () => config.idpIterationDuration
  }

  val errorIfShortestPathFallbackUsedAtRuntime: () => Boolean = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(!GraphDatabaseSettings.forbid_exhaustive_shortestpath.dynamic())
    () => config.errorIfShortestPathFallbackUsedAtRuntime
  }

  val errorIfShortestPathHasCommonNodesAtRuntime: () => Boolean = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(!GraphDatabaseSettings.forbid_shortestpath_common_nodes.dynamic())
    () => config.errorIfShortestPathHasCommonNodesAtRuntime
  }

  val legacyCsvQuoteEscaping: () => Boolean = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(!GraphDatabaseSettings.csv_legacy_quote_escaping.dynamic())
    () => config.legacyCsvQuoteEscaping
  }

  val csvBufferSize: () => Int = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(!GraphDatabaseSettings.csv_buffer_size.dynamic())
    () => config.csvBufferSize
  }

  val nonIndexedLabelWarningThreshold: () => Long = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.query_non_indexed_label_warning_threshold.dynamic()
    )
    () => cfg.get(GraphDatabaseInternalSettings.query_non_indexed_label_warning_threshold).longValue()
  }

  val pipelinedBatchSizeSmall: () => Int = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small.dynamic()
    )
    () => config.pipelinedBatchSizeSmall
  }

  val pipelinedBatchSizeBig: () => Int = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big.dynamic()
    )
    () => config.pipelinedBatchSizeBig
  }

  val planningIntersectionScansEnabled: () => Boolean = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.planning_intersection_scans_enabled.dynamic()
    )
    () => config.planningIntersectionScansEnabled
  }

  val planningSubtractionScansEnabled: () => Boolean = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.planning_subtraction_scans_enabled.dynamic()
    )
    () => config.planningSubtractionScansEnabled
  }

  val predicatesAsUnionMaxSize: () => Int = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.predicates_as_union_max_size.dynamic()
    )
    () => config.predicatesAsUnionMaxSize
  }

  val eagerAnalyzer: () => CypherEagerAnalyzerOption = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.cypher_eager_analysis_implementation.dynamic()
    )
    () => config.eagerAnalyzer
  }

  val queryRouterForCompositeQueriesEnabled: Boolean = config.allowCompositeQueries

  val statefulShortestPlanningMode: () => CypherStatefulShortestPlanningModeOption = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.stateful_shortest_planning_mode.dynamic()
    )
    () => config.statefulShortestPlanningMode
  }

  val planVarExpandInto: () => CypherPlanVarExpandInto = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.plan_var_expand_into.dynamic()
    )
    () => config.planVarExpandInto
  }

  val gpmShortestToLegacyShortestEnabled: () => Boolean = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.gpm_shortest_to_legacy_shortest_enabled.dynamic()
    )
    () => config.gpmShortestToLegacyShortestEnabled
  }

  val lpEagerFallbackEnabled: () => Boolean = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.cypher_lp_eager_analysis_fallback_enabled.dynamic()
    )
    () => config.lpEagerFallbackEnabled
  }

  val statefulShortestPlanningRewriteQuantifiersAbove: () => Int = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.stateful_shortest_planning_rewrite_quantifiers_above.dynamic()
    )
    () => config.statefulShortestPlanningRewriteQuantifiersAbove
  }

  val cachePropertiesForEntities: () => Boolean = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.planning_cache_properties_for_entities_enabled.dynamic()
    )
    () => config.cachePropertiesForEntities
  }

  val cachePropertiesForEntitiesWithFilter: () => Boolean = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.push_predicates_into_remote_batch_properties.dynamic()
    )
    () => config.cachePropertiesForEntitiesWithFilter
  }
}
