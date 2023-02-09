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
package org.neo4j.cypher.internal.compiler

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.ExtractLiteral
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.helpers.ParameterValueTypeHelper
import org.neo4j.cypher.internal.compiler.phases.BaseContextImpl
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.ParsingConfig
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.planPipeLine
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.prepareForCaching
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.systemPipeLine
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.MetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.debug.DebugPrinter
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlannerNameFor
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.graphdb.config.Setting
import org.neo4j.values.virtual.MapValue

import java.time.Clock

import scala.jdk.CollectionConverters.MapHasAsJava

case class CypherPlanner[Context <: PlannerContext](
  monitors: Monitors,
  metricsFactory: MetricsFactory,
  config: CypherPlannerConfiguration,
  updateStrategy: UpdateStrategy,
  clock: Clock
) {

  def normalizeQuery(state: BaseState, context: Context): BaseState = prepareForCaching.transform(state, context)

  def planPreparedQuery(state: BaseState, context: Context): LogicalPlanState = {
    val pipeLine =
      if (config.planSystemCommands)
        systemPipeLine
      else if (context.debugOptions.toStringEnabled)
        planPipeLine(semanticFeatures = context.config.enabledSemanticFeatures()) andThen DebugPrinter
      else
        planPipeLine(semanticFeatures = context.config.enabledSemanticFeatures())

    pipeLine.transform(state, context)
  }

  def parseQuery(
    queryText: String,
    rawQueryText: String,
    notificationLogger: InternalNotificationLogger,
    plannerNameText: String = IDPPlannerName.name,
    offset: Option[InputPosition],
    tracer: CompilationPhaseTracer,
    params: MapValue,
    cancellationChecker: CancellationChecker
  ): BaseState = {

    val plannerName = PlannerNameFor(plannerNameText)
    val startState = InitialState(queryText, offset, plannerName, new AnonymousVariableNameGenerator)
    val context = BaseContextImpl(tracer, notificationLogger, rawQueryText, offset, monitors, cancellationChecker)
    CompilationPhases.parsing(ParsingConfig(
      extractLiterals = config.extractLiterals(),
      semanticFeatures = config.enabledSemanticFeatures(),
      parameterTypeMapping = ParameterValueTypeHelper.asCypherTypeMap(params),
      obfuscateLiterals = config.obfuscateLiterals()
    )).transform(startState, context)
  }

}

object CypherPlannerConfiguration {

  def fromCypherConfiguration(
    config: CypherConfiguration,
    cfg: Config,
    planSystemCommands: Boolean
  ): CypherPlannerConfiguration =
    new CypherPlannerConfiguration(config, cfg, planSystemCommands)

  def defaults(): CypherPlannerConfiguration = {
    val cfg = Config.defaults()
    fromCypherConfiguration(CypherConfiguration.fromConfig(cfg), cfg, planSystemCommands = false)
  }

  def withSettings(settings: Map[Setting[_], AnyRef]): CypherPlannerConfiguration = {
    val cfg = Config.defaults(
      settings.asJava
    )
    fromCypherConfiguration(CypherConfiguration.fromConfig(cfg), cfg, planSystemCommands = false)
  }

}

/**
 * Static configuration for the planner.
 *
 * Any field below must either be a static configuration, or one that does not affect caching.
 * If you introduce a dynamic setting here, you will have to make sure it ends up in the relevant cache keys.
 */
class CypherPlannerConfiguration(config: CypherConfiguration, cfg: Config, val planSystemCommands: Boolean) {

  val queryCacheSize: () => Int = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(!GraphDatabaseSettings.query_cache_size.dynamic())
    () => config.queryCacheSize
  }

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

  val obfuscateLiterals: () => Boolean = {
    // Is dynamic, but documented to not affect caching.
    () => config.obfuscateLiterals
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

  val enabledSemanticFeatures: () => Seq[SemanticFeature] = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.cypher_enable_extra_semantic_features.dynamic()
    )
    () => CompilationPhases.enabledSemanticFeatures(config.enableExtraSemanticFeatures ++ showSettingFeature)
  }

  private val showSettingFeature: Set[String] = {
    if (config.showSettingFeatureEnabled)
      Set(SemanticFeature.ShowSetting.productPrefix)
    else
      Set.empty
  }

  val planningIntersectionScansEnabled: () => Boolean = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.planning_intersection_scans_enabled.dynamic()
    )
    () => config.planningIntersectionScansEnabled
  }

  val planningRelationshipUniqueIndexSeekEnabled: () => Boolean = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.planning_relationship_unique_index_seek_enabled.dynamic()
    )
    () => config.planningRelationshipUniqueIndexSeekEnabled
  }

  val planningMergeRelationshipUniqueIndexSeekEnabled: () => Boolean = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.planning_merge_relationship_unique_index_seek_enabled.dynamic()
    )
    () => config.planningMergeRelationshipUniqueIndexSeekEnabled
  }

  val predicatesAsUnionMaxSize: () => Int = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.predicates_as_union_max_size.dynamic()
    )
    () => config.predicatesAsUnionMaxSize
  }

  val extractLiterals: () => ExtractLiteral = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.extract_literals.dynamic()
    )
    () => config.extractLiterals
  }

  val useLegacyShortestPath: () => Boolean = {
    // Is dynamic, but documented to not affect caching.
    () => config.useLegacyShortestPath
  }
}
