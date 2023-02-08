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
package org.neo4j.cypher.internal.config

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.ExtractLiteral
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.config.CypherConfiguration.statsDivergenceFromConfig
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.options.CypherParallelRuntimeSupportOption
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption

import java.io.File

import scala.jdk.CollectionConverters.SetHasAsScala

/**
 * Holds all configuration options for the Neo4j Cypher execution engine, compilers and runtimes.
 */
object CypherConfiguration {
  def fromConfig(config: Config): CypherConfiguration = new CypherConfiguration(config)

  def statsDivergenceFromConfig(config: Config): StatsDivergenceCalculatorConfig = {
    val divergenceThreshold = config.get(GraphDatabaseSettings.query_statistics_divergence_threshold).doubleValue()
    val targetThreshold = config.get(GraphDatabaseInternalSettings.query_statistics_divergence_target).doubleValue()
    val minReplanInterval = config.get(GraphDatabaseSettings.cypher_min_replan_interval).toMillis.longValue()
    val targetReplanInterval =
      config.get(GraphDatabaseInternalSettings.cypher_replan_interval_target).toMillis.longValue()
    val divergenceAlgorithm = config.get(GraphDatabaseInternalSettings.cypher_replan_algorithm)
    StatsDivergenceCalculatorConfig(
      divergenceAlgorithm,
      divergenceThreshold,
      targetThreshold,
      minReplanInterval,
      targetReplanInterval
    )
  }
}

class CypherConfiguration private (val config: Config) {

  // static configurations
  val planner: CypherPlannerOption = CypherPlannerOption.fromConfig(config)
  val runtime: CypherRuntimeOption = CypherRuntimeOption.fromConfig(config)
  val queryCacheSize: Int = config.get(GraphDatabaseSettings.query_cache_size).toInt
  val executionPlanCacheSize: Int = config.get(GraphDatabaseInternalSettings.query_execution_plan_cache_size).toInt
  val statsDivergenceCalculator: StatsDivergenceCalculatorConfig = statsDivergenceFromConfig(config)
  val useErrorsOverWarnings: Boolean = config.get(GraphDatabaseSettings.cypher_hints_error)
  val idpMaxTableSize: Int = config.get(GraphDatabaseInternalSettings.cypher_idp_solver_table_threshold).toInt
  val idpIterationDuration: Long = config.get(GraphDatabaseInternalSettings.cypher_idp_solver_duration_threshold).toLong
  val predicatesAsUnionMaxSize: Int = config.get(GraphDatabaseInternalSettings.predicates_as_union_max_size)

  val errorIfShortestPathFallbackUsedAtRuntime: Boolean =
    config.get(GraphDatabaseSettings.forbid_exhaustive_shortestpath)

  val errorIfShortestPathHasCommonNodesAtRuntime: Boolean =
    config.get(GraphDatabaseSettings.forbid_shortestpath_common_nodes)
  val legacyCsvQuoteEscaping: Boolean = config.get(GraphDatabaseSettings.csv_legacy_quote_escaping)
  val csvBufferSize: Int = config.get(GraphDatabaseSettings.csv_buffer_size).intValue()
  val expressionEngineOption: CypherExpressionEngineOption = CypherExpressionEngineOption.fromConfig(config)
  val lenientCreateRelationship: Boolean = config.get(GraphDatabaseSettings.cypher_lenient_create_relationship)
  val pipelinedBatchSizeSmall: Int = config.get(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small)
  val pipelinedBatchSizeBig: Int = config.get(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big)
  val doSchedulerTracing: Boolean = config.get(GraphDatabaseInternalSettings.enable_pipelined_runtime_trace)
  val schedulerTracingFile: File = config.get(GraphDatabaseInternalSettings.pipelined_scheduler_trace_filename).toFile
  val recompilationLimit: Int = config.get(GraphDatabaseInternalSettings.cypher_expression_recompilation_limit)
  val operatorEngine: CypherOperatorEngineOption = CypherOperatorEngineOption.fromConfig(config)

  val compiledExpressionMethodLimit: Int =
    config.get(GraphDatabaseInternalSettings.cypher_expression_compiled_method_limit)
  val operatorFusingMethodLimit: Int = config.get(GraphDatabaseInternalSettings.cypher_operator_compiled_method_limit)

  val interpretedPipesFallback: CypherInterpretedPipesFallbackOption =
    CypherInterpretedPipesFallbackOption.fromConfig(config)

  val operatorFusionOverPipelineLimit: Int =
    config.get(GraphDatabaseInternalSettings.cypher_pipelined_operator_fusion_over_pipeline_limit).intValue()
  val memoryTrackingController: MemoryTrackingController = MEMORY_TRACKING_ENABLED_CONTROLLER
  val enableMonitors: Boolean = config.get(GraphDatabaseInternalSettings.cypher_enable_runtime_monitors)

  val disallowSplittingTop: Boolean = config.get(
    GraphDatabaseInternalSettings.cypher_splitting_top_behavior
  ) == GraphDatabaseInternalSettings.SplittingTopBehavior.DISALLOW

  val enableExtraSemanticFeatures: Set[String] =
    config.get(GraphDatabaseInternalSettings.cypher_enable_extra_semantic_features).asScala.toSet

  val showSettingFeatureEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.show_setting)

  val planningIntersectionScansEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.planning_intersection_scans_enabled)

  val planningRelationshipUniqueIndexSeekEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.planning_relationship_unique_index_seek_enabled)

  val planningMergeRelationshipUniqueIndexSeekEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.planning_merge_relationship_unique_index_seek_enabled)

  val useLPEagerAnalyzer: Boolean = config.get(
    GraphDatabaseInternalSettings.cypher_eager_analysis_implementation
  ) == GraphDatabaseInternalSettings.EagerAnalysisImplementation.LP

  val varExpandRelationshipIdSetThreshold: Integer =
    config.get(GraphDatabaseInternalSettings.var_expand_relationship_id_set_threshold)

  val extractLiterals: ExtractLiteral = config.get(GraphDatabaseInternalSettings.extract_literals)

  val allowSourceGeneration: Boolean = config.get(GraphDatabaseInternalSettings.cypher_allow_source_generation)

  val useParameterSizeHint: Boolean = config.get(GraphDatabaseInternalSettings.cypher_size_hint_parameters)

  // dynamic configurations
  private var _obfuscateLiterals: Boolean = config.get(GraphDatabaseSettings.log_queries_obfuscate_literals)
  private var _renderPlanDescription: Boolean = config.get(GraphDatabaseSettings.cypher_render_plan_descriptions)
  private var _legacyShortestPath: Boolean = config.get(GraphDatabaseInternalSettings.use_legacy_shortest_path)

  private var _parallelRuntimeSupport: CypherParallelRuntimeSupportOption =
    CypherParallelRuntimeSupportOption.fromConfig(config)

  config.addListener[GraphDatabaseInternalSettings.CypherParallelRuntimeSupport](
    GraphDatabaseInternalSettings.cypher_parallel_runtime_support,
    (
      _: GraphDatabaseInternalSettings.CypherParallelRuntimeSupport,
      newValue: GraphDatabaseInternalSettings.CypherParallelRuntimeSupport
    ) =>
      _parallelRuntimeSupport =
        CypherParallelRuntimeSupportOption.fromConfig(Config.newBuilder().set(
          GraphDatabaseInternalSettings.cypher_parallel_runtime_support,
          newValue
        ).build())
  )

  config.addListener[java.lang.Boolean](
    GraphDatabaseSettings.log_queries_obfuscate_literals,
    (_: java.lang.Boolean, newValue: java.lang.Boolean) => _obfuscateLiterals = newValue
  )

  config.addListener[java.lang.Boolean](
    GraphDatabaseInternalSettings.use_legacy_shortest_path,
    (_: java.lang.Boolean, newValue: java.lang.Boolean) => _legacyShortestPath = newValue
  )

  config.addListener[java.lang.Boolean](
    GraphDatabaseSettings.cypher_render_plan_descriptions,
    (_: java.lang.Boolean, newValue: java.lang.Boolean) => _renderPlanDescription = newValue
  )

  def obfuscateLiterals: Boolean = _obfuscateLiterals
  def useLegacyShortestPath: Boolean = _legacyShortestPath
  def renderPlanDescription: Boolean = _renderPlanDescription
  def parallelRuntimeSupport: CypherParallelRuntimeSupportOption = _parallelRuntimeSupport
}
