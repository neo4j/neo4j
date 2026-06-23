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
package org.neo4j.cypher.internal.config

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.ExtractLiteral
import org.neo4j.configuration.GraphDatabaseInternalSettings.RemoteBatchPropertiesImplementation
import org.neo4j.configuration.GraphDatabaseInternalSettings.extractCustomHeapEstimatorCacheConfig
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.config.CypherConfiguration.statsDivergenceFromConfig
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherHeapEstimatorCacheOption
import org.neo4j.cypher.internal.options.CypherInferSchemaPartsOption
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.options.CypherParallelRuntimeConfigOption
import org.neo4j.cypher.internal.options.CypherParallelRuntimeSupportOption
import org.neo4j.cypher.internal.options.CypherPipelinedBatchReuseOption
import org.neo4j.cypher.internal.options.CypherPipelinedBatchSizePresetOption
import org.neo4j.cypher.internal.options.CypherPlanVarExpandInto
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherPlannerVersionOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.options.CypherStatefulShortestPlanningModeOption
import org.neo4j.cypher.internal.options.CypherTransactionBatchStrategyOption
import org.neo4j.graphdb.config.Setting
import org.neo4j.memory.HeapEstimatorCacheConfig

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
  def enableExperimentalCypherVersions: Boolean =
    config.get(GraphDatabaseInternalSettings.enable_experimental_cypher_versions).booleanValue()

  def enableScopeQueries: Boolean =
    config.get(GraphDatabaseInternalSettings.cypher_enable_scope_queries).booleanValue()

  /**
   * The system default query language, NOT the actual language to use.
   * The final resolved language for a query depend on:
   * - The pre-parser options.
   * - The database default language (persisted in system db).
   * - The system default language setting, `db.query.default_language` (this value).
   */
  val systemDefaultLanguage: CypherVersion = config.get(GraphDatabaseSettings.default_language) match {
    case GraphDatabaseSettings.CypherVersion.Cypher5  => CypherVersion.Cypher5
    case GraphDatabaseSettings.CypherVersion.Cypher25 => CypherVersion.Cypher25
  }

  val planner: CypherPlannerOption = CypherPlannerOption.fromConfig(config)
  val runtime: CypherRuntimeOption = CypherRuntimeOption.fromConfig(config)
  val queryCacheSize: ObservableSetting[Integer] = new ObservableSetting(config, GraphDatabaseSettings.query_cache_size)

  val softQueryCacheEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.cypher_soft_cache_enabled).booleanValue()

  val queryCacheStrongSize: ObservableSetting[Integer] =
    new ObservableSetting[Integer](config, GraphDatabaseInternalSettings.query_cache_strong_size)

  val queryCacheSoftSize: ObservableSetting[Integer] =
    new ObservableSetting(config, GraphDatabaseInternalSettings.query_cache_soft_size)

  def queryCacheMaxQueryTextSize: Long =
    config.get(GraphDatabaseInternalSettings.query_cache_max_query_text_size).longValue()
  def queryCacheMaxAstSize: Long = config.get(GraphDatabaseInternalSettings.query_cache_max_ast_size).longValue()

  def queryCacheMaxLogicalPlanSize: Long =
    config.get(GraphDatabaseInternalSettings.query_cache_max_logical_plan_size).longValue()
  val executionPlanCacheSize: Int = config.get(GraphDatabaseInternalSettings.query_execution_plan_cache_size).toInt
  val statsDivergenceCalculator: StatsDivergenceCalculatorConfig = statsDivergenceFromConfig(config)
  val useErrorsOverWarnings: Boolean = config.get(GraphDatabaseSettings.cypher_hints_error).booleanValue()
  val idpMaxTableSize: Int = config.get(GraphDatabaseInternalSettings.cypher_idp_solver_table_threshold).toInt
  val idpIterationDuration: Long = config.get(GraphDatabaseInternalSettings.cypher_idp_solver_duration_threshold).toLong
  val predicatesAsUnionMaxSize: Int = config.get(GraphDatabaseInternalSettings.predicates_as_union_max_size).intValue()

  val allowCompositeQueries: Boolean =
    config.get(GraphDatabaseInternalSettings.composite_queries_with_query_router).booleanValue()

  val gpmShortestToLegacyShortestEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.gpm_shortest_to_legacy_shortest_enabled).booleanValue()

  val labelInference: CypherInferSchemaPartsOption = CypherInferSchemaPartsOption.fromConfig(config)

  val uuidTypeEnabled: Boolean = config.get(GraphDatabaseInternalSettings.cypher_uuid_type_enabled).booleanValue()

  val groupByClauseEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.cypher_group_by_clause_enabled).booleanValue()

  val statefulShortestPlanningMode: CypherStatefulShortestPlanningModeOption =
    CypherStatefulShortestPlanningModeOption.fromConfig(config)

  val planVarExpandInto: CypherPlanVarExpandInto =
    CypherPlanVarExpandInto.fromConfig(config)

  val plannerVersion: CypherPlannerVersionOption =
    CypherPlannerVersionOption.fromConfig(config)

  val errorIfShortestPathFallbackUsedAtRuntime: Boolean =
    config.get(GraphDatabaseSettings.forbid_exhaustive_shortestpath).booleanValue()

  val errorIfShortestPathHasCommonNodesAtRuntime: Boolean =
    config.get(GraphDatabaseSettings.forbid_shortestpath_common_nodes).booleanValue()
  val legacyCsvQuoteEscaping: Boolean = config.get(GraphDatabaseSettings.csv_legacy_quote_escaping).booleanValue()
  val csvBufferSize: Int = config.get(GraphDatabaseSettings.csv_buffer_size).intValue()
  val expressionEngineOption: CypherExpressionEngineOption = CypherExpressionEngineOption.fromConfig(config)

  val lenientCreateRelationship: Boolean =
    config.get(GraphDatabaseSettings.cypher_lenient_create_relationship).booleanValue()

  def pipelinedBatchSizePreset: CypherPipelinedBatchSizePresetOption =
    CypherPipelinedBatchSizePresetOption.fromConfig(config)

  def pipelinedBatchSizeSmall: Int =
    config.get(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small).intValue()
  def pipelinedBatchSizeBig: Int = config.get(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big).intValue()
  def pipelinedBatchReuse: CypherPipelinedBatchReuseOption = CypherPipelinedBatchReuseOption.fromConfig(config)

  def pipelinedTopOperatorMemoryTrackingStrategyThreshold: Long =
    config.get(
      GraphDatabaseInternalSettings.cypher_pipelined_memory_top_operator_memory_tracking_strategy_threshold
    ).longValue()

  def transactionsDefaultBatchStrategy: CypherTransactionBatchStrategyOption =
    CypherTransactionBatchStrategyOption.fromConfig(config)

  val doSchedulerTracing: Boolean =
    config.get(GraphDatabaseInternalSettings.enable_pipelined_runtime_trace).booleanValue()
  val schedulerTracingFile: File = config.get(GraphDatabaseInternalSettings.pipelined_scheduler_trace_filename).toFile

  val recompilationLimit: Int =
    config.get(GraphDatabaseInternalSettings.cypher_expression_recompilation_limit).intValue()
  val operatorEngine: CypherOperatorEngineOption = CypherOperatorEngineOption.fromConfig(config)

  val compiledExpressionMethodLimit: Int =
    config.get(GraphDatabaseInternalSettings.cypher_expression_compiled_method_limit).intValue()

  val operatorFusingMethodLimit: Int =
    config.get(GraphDatabaseInternalSettings.cypher_operator_compiled_method_limit).intValue()

  val interpretedPipesFallback: CypherInterpretedPipesFallbackOption =
    CypherInterpretedPipesFallbackOption.fromConfig(config)

  val operatorFusionOverPipelineLimit: Int =
    config.get(GraphDatabaseInternalSettings.cypher_pipelined_operator_fusion_over_pipeline_limit).intValue()

  val operatorFusionLowerLimit: Int =
    config.get(GraphDatabaseInternalSettings.cypher_pipelined_operator_fusion_lower_limit).intValue()

  val memoryTrackingController: MemoryTrackingController =
    if (
      runtime == CypherRuntimeOption.parallel && !config.get(
        GraphDatabaseInternalSettings.enable_parallel_runtime_memory_tracking
      )
    ) {
      MEMORY_TRACKING_DISABLED_CONTROLLER
    } else {
      MEMORY_TRACKING_ENABLED_CONTROLLER
    }

  val enableMonitors: Boolean = config.get(GraphDatabaseInternalSettings.cypher_enable_runtime_monitors).booleanValue()

  val enableQueryCacheMonitors: Boolean =
    config.get(GraphDatabaseInternalSettings.cypher_enable_query_cache_monitors).booleanValue()

  val enableExtraSemanticFeatures: Set[String] =
    config.get(GraphDatabaseInternalSettings.cypher_enable_extra_semantic_features).asScala.toSet

  val planningIntersectionScansEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.planning_intersection_scans_enabled).booleanValue()

  val planningSubtractionScansEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.planning_subtraction_scans_enabled).booleanValue()

  val varExpandRelationshipIdSetThreshold: Integer =
    config.get(GraphDatabaseInternalSettings.var_expand_relationship_id_set_threshold)

  val extractLiterals: ExtractLiteral = config.get(GraphDatabaseInternalSettings.extract_literals)

  val allowSourceGeneration: Boolean =
    config.get(GraphDatabaseInternalSettings.cypher_allow_source_generation).booleanValue()

  val pipelinedSubqueryTransactionRetryEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.cypher_pipelined_subquery_transaction_retry_enabled).booleanValue()

  val useParameterSizeHint: Boolean =
    config.get(GraphDatabaseInternalSettings.cypher_size_hint_parameters).booleanValue()

  val resolveSimpleDynamicExpressions: Boolean =
    config.get(GraphDatabaseInternalSettings.resolve_simple_dynamic_expressions).booleanValue()

  val freeMemoryOfUnusedColumns: Boolean =
    config.get(GraphDatabaseInternalSettings.cypher_free_memory_of_unused_columns).booleanValue()

  val warnOnAggregationSkipNull: Boolean =
    config.get(GraphDatabaseInternalSettings.cypher_warn_on_aggregation_skip_null).booleanValue()

  val lpEagerFallbackEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.cypher_lp_eager_analysis_fallback_enabled).booleanValue()

  val statefulShortestPlanningRewriteQuantifiersAbove: Int =
    config.get(GraphDatabaseInternalSettings.stateful_shortest_planning_rewrite_quantifiers_above).intValue()

  val shardedPropertyBatchSize: Int =
    config.get(GraphDatabaseInternalSettings.sharded_property_database_batch_size).intValue()

  val cachePropertiesForEntities: Boolean =
    config.get(GraphDatabaseInternalSettings.planning_cache_properties_for_entities_enabled).booleanValue()

  val remoteBatchPropertiesImplementation: RemoteBatchPropertiesImplementation =
    config.get(GraphDatabaseInternalSettings.cypher_remote_batch_properties_implementation)

  val pushOperatorsToRemoteBatchPropertiesEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.push_operators_into_remote_batch_properties).booleanValue()

  val parallel_runtime_config: CypherParallelRuntimeConfigOption =
    CypherParallelRuntimeConfigOption.fromConfig(config)

  val histogramData: java.util.Set[java.util.Map[String, String]] =
    config.get(GraphDatabaseInternalSettings.histogram_data)

  val planningGraphSchemaOptimizationsEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.planning_graph_schema_optimizations_enabled).booleanValue()

  val graphTypeEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.graph_type_enabled).booleanValue()

  val optionalMatchRemoverEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.optional_match_remover_enabled).booleanValue()

  val dynamicLabelScansEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.cypher_enable_dynamic_label_scan).booleanValue()

  val dynamicLabelIndexUseEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.cypher_enable_dynamic_label_index_use).booleanValue()

  val enableNonFusedMerge: Boolean =
    config.get(GraphDatabaseInternalSettings.cypher_enable_non_fused_merge).booleanValue()

  val limitBeforeCountRewriterEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.planning_limit_before_count_rewriter_enabled).booleanValue()

  val existsWithImplicitLimitEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.planning_exists_with_implicit_limit_enabled).booleanValue()

  val selectorCandidatesMaximum: Int =
    config.get(GraphDatabaseInternalSettings.planning_selector_candidates_maximum).intValue()

  val allowDuplicatingSubqueryExpressionsInCnfNormalizer: Boolean =
    config.get(GraphDatabaseInternalSettings.allow_duplicating_subquery_expressions_in_cnf_normalizer).booleanValue()

  val planningMergeJoinEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.planning_merge_join_enabled).booleanValue()

  val mergeOptimizationEnabled: Boolean =
    config.get(GraphDatabaseInternalSettings.merge_optimization_enabled).booleanValue()

  val remoteLeafOperators: Boolean =
    config.get(GraphDatabaseInternalSettings.remote_leaf_operators).booleanValue()

  val displayPlannerVersion: Boolean =
    config.get(GraphDatabaseInternalSettings.display_planner_version).booleanValue()

  val useVirtualGraph: Boolean =
    if (config.getDeclaredSettings.containsKey("internal.virtual_graph.enabled")) {
      val setting = config.getSetting("internal.virtual_graph.enabled")
      java.lang.Boolean.TRUE == config.get(setting)
    } else {
      false
    }

  val exposeFullyObfuscatedQueryView: Boolean =
    config.get(GraphDatabaseInternalSettings.expose_fully_obfuscated_query_view).booleanValue()

  // dynamic configurations
  private var _obfuscateLiterals: Boolean =
    config.get(GraphDatabaseSettings.log_queries_obfuscate_literals).booleanValue()

  private var _renderPlanDescription: Boolean =
    config.get(GraphDatabaseSettings.cypher_render_plan_descriptions).booleanValue()

  private var _parallelRuntimeSupport: CypherParallelRuntimeSupportOption =
    CypherParallelRuntimeSupportOption.fromConfig(config)

  private var _customHeapEstimatorCacheConfig: HeapEstimatorCacheConfig =
    GraphDatabaseInternalSettings.extractCustomHeapEstimatorCacheConfig(config)

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
    GraphDatabaseSettings.cypher_render_plan_descriptions,
    (_: java.lang.Boolean, newValue: java.lang.Boolean) => _renderPlanDescription = newValue
  )

  // Heap estimator cache config
  config.addListener[java.lang.Integer](
    GraphDatabaseInternalSettings.heap_estimator_cache_size_limit,
    (_: java.lang.Integer, _: java.lang.Integer) =>
      _customHeapEstimatorCacheConfig = extractCustomHeapEstimatorCacheConfig(config)
  )

  config.addListener[java.lang.Long](
    GraphDatabaseInternalSettings.heap_estimator_cache_large_object_threshold,
    (_: java.lang.Long, _: java.lang.Long) =>
      _customHeapEstimatorCacheConfig = extractCustomHeapEstimatorCacheConfig(config)
  )

  def toggledFeatures(defaultFeatures: Seq[String], features: (Setting[java.lang.Boolean], String)*): Set[String] = {
    val (toggledOnFeatures, toggledOffFeatures) = features.partitionMap {
      case (setting, name) if config.get(setting).booleanValue() => Left(name)
      case (_, name)                                             => Right(name)
    }
    (defaultFeatures.diff(toggledOffFeatures) ++ toggledOnFeatures).toSet

  }

  def obfuscateLiterals: Boolean = _obfuscateLiterals
  def renderPlanDescription: Boolean = _renderPlanDescription
  def parallelRuntimeSupport: CypherParallelRuntimeSupportOption = _parallelRuntimeSupport

  def heapEstimatorCacheOption: CypherHeapEstimatorCacheOption = CypherHeapEstimatorCacheOption.fromConfig(config)
  def customHeapEstimatorCacheConfig: HeapEstimatorCacheConfig = _customHeapEstimatorCacheConfig
}
