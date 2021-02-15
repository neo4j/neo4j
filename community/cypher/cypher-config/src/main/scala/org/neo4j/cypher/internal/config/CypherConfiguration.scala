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

import java.io.File

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.options.CypherVersion

/**
 * Holds all configuration options for the Neo4j Cypher execution engine, compilers and runtimes.
 */
object CypherConfiguration {
  def fromConfig(config: Config): CypherConfiguration = {
    CypherConfiguration(
      CypherVersion.fromConfig(config),
      CypherPlannerOption.fromConfig(config),
      CypherRuntimeOption.fromConfig(config),
      config.get(GraphDatabaseSettings.query_cache_size).toInt,
      statsDivergenceFromConfig(config),
      config.get(GraphDatabaseSettings.cypher_hints_error),
      config.get(GraphDatabaseInternalSettings.cypher_idp_solver_table_threshold).toInt,
      config.get(GraphDatabaseInternalSettings.cypher_idp_solver_duration_threshold).toLong,
      config.get(GraphDatabaseSettings.forbid_exhaustive_shortestpath),
      config.get(GraphDatabaseSettings.forbid_shortestpath_common_nodes),
      config.get(GraphDatabaseSettings.csv_legacy_quote_escaping),
      config.get(GraphDatabaseSettings.csv_buffer_size).intValue(),
      CypherExpressionEngineOption.fromConfig(config),
      config.get(GraphDatabaseSettings.cypher_lenient_create_relationship),
      config.get(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small),
      config.get(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big),
      config.get(GraphDatabaseInternalSettings.enable_pipelined_runtime_trace),
      config.get(GraphDatabaseInternalSettings.pipelined_scheduler_trace_filename).toFile,
      config.get(GraphDatabaseInternalSettings.cypher_expression_recompilation_limit),
      CypherOperatorEngineOption.fromConfig(config),
      CypherInterpretedPipesFallbackOption.fromConfig(config),
      config.get(GraphDatabaseInternalSettings.cypher_pipelined_operator_fusion_over_pipeline_limit).intValue(),
      new ConfigMemoryTrackingController(config),
      config.get(GraphDatabaseInternalSettings.cypher_enable_runtime_monitors),
      config.get(GraphDatabaseInternalSettings.cypher_parser) != GraphDatabaseInternalSettings.CypherParser.PARBOILED,
      config.get(GraphDatabaseInternalSettings.cypher_splitting_top_behavior) == GraphDatabaseInternalSettings.SplittingTopBehavior.DISALLOW
    )
  }

  def statsDivergenceFromConfig(config: Config): StatsDivergenceCalculatorConfig = {
    val divergenceThreshold = config.get(GraphDatabaseSettings.query_statistics_divergence_threshold).doubleValue()
    val targetThreshold = config.get(GraphDatabaseInternalSettings.query_statistics_divergence_target).doubleValue()
    val minReplanTime = config.get(GraphDatabaseSettings.cypher_min_replan_interval).toMillis.longValue()
    val targetReplanTime = config.get(GraphDatabaseInternalSettings.cypher_replan_interval_target).toMillis.longValue()
    val divergenceAlgorithm = config.get(GraphDatabaseInternalSettings.cypher_replan_algorithm).toString
    StatsDivergenceCalculatorConfig(
      divergenceAlgorithm,
      divergenceThreshold,
      targetThreshold,
      minReplanTime,
      targetReplanTime)
  }
}

case class CypherConfiguration(
  version: CypherVersion,
  planner: CypherPlannerOption,
  runtime: CypherRuntimeOption,
  queryCacheSize: Int,
  statsDivergenceCalculator: StatsDivergenceCalculatorConfig,
  useErrorsOverWarnings: Boolean,
  idpMaxTableSize: Int,
  idpIterationDuration: Long,
  errorIfShortestPathFallbackUsedAtRuntime: Boolean,
  errorIfShortestPathHasCommonNodesAtRuntime: Boolean,
  legacyCsvQuoteEscaping: Boolean,
  csvBufferSize: Int,
  expressionEngineOption: CypherExpressionEngineOption,
  lenientCreateRelationship: Boolean,
  pipelinedBatchSizeSmall: Int,
  pipelinedBatchSizeBig: Int,
  doSchedulerTracing: Boolean,
  schedulerTracingFile: File,
  recompilationLimit: Int,
  operatorEngine: CypherOperatorEngineOption,
  interpretedPipesFallback: CypherInterpretedPipesFallbackOption,
  operatorFusionOverPipelineLimit: Int,
  memoryTrackingController: MemoryTrackingController,
  enableMonitors: Boolean,
  useJavaCCParser: Boolean,
  disallowSplittingTop: Boolean,
)
