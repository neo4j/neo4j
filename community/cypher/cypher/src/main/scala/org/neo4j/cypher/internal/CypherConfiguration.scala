/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal

import java.io.File

import org.neo4j.configuration.{Config, GraphDatabaseSettings, SettingChangeListener}
import org.neo4j.cypher._
import org.neo4j.cypher.internal.compiler.{CypherPlannerConfiguration, StatsDivergenceCalculator}
import org.neo4j.cypher.internal.runtime._

/**
  * Holds all configuration options for the Neo4j Cypher execution engine, compilers and runtimes.
  */
object CypherConfiguration {
  def fromConfig(config: Config): CypherConfiguration = {
    CypherConfiguration(
      CypherVersion(config.get(GraphDatabaseSettings.cypher_parser_version).toString),
      CypherPlannerOption(config.get(GraphDatabaseSettings.cypher_planner).toString),
      CypherRuntimeOption(config.get(GraphDatabaseSettings.cypher_runtime).toString),
      config.get(GraphDatabaseSettings.query_cache_size).toInt,
      statsDivergenceFromConfig(config),
      config.get(GraphDatabaseSettings.cypher_hints_error),
      config.get(GraphDatabaseSettings.cypher_idp_solver_table_threshold).toInt,
      config.get(GraphDatabaseSettings.cypher_idp_solver_duration_threshold).toLong,
      config.get(GraphDatabaseSettings.forbid_exhaustive_shortestpath),
      config.get(GraphDatabaseSettings.forbid_shortestpath_common_nodes),
      config.get(GraphDatabaseSettings.csv_legacy_quote_escaping),
      config.get(GraphDatabaseSettings.csv_buffer_size).intValue(),
      CypherExpressionEngineOption(config.get(GraphDatabaseSettings.cypher_expression_engine).toString),
      config.get(GraphDatabaseSettings.cypher_lenient_create_relationship),
      config.get(GraphDatabaseSettings.cypher_worker_count),
      config.get(GraphDatabaseSettings.cypher_pipelined_batch_size_small),
      config.get(GraphDatabaseSettings.cypher_pipelined_batch_size_big),
      config.get(GraphDatabaseSettings.enable_pipelined_runtime_trace),
      config.get(GraphDatabaseSettings.pipelined_scheduler_trace_filename).toFile,
      config.get(GraphDatabaseSettings.cypher_expression_recompilation_limit),
      CypherOperatorEngineOption(config.get(GraphDatabaseSettings.cypher_operator_engine).toString),
      CypherInterpretedPipesFallbackOption(config.get(GraphDatabaseSettings.cypher_pipelined_interpreted_pipes_fallback).toString),
      new ConfigMemoryTrackingController(config),
      config.get(GraphDatabaseSettings.cypher_enable_runtime_monitors)
    )
  }

  def statsDivergenceFromConfig(config: Config): StatsDivergenceCalculator = {
    val divergenceThreshold = config.get(GraphDatabaseSettings.query_statistics_divergence_threshold).doubleValue()
    val targetThreshold = config.get(GraphDatabaseSettings.query_statistics_divergence_target).doubleValue()
    val minReplanTime = config.get(GraphDatabaseSettings.cypher_min_replan_interval).toMillis.longValue()
    val targetReplanTime = config.get(GraphDatabaseSettings.cypher_replan_interval_target).toMillis.longValue()
    val divergenceAlgorithm = config.get(GraphDatabaseSettings.cypher_replan_algorithm).toString
    StatsDivergenceCalculator.divergenceCalculatorFor(divergenceAlgorithm,
                                                      divergenceThreshold,
                                                      targetThreshold,
                                                      minReplanTime,
                                                      targetReplanTime)
  }
}

class ConfigMemoryTrackingController(config: Config) extends MemoryTrackingController {

  @volatile private var _memoryTracking: MemoryTracking =
    getMemoryTracking(
      config.get(GraphDatabaseSettings.track_query_allocation),
      config.get(GraphDatabaseSettings.query_max_memory))

  override def memoryTracking: MemoryTracking = _memoryTracking

  config.addListener(GraphDatabaseSettings.track_query_allocation,
                     new SettingChangeListener[java.lang.Boolean] {
                       override def accept(before: java.lang.Boolean, after: java.lang.Boolean): Unit =
                         _memoryTracking = getMemoryTracking(after, config.get(GraphDatabaseSettings.query_max_memory))
                     })

  config.addListener(GraphDatabaseSettings.query_max_memory,
                     new SettingChangeListener[java.lang.Long] {
                       override def accept(before: java.lang.Long, after: java.lang.Long): Unit =
                        _memoryTracking = getMemoryTracking(config.get(GraphDatabaseSettings.track_query_allocation), after)
                     })

  private def getMemoryTracking(trackQueryAllocation: Boolean, queryMaxMemory: Long): MemoryTracking =
    if (trackQueryAllocation && queryMaxMemory > 0) MEMORY_BOUND(queryMaxMemory)
    else if (trackQueryAllocation) MEMORY_TRACKING
    else NO_TRACKING
}

case class CypherConfiguration(version: CypherVersion,
                               planner: CypherPlannerOption,
                               runtime: CypherRuntimeOption,
                               queryCacheSize: Int,
                               statsDivergenceCalculator: StatsDivergenceCalculator,
                               useErrorsOverWarnings: Boolean,
                               idpMaxTableSize: Int,
                               idpIterationDuration: Long,
                               errorIfShortestPathFallbackUsedAtRuntime: Boolean,
                               errorIfShortestPathHasCommonNodesAtRuntime: Boolean,
                               legacyCsvQuoteEscaping: Boolean,
                               csvBufferSize: Int,
                               expressionEngineOption: CypherExpressionEngineOption,
                               lenientCreateRelationship: Boolean,
                               workers: Int,
                               pipelinedBatchSizeSmall: Int,
                               pipelinedBatchSizeBig: Int,
                               doSchedulerTracing: Boolean,
                               schedulerTracingFile: File,
                               recompilationLimit: Int,
                               operatorEngine: CypherOperatorEngineOption,
                               interpretedPipesFallback: CypherInterpretedPipesFallbackOption,
                               memoryTrackingController: MemoryTrackingController,
                               enableMonitors: Boolean) {

  def toCypherRuntimeConfiguration: CypherRuntimeConfiguration =
    CypherRuntimeConfiguration(
      workers = workers,
      pipelinedBatchSizeSmall = pipelinedBatchSizeSmall,
      pipelinedBatchSizeBig = pipelinedBatchSizeBig,
      schedulerTracing = toSchedulerTracingConfiguration(doSchedulerTracing, schedulerTracingFile),
      lenientCreateRelationship = lenientCreateRelationship,
      memoryTrackingController = memoryTrackingController,
      enableMonitors
    )

  def toSchedulerTracingConfiguration(doSchedulerTracing: Boolean,
                                      schedulerTracingFile: File): SchedulerTracingConfiguration =
    if (doSchedulerTracing)
      if (schedulerTracingFile.getName == "stdOut") StdOutSchedulerTracing
      else FileSchedulerTracing(schedulerTracingFile)
    else NoSchedulerTracing

  def toCypherPlannerConfiguration(config: Config, planSystemCommands: Boolean): CypherPlannerConfiguration =
    CypherPlannerConfiguration(
      queryCacheSize = queryCacheSize,
      statsDivergenceCalculator = CypherConfiguration.statsDivergenceFromConfig(config),
      useErrorsOverWarnings = useErrorsOverWarnings,
      idpMaxTableSize = idpMaxTableSize,
      idpIterationDuration = idpIterationDuration,
      errorIfShortestPathFallbackUsedAtRuntime = errorIfShortestPathFallbackUsedAtRuntime,
      errorIfShortestPathHasCommonNodesAtRuntime = errorIfShortestPathHasCommonNodesAtRuntime,
      legacyCsvQuoteEscaping = legacyCsvQuoteEscaping,
      csvBufferSize = csvBufferSize,
      nonIndexedLabelWarningThreshold = config.get(GraphDatabaseSettings.query_non_indexed_label_warning_threshold).longValue(),
      planSystemCommands = planSystemCommands
    )
}
