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

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.SettingChangeListener
import org.neo4j.cypher.CypherExpressionEngineOption
import org.neo4j.cypher.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.CypherOperatorEngineOption
import org.neo4j.cypher.CypherPlannerOption
import org.neo4j.cypher.CypherRuntimeOption
import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.StatsDivergenceCalculator
import org.neo4j.cypher.internal.runtime.MEMORY_TRACKING
import org.neo4j.cypher.internal.runtime.MemoryTracking
import org.neo4j.cypher.internal.runtime.MemoryTrackingController
import org.neo4j.cypher.internal.runtime.NO_TRACKING

/**
 * Holds all configuration options for the Neo4j Cypher execution engine, compilers and runtimes.
 */
object CypherConfiguration {
  def fromConfig(config: Config): CypherConfiguration = {
    CypherConfiguration(
      CypherVersion(config.get(GraphDatabaseSettings.cypher_parser_version).toString),
      CypherPlannerOption(config.get(GraphDatabaseSettings.cypher_planner).toString),
      CypherRuntimeOption(config.get(GraphDatabaseInternalSettings.cypher_runtime).toString),
      config.get(GraphDatabaseSettings.query_cache_size).toInt,
      statsDivergenceFromConfig(config),
      config.get(GraphDatabaseSettings.cypher_hints_error),
      config.get(GraphDatabaseInternalSettings.cypher_idp_solver_table_threshold).toInt,
      config.get(GraphDatabaseInternalSettings.cypher_idp_solver_duration_threshold).toLong,
      config.get(GraphDatabaseSettings.forbid_exhaustive_shortestpath),
      config.get(GraphDatabaseSettings.forbid_shortestpath_common_nodes),
      config.get(GraphDatabaseSettings.csv_legacy_quote_escaping),
      config.get(GraphDatabaseSettings.csv_buffer_size).intValue(),
      CypherExpressionEngineOption(config.get(GraphDatabaseInternalSettings.cypher_expression_engine).toString),
      config.get(GraphDatabaseSettings.cypher_lenient_create_relationship),
      config.get(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small),
      config.get(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big),
      config.get(GraphDatabaseInternalSettings.enable_pipelined_runtime_trace),
      config.get(GraphDatabaseInternalSettings.pipelined_scheduler_trace_filename).toFile,
      config.get(GraphDatabaseInternalSettings.cypher_expression_recompilation_limit),
      CypherOperatorEngineOption(config.get(GraphDatabaseInternalSettings.cypher_operator_engine).toString),
      CypherInterpretedPipesFallbackOption(config.get(GraphDatabaseInternalSettings.cypher_pipelined_interpreted_pipes_fallback).toString),
      new ConfigMemoryTrackingController(config),
      config.get(GraphDatabaseInternalSettings.cypher_enable_runtime_monitors),
      config.get(GraphDatabaseInternalSettings.cypher_parser) != GraphDatabaseInternalSettings.CypherParser.PARBOILED
    )
  }

  def statsDivergenceFromConfig(config: Config): StatsDivergenceCalculator = {
    val divergenceThreshold = config.get(GraphDatabaseSettings.query_statistics_divergence_threshold).doubleValue()
    val targetThreshold = config.get(GraphDatabaseInternalSettings.query_statistics_divergence_target).doubleValue()
    val minReplanTime = config.get(GraphDatabaseSettings.cypher_min_replan_interval).toMillis.longValue()
    val targetReplanTime = config.get(GraphDatabaseInternalSettings.cypher_replan_interval_target).toMillis.longValue()
    val divergenceAlgorithm = config.get(GraphDatabaseInternalSettings.cypher_replan_algorithm).toString
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
      config.get(GraphDatabaseSettings.track_query_allocation)
    )

  override def memoryTracking(doProfile: Boolean): MemoryTracking = if (doProfile && _memoryTracking == NO_TRACKING) {
    getMemoryTracking(trackQueryAllocation = true)
  } else {
    _memoryTracking
  }

  config.addListener(GraphDatabaseSettings.track_query_allocation,
    new SettingChangeListener[java.lang.Boolean] {
      override def accept(before: java.lang.Boolean, after: java.lang.Boolean): Unit =
        _memoryTracking = getMemoryTracking(after)
    })

  private def getMemoryTracking(trackQueryAllocation: Boolean): MemoryTracking =
    if (trackQueryAllocation) MEMORY_TRACKING
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
                               pipelinedBatchSizeSmall: Int,
                               pipelinedBatchSizeBig: Int,
                               doSchedulerTracing: Boolean,
                               schedulerTracingFile: File,
                               recompilationLimit: Int,
                               operatorEngine: CypherOperatorEngineOption,
                               interpretedPipesFallback: CypherInterpretedPipesFallbackOption,
                               memoryTrackingController: MemoryTrackingController,
                               enableMonitors: Boolean,
                               useJavaCCParser: Boolean) {

  def toCypherRuntimeConfiguration: CypherRuntimeConfiguration =
    CypherRuntimeConfiguration(
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
      nonIndexedLabelWarningThreshold = config.get(GraphDatabaseInternalSettings.query_non_indexed_label_warning_threshold).longValue(),
      planSystemCommands = planSystemCommands,
      readPropertiesFromCursor = config.get(GraphDatabaseInternalSettings.cypher_read_properties_from_cursor),
      useJavaCCParser = useJavaCCParser
    )
}
