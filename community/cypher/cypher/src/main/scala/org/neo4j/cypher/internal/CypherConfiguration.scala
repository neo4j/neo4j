/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.cypher.internal.compiler.v3_5.StatsDivergenceCalculator
import org.neo4j.cypher.{CypherPlannerOption, CypherRuntimeOption, CypherVersion}
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.configuration.Config

/**
  * Holds all configuration options for the Neo4j Cypher execution engine, compilers and runtimes.
  */
object CypherConfiguration {

  def fromConfig(config: Config): CypherConfiguration = {
    CypherConfiguration(
      CypherVersion(config.get(GraphDatabaseSettings.cypher_parser_version)),
      CypherPlannerOption(config.get(GraphDatabaseSettings.cypher_planner)),
      CypherRuntimeOption(config.get(GraphDatabaseSettings.cypher_runtime)),
      config.get(GraphDatabaseSettings.query_cache_size).toInt,
      statsDivergenceFromConfig(config),
      config.get(GraphDatabaseSettings.cypher_hints_error),
      config.get(GraphDatabaseSettings.cypher_idp_solver_table_threshold).toInt,
      config.get(GraphDatabaseSettings.cypher_idp_solver_duration_threshold).toLong,
      config.get(GraphDatabaseSettings.forbid_exhaustive_shortestpath),
      config.get(GraphDatabaseSettings.forbid_shortestpath_common_nodes),
      config.get(GraphDatabaseSettings.csv_legacy_quote_escaping),
      config.get(GraphDatabaseSettings.csv_buffer_size),
      config.get(GraphDatabaseSettings.cypher_plan_with_minimum_cardinality_estimates)
    )
  }

  def statsDivergenceFromConfig(config: Config): StatsDivergenceCalculator = {
    val divergenceThreshold = config.get(GraphDatabaseSettings.query_statistics_divergence_threshold).doubleValue()
    val targetThreshold = config.get(GraphDatabaseSettings.query_statistics_divergence_target).doubleValue()
    val minReplanTime = config.get(GraphDatabaseSettings.cypher_min_replan_interval).toMillis().longValue()
    val targetReplanTime = config.get(GraphDatabaseSettings.cypher_replan_interval_target).toMillis().longValue()
    val divergenceAlgorithm = config.get(GraphDatabaseSettings.cypher_replan_algorithm)
    StatsDivergenceCalculator.divergenceCalculatorFor(divergenceAlgorithm,
                                                      divergenceThreshold,
                                                      targetThreshold,
                                                      minReplanTime,
                                                      targetReplanTime)
  }


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
                               planWithMinimumCardinalityEstimates: Boolean)
