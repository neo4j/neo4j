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
import org.neo4j.configuration.GraphDatabaseInternalSettings.RemoteBatchPropertiesImplementation
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.compiler.helpers.HistogramsFromConfigHelper
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.macros.AssertMacros3
import org.neo4j.cypher.internal.options.CypherPlanVarExpandInto
import org.neo4j.cypher.internal.options.CypherStatefulShortestPlanningModeOption
import org.neo4j.cypher.internal.planner.spi.histogram.Histogram
import org.neo4j.graphdb.config.Setting

import scala.jdk.CollectionConverters.MapHasAsJava

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
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(Seq(
      GraphDatabaseSettings.query_statistics_divergence_threshold,
      GraphDatabaseInternalSettings.query_statistics_divergence_target,
      GraphDatabaseSettings.cypher_min_replan_interval,
      GraphDatabaseInternalSettings.cypher_replan_interval_target,
      GraphDatabaseInternalSettings.cypher_replan_algorithm
    ).forall(!_.dynamic()))
    () => StatsDivergenceCalculator.divergenceCalculatorFor(config.statsDivergenceCalculator)
  }

  val useErrorsOverWarnings: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(!GraphDatabaseSettings.cypher_hints_error.dynamic())
    () => config.useErrorsOverWarnings
  }

  val histograms: Set[Histogram] = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.histogram_data.dynamic()
    )
    HistogramsFromConfigHelper.getHistogramsFromConfig(config.histogramData)
  }

  val idpMaxTableSize: () => Int = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.cypher_idp_solver_table_threshold.dynamic()
    )
    () => config.idpMaxTableSize
  }

  val idpIterationDuration: () => Long = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.cypher_idp_solver_duration_threshold.dynamic()
    )
    () => config.idpIterationDuration
  }

  val errorIfShortestPathFallbackUsedAtRuntime: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(!GraphDatabaseSettings.forbid_exhaustive_shortestpath.dynamic())
    () => config.errorIfShortestPathFallbackUsedAtRuntime
  }

  val errorIfShortestPathHasCommonNodesAtRuntime: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(!GraphDatabaseSettings.forbid_shortestpath_common_nodes.dynamic())
    () => config.errorIfShortestPathHasCommonNodesAtRuntime
  }

  val legacyCsvQuoteEscaping: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(!GraphDatabaseSettings.csv_legacy_quote_escaping.dynamic())
    () => config.legacyCsvQuoteEscaping
  }

  val csvBufferSize: () => Int = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(!GraphDatabaseSettings.csv_buffer_size.dynamic())
    () => config.csvBufferSize
  }

  val nonIndexedLabelWarningThreshold: () => Long = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.query_non_indexed_label_warning_threshold.dynamic()
    )
    () => cfg.get(GraphDatabaseInternalSettings.query_non_indexed_label_warning_threshold).longValue()
  }

  val pipelinedBatchSizeSmall: () => Int = {
    // NOTE: Dynamic setting that affects cache keys
    () => config.pipelinedBatchSizeSmall
  }

  val pipelinedBatchSizeBig: () => Int = {
    // NOTE: Dynamic setting that affects cache keys
    () => config.pipelinedBatchSizeBig
  }

  val planningIntersectionScansEnabled: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.planning_intersection_scans_enabled.dynamic()
    )
    () => config.planningIntersectionScansEnabled
  }

  val planningSubtractionScansEnabled: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.planning_subtraction_scans_enabled.dynamic()
    )
    () => config.planningSubtractionScansEnabled
  }

  val predicatesAsUnionMaxSize: () => Int = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.predicates_as_union_max_size.dynamic()
    )
    () => config.predicatesAsUnionMaxSize
  }

  val queryRouterForCompositeQueriesEnabled: Boolean = config.allowCompositeQueries

  val statefulShortestPlanningMode: () => CypherStatefulShortestPlanningModeOption = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.stateful_shortest_planning_mode.dynamic()
    )
    () => config.statefulShortestPlanningMode
  }

  val planVarExpandInto: () => CypherPlanVarExpandInto = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.plan_var_expand_into.dynamic()
    )
    () => config.planVarExpandInto
  }

  val gpmShortestToLegacyShortestEnabled: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.gpm_shortest_to_legacy_shortest_enabled.dynamic()
    )
    () => config.gpmShortestToLegacyShortestEnabled
  }

  val lpEagerFallbackEnabled: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.cypher_lp_eager_analysis_fallback_enabled.dynamic()
    )
    () => config.lpEagerFallbackEnabled
  }

  val statefulShortestPlanningRewriteQuantifiersAbove: () => Int = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.stateful_shortest_planning_rewrite_quantifiers_above.dynamic()
    )
    () => config.statefulShortestPlanningRewriteQuantifiersAbove
  }

  val cachePropertiesForEntities: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.planning_cache_properties_for_entities_enabled.dynamic()
    )
    () => config.cachePropertiesForEntities
  }

  val remoteBatchPropertiesImplementation: () => RemoteBatchPropertiesImplementation = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.cypher_remote_batch_properties_implementation.dynamic()
    )
    () => config.remoteBatchPropertiesImplementation
  }

  val pushOperatorsToRemoteBatchPropertiesEnabled: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.push_operators_into_remote_batch_properties.dynamic()
    )
    () => config.pushOperatorsToRemoteBatchPropertiesEnabled
  }

  val planningGraphSchemaOptimizationsEnabled: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.graph_type_enabled.dynamic() &&
        !GraphDatabaseInternalSettings.planning_graph_schema_optimizations_enabled.dynamic()
    )
    () => config.graphTypeEnabled && config.planningGraphSchemaOptimizationsEnabled
  }

  val optionalMatchRemoverEnabled: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.optional_match_remover_enabled.dynamic()
    )
    () => config.optionalMatchRemoverEnabled
  }

  val dynamicLabelScansEnabled: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.cypher_enable_dynamic_label_scan.dynamic()
    )
    () => config.dynamicLabelScansEnabled
  }

  val dynamicLabelIndexUseEnabled: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.cypher_enable_dynamic_label_index_use.dynamic()
    )
    () => config.dynamicLabelIndexUseEnabled
  }

  val limitBeforeCountRewriterEnabled: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.planning_limit_before_count_rewriter_enabled.dynamic()
    )
    () => config.limitBeforeCountRewriterEnabled
  }

  val existsWithImplicitLimitEnabled: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.planning_exists_with_implicit_limit_enabled.dynamic()
    )
    () => config.existsWithImplicitLimitEnabled
  }

  val selectorCandidatesMaximum: () => Int = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.planning_selector_candidates_maximum.dynamic()
    )
    () => config.selectorCandidatesMaximum
  }

  val allowDuplicatingSubqueryExpressionsInCnfNormalizer: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.allow_duplicating_subquery_expressions_in_cnf_normalizer.dynamic()
    )
    () => config.allowDuplicatingSubqueryExpressionsInCnfNormalizer
  }

  val planningMergeJoinEnabled: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.planning_merge_join_enabled.dynamic()
    )
    () => config.planningMergeJoinEnabled
  }

  val mergeOptimizationEnabled: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.merge_optimization_enabled.dynamic()
    )
    () => config.mergeOptimizationEnabled
  }

  val remoteLeafOperators: () => Boolean = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      !GraphDatabaseInternalSettings.remote_leaf_operators.dynamic()
    )
    () => config.remoteLeafOperators
  }
}
