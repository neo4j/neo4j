/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.cypher.CypherVersion._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compiler.v2_3.InputPosition
import org.neo4j.cypher.internal.compiler.v2_2.{ConservativePlannerName => ConservativePlanner2_2, IDPPlannerName => IDPPlanner2_2, DPPlannerName => DPPlanner2_2,  CostPlannerName => CostPlanner2_2}
import org.neo4j.cypher.internal.compiler.v2_3.{DPPlannerName, RulePlannerName, CostPlannerName, IDPPlannerName, ConservativePlannerName, PlannerName, InternalNotificationLogger, RecordingNotificationLogger, devNullLogger}
import org.neo4j.cypher.internal.compiler.v2_2.{ConservativePlannerName => ConservativePlanner2_2, CostPlannerName => CostPlanner2_2, IDPPlannerName => IDPPlanner2_2}
import org.neo4j.cypher.internal.compiler.v2_3.{ConservativePlannerName, CostPlannerName, DPPlannerName, IDPPlannerName, InputPosition, InternalNotificationLogger, PlannerName, RecordingNotificationLogger, RulePlannerName, devNullLogger, _}
import org.neo4j.cypher.{InvalidArgumentException, InvalidSemanticsException, SyntaxException, _}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.helpers.Clock
import org.neo4j.kernel.InternalAbstractGraphDatabase
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.impl.util.StringLogger
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

object CypherCompiler {
  val DEFAULT_QUERY_CACHE_SIZE: Int = 128
  val DEFAULT_QUERY_PLAN_TTL: Long = 1000 // 1 second
  val CLOCK = Clock.SYSTEM_CLOCK
  val STATISTICS_DIVERGENCE_THRESHOLD = 0.5

  def notificationLoggerBuilder(executionMode: ExecutionMode): InternalNotificationLogger = executionMode  match {
      case ExplainMode => new RecordingNotificationLogger()
      case _ => devNullLogger
    }
}

case class PreParsedQuery(statement: String, version: CypherVersion, executionMode: ExecutionMode, planner: PlannerName, runtime: RuntimeName)
                         (val offset: InputPosition) {
  val statementWithVersionAndPlanner = s"CYPHER ${version.name} planner=${planner.name} runtime=${runtime.name} $statement"
}


class CypherCompiler(graph: GraphDatabaseService,
                     kernelAPI: KernelAPI,
                     kernelMonitors: KernelMonitors,
                     defaultVersion: CypherVersion,
                     defaultPlanner: PlannerName,
                     defaultRuntime: RuntimeName,
                     optionParser: CypherOptionParser,
                     logger: StringLogger) {
  import org.neo4j.cypher.internal.CypherCompiler._

  private val queryCacheSize: Int = getQueryCacheSize
  private val queryPlanTTL: Long = getMinimumTimeBeforeReplanning

  private val compatibilityFor1_9 = CompatibilityFor1_9(graph, queryCacheSize, kernelMonitors)

  private val compatibilityFor2_0 = CompatibilityFor2_0(graph, queryCacheSize, kernelMonitors)

  private val compatibilityFor2_1 = CompatibilityFor2_1(graph, queryCacheSize, kernelMonitors, kernelAPI)

  private val compatibilityFor2_2Rule = CompatibilityFor2_2Rule(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI)
  private val compatibilityFor2_2Cost = CompatibilityFor2_2Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, CostPlanner2_2)
  private val compatibilityFor2_2IDP = CompatibilityFor2_2Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, IDPPlanner2_2)
  private val compatibilityFor2_2DP = CompatibilityFor2_2Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, DPPlanner2_2)
  private val compatibilityFor2_2 = CompatibilityFor2_2Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, ConservativePlanner2_2)

  private val compatibilityFor2_3Rule = CompatibilityFor2_3Rule(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, notificationLoggerBuilder)
  private val compatibilityFor2_3CostInterpreted = CompatibilityFor2_3Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, notificationLoggerBuilder, CostPlannerName, InterpretedRuntimeName)
  private val compatibilityFor2_3IDPInterpreted = CompatibilityFor2_3Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, notificationLoggerBuilder, IDPPlannerName, InterpretedRuntimeName)
  private val compatibilityFor2_3DPInterpreted = CompatibilityFor2_3Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, notificationLoggerBuilder, DPPlannerName, InterpretedRuntimeName)
  private val compatibilityFor2_3CostCompiled = CompatibilityFor2_3Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, notificationLoggerBuilder, CostPlannerName, CompiledRuntimeName)
  private val compatibilityFor2_3IDPCompiled = CompatibilityFor2_3Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, notificationLoggerBuilder, IDPPlannerName, CompiledRuntimeName)
  private val compatibilityFor2_3DPCompiled = CompatibilityFor2_3Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, notificationLoggerBuilder, DPPlannerName, CompiledRuntimeName)
  private val compatibilityFor2_3ConservativeCompiled = CompatibilityFor2_3Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, notificationLoggerBuilder, ConservativePlannerName, CompiledRuntimeName)

  private val compatibilityFor2_3 = CompatibilityFor2_3Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, notificationLoggerBuilder, ConservativePlannerName, InterpretedRuntimeName)

  private final val VERSIONS_WITH_FIXED_PLANNER: Set[CypherVersion] = Set(v1_9, v2_0, v2_1)
  private final val VERSIONS_WITH_FIXED_RUNTIME: Set[CypherVersion] = Set(v1_9, v2_0, v2_1, v2_2)

  private final val ILLEGAL_PLANNER_RUNTIME_COMBINATIONS: Set[(PlannerName, RuntimeName)] = Set((RulePlannerName, CompiledRuntimeName))

  @throws(classOf[SyntaxException])
  def preParseQuery(queryText: String): PreParsedQuery = {
    val queryWithOptions = optionParser(queryText)
    val preParsedQuery = preParse(queryWithOptions)
    preParsedQuery
  }

  @throws(classOf[SyntaxException])
  def parseQuery(preParsedQuery: PreParsedQuery): ParsedQuery = {
    val version = preParsedQuery.version
    val planner = preParsedQuery.planner
    val statementAsText = preParsedQuery.statement
    val offset = preParsedQuery.offset

    val runtime = preParsedQuery.runtime

    (version, planner, runtime) match {
      case (CypherVersion.v2_3, ConservativePlannerName, CompiledRuntimeName)    => compatibilityFor2_3ConservativeCompiled.produceParsedQuery(preParsedQuery, offset)
      case (CypherVersion.v2_3, CostPlannerName, CompiledRuntimeName)            => compatibilityFor2_3CostCompiled.produceParsedQuery(preParsedQuery, offset)
      case (CypherVersion.v2_3, IDPPlannerName, CompiledRuntimeName)             => compatibilityFor2_3IDPCompiled.produceParsedQuery(preParsedQuery, offset)
      case (CypherVersion.v2_3, DPPlannerName, CompiledRuntimeName)              => compatibilityFor2_3DPCompiled.produceParsedQuery(preParsedQuery, offset)
      case (CypherVersion.v2_3, ConservativePlannerName, InterpretedRuntimeName) => compatibilityFor2_3.produceParsedQuery(preParsedQuery, offset)
      case (CypherVersion.v2_3, CostPlannerName, InterpretedRuntimeName)         => compatibilityFor2_3CostInterpreted.produceParsedQuery(preParsedQuery, offset)
      case (CypherVersion.v2_3, IDPPlannerName, InterpretedRuntimeName)          => compatibilityFor2_3IDPInterpreted.produceParsedQuery(preParsedQuery, offset)
      case (CypherVersion.v2_3, DPPlannerName, InterpretedRuntimeName)           => compatibilityFor2_3DPInterpreted.produceParsedQuery(preParsedQuery, offset)
      case (CypherVersion.v2_3, RulePlannerName, _)                              => compatibilityFor2_3Rule.produceParsedQuery(preParsedQuery, offset)
      case (CypherVersion.v2_2, ConservativePlannerName, _)                      => compatibilityFor2_2.produceParsedQuery(statementAsText)
      case (CypherVersion.v2_2, CostPlannerName, _)                              => compatibilityFor2_2Cost.produceParsedQuery(statementAsText)
      case (CypherVersion.v2_2, IDPPlannerName, _)                               => compatibilityFor2_2IDP.produceParsedQuery(statementAsText)
      case (CypherVersion.v2_2, DPPlannerName, _)                                => compatibilityFor2_2DP.produceParsedQuery(statementAsText)
      case (CypherVersion.v2_2, RulePlannerName, _)                              => compatibilityFor2_2Rule.produceParsedQuery(statementAsText)
      case (CypherVersion.v2_2, _, _)                                            => compatibilityFor2_2.produceParsedQuery(statementAsText)
      case (CypherVersion.v2_1, _, _)                                            => compatibilityFor2_1.parseQuery(preParsedQuery.statement)
      case (CypherVersion.v2_0, _, _)                                            => compatibilityFor2_0.parseQuery(preParsedQuery.statement)
      case (CypherVersion.v1_9, _, _)                                            => compatibilityFor1_9.parseQuery(preParsedQuery.statement)
    }
  }

  private def preParse(queryWithOption: CypherQueryWithOptions): PreParsedQuery = {
    val cypherOptions = queryWithOption.options.collectFirst {
      case opt: ConfigurationOptions => opt
    }
    val cypherVersion = cypherOptions.flatMap(_.version)
      .map(v => CypherVersion(v.version))
      .getOrElse(defaultVersion)
    val planner = calculatePlanner(cypherOptions, queryWithOption.options, cypherVersion)
    val executionMode: ExecutionMode = calculateExecutionMode(queryWithOption.options)
    if (executionMode == ExplainMode && VERSIONS_WITH_FIXED_PLANNER(cypherVersion)) {
      throw new InvalidArgumentException("EXPLAIN not supported in versions older than Neo4j v2.2")
    }
    val runtime = calculateRuntime(cypherOptions, planner, cypherVersion)

    PreParsedQuery(queryWithOption.statement, cypherVersion, executionMode, planner, runtime)(queryWithOption.offset)
  }

  private def calculateExecutionMode(options: Seq[CypherOption]) = {
    val executionModes: Seq[ExecutionMode] = options.collect {
      case ExplainOption => ExplainMode
      case ProfileOption => ProfileMode
    }

    executionModes.reduceOption(_ combineWith _).getOrElse(NormalMode)
  }

  private def calculatePlanner(options: Option[ConfigurationOptions], other: Seq[CypherOption], version: CypherVersion) = {
    val planner = options.map(_.options.collect {
          case CostPlannerOption => CostPlannerName
          case RulePlannerOption => RulePlannerName
          case IDPPlannerOption => IDPPlannerName
          case DPPlannerOption => DPPlannerName
        }.distinct).getOrElse(Seq.empty)

    if (VERSIONS_WITH_FIXED_PLANNER(version) && planner.nonEmpty) {
      throw new InvalidArgumentException("PLANNER not supported in versions older than Neo4j v2.2")
    }

    if (planner.size > 1) {
      throw new InvalidSemanticsException("Can't use multiple planners")
    }

    //TODO once the we have removed PLANNER X syntax, change to defaultPlanner here
    if (planner.isEmpty) calculatePlannerDeprecated(other, version) else planner.head
  }

  @deprecated
  private def calculatePlannerDeprecated( options: Seq[CypherOption], version: CypherVersion) = {
    val planner = options.collect {
      case CostPlannerOption => CostPlannerName
      case RulePlannerOption => RulePlannerName
      case IDPPlannerOption => IDPPlannerName
      case DPPlannerOption => DPPlannerName
    }.distinct


    if (VERSIONS_WITH_FIXED_PLANNER(version) && planner.nonEmpty) {
      throw new InvalidArgumentException("PLANNER not supported in versions older than Neo4j v2.2")
    }

    if (planner.size > 1) {
      throw new InvalidSemanticsException("Can't use multiple planners")
    }

    if (planner.isEmpty) defaultPlanner else planner.head
  }

  private def calculateRuntime(options: Option[ConfigurationOptions], planner: PlannerName, version: CypherVersion) = {
    val runtimes = options.map(_.options.collect {
      case InterpretedRuntimeOption => InterpretedRuntimeName
      case CompiledRuntimeOption => CompiledRuntimeName
    }.distinct).getOrElse(Seq.empty)

    if (VERSIONS_WITH_FIXED_RUNTIME(version) && runtimes.nonEmpty) {
      throw new InvalidArgumentException("RUNTIME not supported in versions older than Neo4j v2.3")
    }

    if (runtimes.size > 1) {
      throw new InvalidSemanticsException("Can't use multiple runtimes")
    }

    val runtime = if (runtimes.isEmpty) defaultRuntime else runtimes.head

    if (ILLEGAL_PLANNER_RUNTIME_COMBINATIONS((planner, runtime))) {
      throw new InvalidArgumentException(s"Unsupported PLANNER - RUNTIME combination: ${planner.name} - ${runtime.name}")
    }

    runtime
  }

  private def getQueryCacheSize : Int =
    optGraphAs[InternalAbstractGraphDatabase]
      .andThen(_.getConfig.get(GraphDatabaseSettings.query_cache_size).intValue())
      .applyOrElse(graph, (_: GraphDatabaseService) => DEFAULT_QUERY_CACHE_SIZE)


  private def getMinimumTimeBeforeReplanning: Long = {
    optGraphAs[InternalAbstractGraphDatabase]
      .andThen(_.getConfig.get(GraphDatabaseSettings.cypher_min_replan_interval).longValue())
      .applyOrElse(graph, (_: GraphDatabaseService) => DEFAULT_QUERY_PLAN_TTL)
  }


  private def optGraphAs[T <: GraphDatabaseService : Manifest]: PartialFunction[GraphDatabaseService, T] = {
    case (db: T) => db
  }
}
