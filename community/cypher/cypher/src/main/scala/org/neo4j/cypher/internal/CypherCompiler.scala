/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.InvalidArgumentException
import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compiler.v2_2._
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
  val DEFAULT_STATISTICS_DIVERGENCE_THRESHOLD = 0.5
}

case class PreParsedQuery(statement: String, rawStatement: String, version: CypherVersion, executionMode: ExecutionMode, planner: PlannerName)
                         (val offset: InputPosition) {
  val statementWithVersionAndPlanner = {
    val plannerInfo = planner match {
      case ConservativePlannerName => ""
      case _ => s" PLANNER ${planner.name}"
    }
    s"CYPHER ${version.name}$plannerInfo $statement"
  }
}

class CypherCompiler(graph: GraphDatabaseService,
                     kernelAPI: KernelAPI,
                     kernelMonitors: KernelMonitors,
                     defaultVersion: CypherVersion,
                     defaultPlanner: PlannerName,
                     optionParser: CypherOptionParser,
                     logger: StringLogger) {
  import org.neo4j.cypher.internal.CypherCompiler._

  private val queryCacheSize: Int = getQueryCacheSize
  private val queryPlanTTL: Long = getMinimumTimeBeforeReplanning
  private val statisticsDivergenceThreshold = getStatisticsDivergenceThreshold
  private val compatibilityFor1_9 = CompatibilityFor1_9(graph, queryCacheSize, kernelMonitors)
  private val compatibilityFor2_0 = CompatibilityFor2_0(graph, queryCacheSize, kernelMonitors)
  private val compatibilityFor2_1 = CompatibilityFor2_1(graph, queryCacheSize, kernelMonitors, kernelAPI)
  private val compatibilityFor2_2Rule = CompatibilityFor2_2Rule(graph, queryCacheSize, statisticsDivergenceThreshold, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI)
  private val compatibilityFor2_2Cost = CompatibilityFor2_2Cost(graph, queryCacheSize, statisticsDivergenceThreshold, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, Some(CostPlannerName))
  private val compatibilityFor2_2IDP = CompatibilityFor2_2Cost(graph, queryCacheSize, statisticsDivergenceThreshold, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, Some(IDPPlannerName))
  private val compatibilityFor2_2DP = CompatibilityFor2_2Cost(graph, queryCacheSize, statisticsDivergenceThreshold, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, Some(DPPlannerName))
  private val compatibilityFor2_2 = CompatibilityFor2_2Cost(graph, queryCacheSize, statisticsDivergenceThreshold, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger, Some(ConservativePlannerName))

  @throws(classOf[SyntaxException])
  def preParseQuery(queryText: String): PreParsedQuery = {
    val queryWithOptions = optionParser(queryText)
    val preParsedQuery = preParse(queryWithOptions, queryText)
    preParsedQuery
  }

  @throws(classOf[SyntaxException])
  def parseQuery(preParsedQuery: PreParsedQuery): ParsedQuery = {
    val version = preParsedQuery.version
    val planner = preParsedQuery.planner
    val statementAsText = preParsedQuery.statement
    val rawStatement = preParsedQuery.rawStatement
    val offset = preParsedQuery.offset

    (version, planner) match {
      case (CypherVersion.v2_2, ConservativePlannerName) => compatibilityFor2_2.produceParsedQuery(statementAsText,rawStatement, offset)
      case (CypherVersion.v2_2, CostPlannerName)         => compatibilityFor2_2Cost.produceParsedQuery(statementAsText,rawStatement, offset)
      case (CypherVersion.v2_2, IDPPlannerName)          => compatibilityFor2_2IDP.produceParsedQuery(statementAsText,rawStatement, offset)
      case (CypherVersion.v2_2, DPPlannerName)           => compatibilityFor2_2DP.produceParsedQuery(statementAsText,rawStatement, offset)
      case (CypherVersion.v2_2, RulePlannerName)         => compatibilityFor2_2Rule.produceParsedQuery(statementAsText,rawStatement, offset)
      case (CypherVersion.v2_2, _)                   => compatibilityFor2_2.produceParsedQuery(statementAsText,rawStatement, offset)
      case (CypherVersion.v2_1, _)                   => compatibilityFor2_1.parseQuery(statementAsText)
      case (CypherVersion.v2_0, _)                   => compatibilityFor2_0.parseQuery(statementAsText)
      case (CypherVersion.v1_9, _)                   => compatibilityFor1_9.parseQuery(statementAsText)
    }
  }

  private def preParse(queryWithOption: CypherQueryWithOptions, rawQuery: String): PreParsedQuery = {
    val cypherOptions = queryWithOption.options.collectFirst {
      case opt: ConfigurationOptions => opt
    }
    val cypherVersion = cypherOptions.flatMap(_.version)
      .map(v => CypherVersion(v.version))
      .getOrElse(defaultVersion)
    val planner = calculatePlanner(cypherOptions, queryWithOption.options, cypherVersion)
    val executionMode: ExecutionMode = calculateExecutionMode(queryWithOption.options)
    if (executionMode == ExplainMode && cypherVersion != CypherVersion.v2_2) {
      throw new InvalidArgumentException("EXPLAIN not supported in versions older than Neo4j v2.2")
    }

    PreParsedQuery(queryWithOption.statement, rawQuery, cypherVersion, executionMode, planner)(queryWithOption.offset)
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

    if (version != CypherVersion.v2_2 && planner.nonEmpty) {
      throw new InvalidArgumentException("PLANNER not supported in versions older than Neo4j v2.2")
    }

    if (planner.size > 1) {
      throw new InvalidSemanticsException("Can't use multiple planners")
    }

    //TODO once the we have removed PLANNER=X syntax, change to defaultPlanner here
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
    if (version != CypherVersion.v2_2 && planner.nonEmpty) {
      throw new InvalidArgumentException("PLANNER not supported in versions older than Neo4j v2.2")
    }

    if (planner.size > 1) {
      throw new InvalidSemanticsException("Can't use multiple planners")
    }

    if (planner.isEmpty) defaultPlanner else planner.head
  }

  private def getQueryCacheSize : Int =
    optGraphAs[InternalAbstractGraphDatabase]
      .andThen(_.getConfig.get(GraphDatabaseSettings.query_cache_size).intValue())
      .applyOrElse(graph, (_: GraphDatabaseService) => DEFAULT_QUERY_CACHE_SIZE)


  private def getStatisticsDivergenceThreshold : Double =
    optGraphAs[InternalAbstractGraphDatabase]
      .andThen(_.getConfig.get(GraphDatabaseSettings.query_statistics_divergence_threshold).doubleValue())
      .applyOrElse(graph, (_: GraphDatabaseService) => DEFAULT_STATISTICS_DIVERGENCE_THRESHOLD)


  private def getMinimumTimeBeforeReplanning: Long = {
    optGraphAs[InternalAbstractGraphDatabase]
      .andThen(_.getConfig.get(GraphDatabaseSettings.cypher_min_replan_interval).longValue())
      .applyOrElse(graph, (_: GraphDatabaseService) => DEFAULT_QUERY_PLAN_TTL)
  }


  private def optGraphAs[T <: GraphDatabaseService : Manifest]: PartialFunction[GraphDatabaseService, T] = {
    case (db: T) => db
  }
}
