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

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compiler.v2_2.{RulePlanner, CostPlanner, IDPPlanner, ConservativePlanner, PlannerName}
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
}

case class PreParsedQuery(statement: String, version: CypherVersion, executionMode: ExecutionMode, planner: PlannerName) {
  val statementWithVersionAndPlanner = s"CYPHER ${version.name} PLANNER ${planner.name} $statement"
}

class CypherCompiler(graph: GraphDatabaseService,
                     kernelAPI: KernelAPI,
                     kernelMonitors: KernelMonitors,
                     defaultVersion: CypherVersion,
                     defaultPlanner: PlannerName,
                     optionParser: CypherOptionParser,
                     logger: StringLogger) {
  import CypherCompiler._

  private val queryCacheSize: Int = getQueryCacheSize
  private val queryPlanTTL: Long = getQueryPlanTTL
  private val compatibilityFor1_9 = CompatibilityFor1_9(graph, queryCacheSize, kernelMonitors)
  private val compatibilityFor2_0 = CompatibilityFor2_0(graph, queryCacheSize, kernelMonitors)
  private val compatibilityFor2_1 = CompatibilityFor2_1(graph, queryCacheSize, kernelMonitors, kernelAPI)
  private val compatibilityFor2_2Rule = CompatibilityFor2_2Rule(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI)
  private val compatibilityFor2_2Cost = CompatibilityFor2_2Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger)
  private val compatibilityFor2_2IDP = CompatibilityFor2_2IDP(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger)
  private val compatibilityFor2_2 = CompatibilityFor2_2Conservative(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, logger)

  @throws(classOf[SyntaxException])
  def preParseQuery(queryText: String): PreParsedQuery = {
    val queryWithOptions = optionParser(queryText)
    val preParsedQuery: PreParsedQuery = preParse(queryWithOptions)
    preParsedQuery
  }

  @throws(classOf[SyntaxException])
  def parseQuery(preParsedQuery: PreParsedQuery): ParsedQuery = {
    val executionMode = preParsedQuery.executionMode
    val version = preParsedQuery.version
    val planner = preParsedQuery.planner
    val statementAsText = preParsedQuery.statement

    (version, planner) match {
      case (CypherVersion.v2_2, ConservativePlanner) => compatibilityFor2_2.produceParsedQuery(statementAsText)
      case (CypherVersion.v2_2, CostPlanner)         => compatibilityFor2_2Cost.produceParsedQuery(statementAsText)
      case (CypherVersion.v2_2, IDPPlanner)          => compatibilityFor2_2IDP.produceParsedQuery(statementAsText)
      case (CypherVersion.v2_2, RulePlanner)         => compatibilityFor2_2Rule.produceParsedQuery(statementAsText)
      case (CypherVersion.v2_2, _)                   => compatibilityFor2_2.produceParsedQuery(statementAsText)
      case (CypherVersion.v2_1, _)                   => compatibilityFor2_1.parseQuery(statementAsText)
      case (CypherVersion.v2_0, _)                   => compatibilityFor2_0.parseQuery(statementAsText)
      case (CypherVersion.v1_9, _)                   => compatibilityFor1_9.parseQuery(statementAsText)
    }
  }

  private def preParse(queryWithOption: CypherQueryWithOptions): PreParsedQuery = {

    import org.neo4j.cypher.internal.CollectionFrosting._

    val versionOptions = queryWithOption.options.collectSingle {
      case VersionOption(v) => CypherVersion(v)
    }

    val cypherVersion = versionOptions match {
      case Right(version) => version.getOrElse(defaultVersion)
      case Left(versions) => throw new SyntaxException(s"You must specify only one version for a query (found: $versions)")
    }

    val executionMode: ExecutionMode = calculateExecutionMode(queryWithOption.options)
    val planner = calculatePlanner(queryWithOption.options, cypherVersion)
    if (executionMode == ExplainMode &&
      cypherVersion != CypherVersion.v2_2) {
      throw new InvalidArgumentException("EXPLAIN not supported in versions older than Neo4j v2.2")
    }

    PreParsedQuery(queryWithOption.statement, cypherVersion, executionMode, planner)
  }

  private def calculateExecutionMode(options: Seq[CypherOption]) = {
    val executionModes: Seq[ExecutionMode] = options.collect {
      case ExplainOption => ExplainMode
      case ProfileOption => ProfileMode
    }

    executionModes.reduceOption(_ combineWith _).getOrElse(NormalMode)
  }

  private def calculatePlanner(options: Seq[CypherOption], version: CypherVersion) = {
    val planner = options.collect {
      case CostPlannerOption => CostPlanner
      case RulePlannerOption => RulePlanner
      case IDPPlannerOption => IDPPlanner
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


  private def getQueryPlanTTL: Long = {
    optGraphAs[InternalAbstractGraphDatabase]
      .andThen(_.getConfig.get(GraphDatabaseSettings.query_plan_ttl).longValue())
      .applyOrElse(graph, (_: GraphDatabaseService) => DEFAULT_QUERY_PLAN_TTL)
  }


  private def optGraphAs[T <: GraphDatabaseService : Manifest]: PartialFunction[GraphDatabaseService, T] = {
    case (db: T) => db
  }
}
