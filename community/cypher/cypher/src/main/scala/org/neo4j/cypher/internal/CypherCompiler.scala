/*
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
import org.neo4j.cypher.internal.CypherStatementWithOptions
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compiler.v2_3.notification.LegacyPlannerNotification
import org.neo4j.cypher.internal.compiler.v2_3.{InputPosition, InternalNotificationLogger, PlannerName, RecordingNotificationLogger, devNullLogger, _}
import org.neo4j.cypher.{InvalidArgumentException, InvalidSemanticsException, SyntaxException, _}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.helpers.Clock
import org.neo4j.kernel.InternalAbstractGraphDatabase
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.{Log, LogProvider}

object CypherCompiler {
  val DEFAULT_QUERY_CACHE_SIZE: Int = 128
  val DEFAULT_QUERY_PLAN_TTL: Long = 1000 // 1 second
  val CLOCK = Clock.SYSTEM_CLOCK
  val STATISTICS_DIVERGENCE_THRESHOLD = 0.5

  def notificationLoggerBuilder(executionMode: CypherExecutionMode): InternalNotificationLogger = executionMode  match {
      case CypherExecutionMode.explain => new RecordingNotificationLogger()
      case _ => devNullLogger
    }
}

case class PreParsedQuery(statement: String, version: CypherVersion, executionMode: CypherExecutionMode,
                          planner: CypherPlanner, runtime: CypherRuntime, notificationLogger: InternalNotificationLogger)
                         (val offset: InputPosition) {
  val statementWithVersionAndPlanner = s"CYPHER ${version.name} planner=${planner.name} runtime=${runtime.name} $statement"
}

class CypherCompiler(graph: GraphDatabaseService,
                     kernelAPI: KernelAPI,
                     kernelMonitors: KernelMonitors,
                     configuredVersion: CypherVersion,
                     configuredPlanner: CypherPlanner,
                     configuredRuntime: CypherRuntime,
                     optionParser: CypherPreParser,
                     logProvider: LogProvider) {
  import org.neo4j.cypher.internal.CypherCompiler._

  private val factory = new PlannerFactory {
    private val log: Log = logProvider.getLog(getClass)
    private val queryCacheSize: Int = getQueryCacheSize
    private val queryPlanTTL: Long = getMinimumTimeBeforeReplanning

    override def create[T](spec: PlannerSpec[T]): T = spec match {
      case PlannerSpec_v1_9 => CompatibilityFor1_9(graph, queryCacheSize, kernelMonitors).asInstanceOf[T]
      case PlannerSpec_v2_2(planner) => planner match {
        case CypherPlanner.rule => CompatibilityFor2_2Rule(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI).asInstanceOf[T]
        case _ => CompatibilityFor2_2Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, log, planner).asInstanceOf[T]
      }
      case PlannerSpec_v2_3(planner, runtime) => planner match {
        case CypherPlanner.rule => CompatibilityFor2_3Rule(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI).asInstanceOf[T]
        case _ => CompatibilityFor2_3Cost(graph, queryCacheSize, STATISTICS_DIVERGENCE_THRESHOLD, queryPlanTTL, CLOCK, kernelMonitors, kernelAPI, log, planner, runtime).asInstanceOf[T]
      }
    }
  }

  private val planners = new PlannerCache(factory)

  private final val VERSIONS_WITH_FIXED_PLANNER: Set[CypherVersion] = Set(v1_9)
  private final val VERSIONS_WITH_FIXED_RUNTIME: Set[CypherVersion] = Set(v1_9, v2_2)

  private final val ILLEGAL_PLANNER_RUNTIME_COMBINATIONS: Set[(CypherPlanner, CypherRuntime)] = Set((CypherPlanner.rule, CypherRuntime.compiled))

  @throws(classOf[SyntaxException])
  def preParseQuery(queryText: String): PreParsedQuery = {
    val preParsedStatement = optionParser(queryText)
    val statementWithOptions = CypherStatementWithOptions(preParsedStatement)
    val CypherStatementWithOptions(statement, offset, version, planner, runtime, mode, notifications) = statementWithOptions

    val cypherVersion = version.getOrElse(configuredVersion)
    val pickedExecutionMode = mode.getOrElse(CypherExecutionMode.default)

    val pickedPlanner = pick(planner, CypherPlanner, if (cypherVersion == configuredVersion) Some(configuredPlanner) else None)
    val pickedRuntime = pick(runtime, CypherRuntime, if (cypherVersion == configuredVersion) Some(configuredRuntime) else None)

    assertValidOptions(statementWithOptions, cypherVersion, pickedExecutionMode, pickedPlanner, pickedRuntime)

    val logger = notificationLoggerBuilder(pickedExecutionMode)
    notifications.foreach( logger += _ )

    PreParsedQuery(statement, cypherVersion, pickedExecutionMode, pickedPlanner, pickedRuntime, logger)(offset)
  }

  private def pick[O <: CypherOption](candidate: Option[O], companion: CypherOptionCompanion[O], configured: Option[O]): O = {
    val specified = candidate.getOrElse(companion.default)
    if (specified == companion.default) configured.getOrElse(specified) else specified
  }

  private def assertValidOptions(statementWithOption: CypherStatementWithOptions,
                                 cypherVersion: CypherVersion, executionMode: CypherExecutionMode,
                                 planner: CypherPlanner, runtime: CypherRuntime) {
    if (VERSIONS_WITH_FIXED_PLANNER(cypherVersion)) {
      if (statementWithOption.planner.nonEmpty)
        throw new InvalidArgumentException("PLANNER not supported in versions older than Neo4j v2.2")

      if (executionMode == CypherExecutionMode.explain)
        throw new InvalidArgumentException("EXPLAIN not supported in versions older than Neo4j v2.2")
    }

    if (VERSIONS_WITH_FIXED_RUNTIME(cypherVersion) && statementWithOption.runtime.nonEmpty)
      throw new InvalidArgumentException("RUNTIME not supported in versions older than Neo4j v2.3")

    if (ILLEGAL_PLANNER_RUNTIME_COMBINATIONS((planner, runtime)))
      throw new InvalidArgumentException(s"Unsupported PLANNER - RUNTIME combination: ${planner.name} - ${runtime.name}")
  }

  @throws(classOf[SyntaxException])
  def parseQuery(preParsedQuery: PreParsedQuery): ParsedQuery = {
    val planner = preParsedQuery.planner
    val runtime = preParsedQuery.runtime

    preParsedQuery.version match {
      case CypherVersion.v2_3 => planners(PlannerSpec_v2_3(planner, runtime)).produceParsedQuery(preParsedQuery)
      case CypherVersion.v2_2 => planners(PlannerSpec_v2_2(planner)).produceParsedQuery(preParsedQuery.statement)
      case CypherVersion.v1_9 => planners(PlannerSpec_v1_9).parseQuery(preParsedQuery.statement)
    }
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
