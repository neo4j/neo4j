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

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_5.{CypherPlannerConfiguration, StatsDivergenceCalculator}
import org.neo4j.cypher.{InvalidArgumentException, _}
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.impl.notification.NotificationCode._
import org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.message
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.{Log, LogProvider}
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer
import org.opencypher.v9_0.util
import org.opencypher.v9_0.util.InputPosition

object MasterCompiler {
  val DEFAULT_QUERY_CACHE_SIZE: Int = 128
  val DEFAULT_QUERY_PLAN_TTL: Long = 1000 // 1 second
  val DEFAULT_QUERY_PLAN_TARGET: Long = 1000 * 60 * 60 * 7 // 7 hours
  val CLOCK: Clock = Clock.systemUTC()
  val DEFAULT_STATISTICS_DIVERGENCE_THRESHOLD = 0.5
  val DEFAULT_STATISTICS_DIVERGENCE_TARGET = 0.1
  val DEFAULT_DIVERGENCE_ALGORITHM: String = StatsDivergenceCalculator.inverse
  val DEFAULT_NON_INDEXED_LABEL_WARNING_THRESHOLD = 10000
}

/**
  * Selects the correct cypher implementation based on a pre-parsed query.
  */
class MasterCompiler(graph: GraphDatabaseQueryService,
                     kernelMonitors: KernelMonitors,
                     config: CypherConfiguration,
                     logProvider: LogProvider,
                     compilerLibrary: CompilerLibrary) {

  import org.neo4j.cypher.internal.MasterCompiler._

  private val log: Log = logProvider.getLog(getClass)

  private val compilerConfig = CypherPlannerConfiguration(
    queryCacheSize = config.queryCacheSize,
    statsDivergenceCalculator = getStatisticsDivergenceCalculator,
    useErrorsOverWarnings = config.useErrorsOverWarnings,
    idpMaxTableSize = config.idpMaxTableSize,
    idpIterationDuration = config.idpIterationDuration,
    errorIfShortestPathFallbackUsedAtRuntime = config.errorIfShortestPathFallbackUsedAtRuntime,
    errorIfShortestPathHasCommonNodesAtRuntime = config.errorIfShortestPathHasCommonNodesAtRuntime,
    legacyCsvQuoteEscaping = config.legacyCsvQuoteEscaping,
    csvBufferSize = config.csvBufferSize,
    nonIndexedLabelWarningThreshold = getNonIndexedLabelWarningThreshold,
    planWithMinimumCardinalityEstimates = config.planWithMinimumCardinalityEstimates
  )

  /**
    * Clear all compiler caches.
    *
    * @return the maximum number of entries clear from any cache
    */
  def clearCaches(): Long = {
    compilerLibrary.clearCaches()
  }

  /**
    * Compile pre-parsed query into executable query.
    *
    * @param preParsedQuery          pre-parsed query to convert
    * @param tracer                  compilation tracer to which events of the compilation process are reported
    * @param transactionalContext    transactional context to use during compilation (in logical and physical planning)
    * @return a compiled and executable query
    */
  def compile(preParsedQuery: PreParsedQuery,
              tracer: CompilationPhaseTracer,
              transactionalContext: TransactionalContext
             ): ExecutableQuery = {

    var notifications = Set.newBuilder[org.neo4j.graphdb.Notification]
    val supportedRuntimes3_1 = Seq(CypherRuntimeOption.interpreted, CypherRuntimeOption.default)
    val inputPosition = preParsedQuery.offset

    def assertSupportedRuntime(ex: util.SyntaxException, runtime: CypherRuntimeOption): Unit = {
      if (!supportedRuntimes3_1.contains(runtime)) {
        if (config.useErrorsOverWarnings) {
          throw new InvalidArgumentException("The given query is not currently supported in the selected runtime")
        } else {
          notifications += runtimeUnsupportedNotification(ex, inputPosition)
        }
      }
    }

    /**
      * Compile query or recursively fallback to 3.1 in some cases.
      *
      * @param preParsedQuery the query to compile
      * @return the compiled query
      */
    def innerCompile(preParsedQuery: PreParsedQuery): ExecutableQuery = {

      if ((preParsedQuery.version == CypherVersion.v3_3 || preParsedQuery.version == CypherVersion.v3_5) && preParsedQuery.planner == CypherPlannerOption.rule) {
        notifications += rulePlannerUnavailableFallbackNotification(preParsedQuery.offset)
        innerCompile(preParsedQuery.copy(version = CypherVersion.v3_1))

      } else if (preParsedQuery.version == CypherVersion.v3_5) {
        val compiler3_5 = compilerLibrary.selectCompiler(preParsedQuery.version,
                                                         preParsedQuery.planner,
                                                         preParsedQuery.runtime,
                                                         preParsedQuery.updateStrategy,
                                                         compilerConfig)

        try {
          compiler3_5.compile(preParsedQuery, tracer, notifications.result(), transactionalContext)
        } catch {
          case ex: SyntaxException if ex.getMessage.startsWith("CREATE UNIQUE") =>
            val ex3_5 = ex.getCause.asInstanceOf[util.SyntaxException]
            notifications += createUniqueNotification(ex3_5, inputPosition)
            assertSupportedRuntime(ex3_5, preParsedQuery.runtime)
            innerCompile(preParsedQuery.copy(version = CypherVersion.v3_1, runtime = CypherRuntimeOption.interpreted))

          case ex: SyntaxException if ex.getMessage.startsWith("START is deprecated") =>
            val ex3_5 = ex.getCause.asInstanceOf[util.SyntaxException]
            notifications += createStartUnavailableNotification(ex3_5, inputPosition)
            notifications += createStartDeprecatedNotification(ex3_5, inputPosition)
            assertSupportedRuntime(ex3_5, preParsedQuery.runtime)
            innerCompile(preParsedQuery.copy(version = CypherVersion.v3_1, runtime = CypherRuntimeOption.interpreted))
        }

      } else {

        val compiler = compilerLibrary.selectCompiler(preParsedQuery.version,
                                                      preParsedQuery.planner,
                                                      preParsedQuery.runtime,
                                                      preParsedQuery.updateStrategy,
                                                      compilerConfig)

        compiler.compile(preParsedQuery, tracer, notifications.result(), transactionalContext)
      }
    }

    // Do the compilation
    innerCompile(preParsedQuery)
  }

  private def createStartUnavailableNotification(ex: util.SyntaxException, inputPosition: InputPosition) = {
    val pos = convertInputPosition(ex.pos.getOrElse(inputPosition))
    START_UNAVAILABLE_FALLBACK.notification(pos)
  }

  private def createStartDeprecatedNotification(ex: util.SyntaxException, inputPosition: InputPosition) = {
    val pos = convertInputPosition(ex.pos.getOrElse(inputPosition))
    START_DEPRECATED.notification(pos, message("START", ex.getMessage))
  }

  private def runtimeUnsupportedNotification(ex: util.SyntaxException, inputPosition: InputPosition) = {
    val pos = convertInputPosition(ex.pos.getOrElse(inputPosition))
    RUNTIME_UNSUPPORTED.notification(pos)
  }

  private def createUniqueNotification(ex: util.SyntaxException, inputPosition: InputPosition) = {
    val pos = convertInputPosition(ex.pos.getOrElse(inputPosition))
    CREATE_UNIQUE_UNAVAILABLE_FALLBACK.notification(pos)
  }

  private def rulePlannerUnavailableFallbackNotification(offset: InputPosition) =
    RULE_PLANNER_UNAVAILABLE_FALLBACK.notification(convertInputPosition(offset))

  private def convertInputPosition(offset: InputPosition) =
    new org.neo4j.graphdb.InputPosition(offset.offset, offset.line, offset.column)

  private def getStatisticsDivergenceCalculator: StatsDivergenceCalculator = {
    val divergenceThreshold = getSetting(graph,
      config => config.get(GraphDatabaseSettings.query_statistics_divergence_threshold).doubleValue(),
      DEFAULT_STATISTICS_DIVERGENCE_THRESHOLD)
    val targetThreshold = getSetting(graph,
      config => config.get(GraphDatabaseSettings.query_statistics_divergence_target).doubleValue(),
      DEFAULT_STATISTICS_DIVERGENCE_TARGET)
    val minReplanTime = getSetting(graph,
      config => config.get(GraphDatabaseSettings.cypher_min_replan_interval).toMillis.longValue(),
      DEFAULT_QUERY_PLAN_TTL)
    val targetReplanTime = getSetting(graph,
      config => config.get(GraphDatabaseSettings.cypher_replan_interval_target).toMillis.longValue(),
      DEFAULT_QUERY_PLAN_TARGET)
    val divergenceAlgorithm = getSetting(graph,
      config => config.get(GraphDatabaseSettings.cypher_replan_algorithm),
      DEFAULT_DIVERGENCE_ALGORITHM)
    StatsDivergenceCalculator.divergenceCalculatorFor(divergenceAlgorithm, divergenceThreshold, targetThreshold, minReplanTime, targetReplanTime)
  }

  private def getNonIndexedLabelWarningThreshold: Long = {
    val setting: (Config) => Long = config => config.get(GraphDatabaseSettings.query_non_indexed_label_warning_threshold).longValue()
    getSetting(graph, setting, DEFAULT_NON_INDEXED_LABEL_WARNING_THRESHOLD)
  }

  private def getSetting[A](gds: GraphDatabaseQueryService, configLookup: Config => A, default: A): A = gds match {
    // TODO: Cypher should not be pulling out components from casted interfaces, it should ask for Config as a dep
    case (gdbApi:GraphDatabaseQueryService) => configLookup(gdbApi.getDependencyResolver.resolveDependency(classOf[Config]))
    case _ => default
  }
}
