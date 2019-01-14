/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.InternalWrapping.asKernelNotification
import org.neo4j.cypher.internal.compiler.v3_5.{StatsDivergenceCalculator, _}
import org.neo4j.cypher.{InvalidArgumentException, _}
import org.neo4j.graphdb.Notification
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.LogProvider
import org.neo4j.values.virtual.MapValue
import org.neo4j.cypher.internal.v3_5.frontend.phases.{CompilationPhaseTracer, RecordingNotificationLogger}
import org.neo4j.cypher.internal.v3_5.util.{DeprecatedStartNotification, InternalNotification, SyntaxException => InternalSyntaxException}

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
              transactionalContext: TransactionalContext,
              params: MapValue
             ): ExecutableQuery = {

    val logger = new RecordingNotificationLogger(Some(preParsedQuery.offset))

    def notificationsSoFar(): Set[Notification] = logger.notifications.map(asKernelNotification(None))

    val supportedRuntimes3_1 = Seq(CypherRuntimeOption.interpreted, CypherRuntimeOption.default)
    val inputPosition = preParsedQuery.offset

    def assertSupportedRuntime(ex: InternalSyntaxException, runtime: CypherRuntimeOption): Unit = {
      if (!supportedRuntimes3_1.contains(runtime)) {
        if (config.useErrorsOverWarnings) {
          throw new InvalidArgumentException("The given query is not currently supported in the selected runtime")
        } else {
          logger.log(RuntimeUnsupportedNotification)
        }
      }
    }

    /**
      * Compile query or recursively fallback to 3.1 in some cases.
      *
      * @param preParsedQuery the query to compile
      * @return the compiled query
      */
    def innerCompile(preParsedQuery: PreParsedQuery, params: MapValue): ExecutableQuery = {

      if ((preParsedQuery.version == CypherVersion.v3_4 || preParsedQuery.version == CypherVersion.v3_5) && preParsedQuery.planner == CypherPlannerOption.rule) {
        logger.log(RulePlannerUnavailableFallbackNotification)
        innerCompile(preParsedQuery.copy(version = CypherVersion.v3_1), params)

      } else if (preParsedQuery.version == CypherVersion.v3_5) {
        val compiler3_5 = compilerLibrary.selectCompiler(preParsedQuery.version,
                                                         preParsedQuery.planner,
                                                         preParsedQuery.runtime,
                                                         preParsedQuery.updateStrategy)

        try {
          compiler3_5.compile(preParsedQuery, tracer, notificationsSoFar(), transactionalContext, params)
        } catch {
          case ex: SyntaxException if ex.getMessage.startsWith("CREATE UNIQUE") =>
            val ex3_5 = ex.getCause.asInstanceOf[InternalSyntaxException]
            logger.log(CreateUniqueUnavailableFallback(ex3_5.pos.get))
            logger.log(CreateUniqueDeprecated(ex3_5.pos.get))
            assertSupportedRuntime(ex3_5, preParsedQuery.runtime)
            innerCompile(preParsedQuery.copy(version = CypherVersion.v3_1, runtime = CypherRuntimeOption.interpreted), params)

          case ex: SyntaxException if ex.getMessage.startsWith("START is deprecated") =>
            val ex3_5 = ex.getCause.asInstanceOf[InternalSyntaxException]
            logger.log(StartUnavailableFallback)
            logger.log(DeprecatedStartNotification(inputPosition, ex.getMessage))
            assertSupportedRuntime(ex3_5, preParsedQuery.runtime)
            innerCompile(preParsedQuery.copy(version = CypherVersion.v3_1, runtime = CypherRuntimeOption.interpreted), params)
        }

      } else {

        val compiler = compilerLibrary.selectCompiler(preParsedQuery.version,
                                                      preParsedQuery.planner,
                                                      preParsedQuery.runtime,
                                                      preParsedQuery.updateStrategy)

        compiler.compile(preParsedQuery, tracer, notificationsSoFar(), transactionalContext, params)
      }
    }

    if (preParsedQuery.planner == CypherPlannerOption.rule)
      logger.log(DeprecatedRulePlannerNotification)

    if (preParsedQuery.runtime == CypherRuntimeOption.compiled)
      logger.log(DeprecatedCompiledRuntimeNotification)

    // Do the compilation
    innerCompile(preParsedQuery, params)
  }
}
