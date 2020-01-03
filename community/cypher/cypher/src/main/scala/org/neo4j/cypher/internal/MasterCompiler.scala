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

import java.time.Clock

import org.neo4j.cypher._
import org.neo4j.cypher.internal.NotificationWrapping.asKernelNotification
import org.neo4j.cypher.internal.compiler.{DeprecatedCompiledRuntimeNotification, StatsDivergenceCalculator}
import org.neo4j.cypher.internal.v4_0.frontend.phases.{CompilationPhaseTracer, RecordingNotificationLogger}
import org.neo4j.graphdb.Notification
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.logging.LogProvider
import org.neo4j.monitoring.{Monitors => KernelMonitors}
import org.neo4j.values.virtual.MapValue

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
class MasterCompiler(config: CypherConfiguration,
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
    * Compile submitted query into executable query. TODO where is this used from?
    *
    * @param query                   query to convert
    * @param tracer                  compilation tracer to which events of the compilation process are reported
    * @param transactionalContext    transactional context to use during compilation (in logical and physical planning)
    * @return a compiled and executable query
    */
  def compile(query: InputQuery,
              tracer: CompilationPhaseTracer,
              transactionalContext: TransactionalContext,
              params: MapValue
             ): ExecutableQuery = {

    val logger = new RecordingNotificationLogger(Some(query.options.offset))

    def notificationsSoFar(): Set[Notification] = logger.notifications.map(asKernelNotification(None))

    if (query.options.runtime == CypherRuntimeOption.compiled)
      logger.log(DeprecatedCompiledRuntimeNotification)

    // Do the compilation
    val compiler = compilerLibrary.selectCompiler(
      query.options.version,
      query.options.planner,
      query.options.runtime,
      query.options.updateStrategy)

    compiler.compile(query, tracer, notificationsSoFar(), transactionalContext, params)
  }
}
